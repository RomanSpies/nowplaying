//! Line-synced lyrics reduced to their visual structure.
//!
//! The real text comes from Spotify's color-lyrics endpoint and is scrambled
//! server-side before anything leaves the process: the first letter of every
//! whitespace-delimited word survives, every further letter is replaced by a
//! seeded-random letter of the same case class, and non-letters keep their
//! positions. Character counts, word rhythm, line structure and timestamps
//! stay real; the expressive content does not. Only `LineSynced` lyrics
//! qualify — anything else is treated as absent, so the wire contract knows
//! no unsynced case.

use librespot::core::error::ErrorKind;
use librespot::core::{Session, SpotifyId};
use librespot::metadata::lyrics::{Lyrics as SpotifyLyrics, SyncType};
use opentelemetry::KeyValue;
use tracing::warn;

use crate::events::{LyricLine, Lyrics};
use crate::spotify::metadata::api_metrics;

/// Outcome of a lyrics fetch, labelled for the `lyrics_fetch_total` metric.
pub enum LyricsFetch {
    Synced(Lyrics),
    Unsynced,
    Missing,
    Error,
}

impl LyricsFetch {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Synced(_) => "ok",
            Self::Unsynced => "unsynced",
            Self::Missing => "none",
            Self::Error => "error",
        }
    }

    pub fn into_option(self) -> Option<Lyrics> {
        match self {
            Self::Synced(lyrics) => Some(lyrics),
            _ => None,
        }
    }
}

/// Fetch and scramble the lyrics for a base62 track id. Never fails the
/// caller: every problem degrades to a non-`Synced` outcome, logged with its
/// cause so a rising `outcome=error` rate is diagnosable.
pub async fn fetch(session: &Session, track_id: &str) -> LyricsFetch {
    let Ok(id) = SpotifyId::from_base62(track_id) else {
        warn!(track_id, "lyrics fetch skipped, unparsable track id");
        return LyricsFetch::Error;
    };
    api_metrics()
        .requests
        .add(1, &[KeyValue::new("endpoint", "lyrics")]);
    match session.spclient().get_lyrics(&id).await {
        Ok(bytes) => {
            api_metrics()
                .response_bytes
                .add(bytes.len() as u64, &[KeyValue::new("endpoint", "lyrics")]);
            match SpotifyLyrics::try_from(&bytes) {
                Ok(lyrics) => convert(lyrics, track_id),
                Err(e) => {
                    warn!(track_id, error = %e, "lyrics response undecodable");
                    LyricsFetch::Error
                }
            }
        }
        Err(e) if e.kind == ErrorKind::NotFound => LyricsFetch::Missing,
        Err(e) => {
            warn!(track_id, error = %e, "lyrics fetch failed");
            LyricsFetch::Error
        }
    }
}

/// Malformed timestamps discard the lyrics entirely — the synced-only
/// contract ships complete data or nothing.
fn convert(sp: SpotifyLyrics, seed_key: &str) -> LyricsFetch {
    if sp.lyrics.sync_type != SyncType::LineSynced {
        return LyricsFetch::Unsynced;
    }
    let mut lines = Vec::with_capacity(sp.lyrics.lines.len());
    for (idx, line) in sp.lyrics.lines.iter().enumerate() {
        let (Ok(start_ms), Ok(end_ms)) = (
            line.start_time_ms.parse::<u64>(),
            line.end_time_ms.parse::<u64>(),
        ) else {
            return LyricsFetch::Error;
        };
        lines.push(LyricLine {
            start_ms,
            end_ms,
            text: scramble_line(&line.words, line_seed(seed_key, idx)),
        });
    }
    if lines.is_empty() {
        return LyricsFetch::Missing;
    }
    LyricsFetch::Synced(Lyrics { lines })
}

/// FNV-1a over the track id, mixed with the line index — deterministic
/// across refetches so cached and refetched frames render identically.
fn line_seed(key: &str, line_idx: usize) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^ (line_idx as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
}

struct XorShift64(u64);

impl XorShift64 {
    /// Zero is the only invalid xorshift state (it is a fixed point).
    fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0x9e37_79b9_7f4a_7c15
        } else {
            seed
        })
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// The first alphabetic character after line start or whitespace survives;
/// every other letter is replaced (same case class, see `replacement`).
/// Non-letters keep their positions, so character count, punctuation and
/// word rhythm match the original exactly.
fn scramble_line(line: &str, seed: u64) -> String {
    let mut rng = XorShift64::new(seed);
    let mut out = String::with_capacity(line.len());
    let mut at_word_start = true;
    for c in line.chars() {
        if c.is_alphabetic() {
            if at_word_start {
                out.push(c);
                at_word_start = false;
            } else {
                out.push(replacement(c, &mut rng));
            }
        } else {
            if c.is_whitespace() {
                at_word_start = true;
            }
            out.push(c);
        }
    }
    out
}

/// Same case class as the original so the word image keeps its shape;
/// non-ASCII letters collapse into the ASCII class of their case.
fn replacement(original: char, rng: &mut XorShift64) -> char {
    let base = if original.is_uppercase() { b'A' } else { b'a' };
    char::from(base + (rng.next() % 26) as u8)
}

#[cfg(test)]
mod tests {
    use librespot::metadata::lyrics::{Colors, Line, LyricsInner};

    use super::*;

    const SEED: u64 = 0x5eed_1234;

    #[test]
    fn scramble_preserves_length_first_letters_and_punctuation() {
        let original = "Never gonna give you up, never let you down!";
        let scrambled = scramble_line(original, SEED);

        assert_eq!(scrambled.chars().count(), original.chars().count());
        for (o, s) in original
            .split_whitespace()
            .zip(scrambled.split_whitespace())
        {
            assert_eq!(o.chars().next(), s.chars().next());
            assert_eq!(o.chars().count(), s.chars().count());
        }
        for (o, s) in original.chars().zip(scrambled.chars()) {
            if !o.is_alphabetic() {
                assert_eq!(o, s, "non-letters must keep their positions");
            }
        }
    }

    #[test]
    fn interior_letters_are_replaced_not_permuted() {
        let scrambled = scramble_line("Believing", SEED);
        assert_ne!(scrambled, "Believing");
        assert!(scrambled.starts_with('B'));
        for c in scrambled.chars().skip(1) {
            assert!(c.is_ascii_lowercase(), "case class must be preserved: {c}");
        }
    }

    #[test]
    fn scramble_is_deterministic_and_seed_sensitive() {
        let line = "Never gonna run around and desert you";
        assert_eq!(scramble_line(line, SEED), scramble_line(line, SEED));
        assert_ne!(scramble_line(line, SEED), scramble_line(line, SEED + 1));
        assert_ne!(line_seed("track_a", 0), line_seed("track_a", 1));
        assert_ne!(line_seed("track_a", 0), line_seed("track_b", 0));
    }

    #[test]
    fn short_words_apostrophes_and_unicode_survive() {
        let scrambled = scramble_line("I am über groß, don't go", SEED);
        assert_eq!(
            scrambled.chars().count(),
            "I am über groß, don't go".chars().count()
        );
        assert!(scrambled.starts_with("I a"));
        assert_eq!(scrambled.chars().nth(5), Some('ü'));
        assert_eq!(
            scrambled.chars().filter(|c| *c == '\'').count(),
            1,
            "apostrophe must survive in place"
        );
    }

    fn spotify_lyrics(sync_type: SyncType, lines: Vec<Line>) -> SpotifyLyrics {
        SpotifyLyrics {
            colors: Colors {
                background: 0,
                highlight_text: 0,
                text: 0,
            },
            has_vocal_removal: false,
            lyrics: LyricsInner {
                is_dense_typeface: false,
                is_rtl_language: false,
                language: "en".into(),
                lines,
                provider: "test".into(),
                provider_display_name: "test".into(),
                provider_lyrics_id: "0".into(),
                sync_lyrics_uri: String::new(),
                sync_type,
            },
        }
    }

    fn line(start: &str, end: &str, words: &str) -> Line {
        Line {
            start_time_ms: start.into(),
            end_time_ms: end.into(),
            words: words.into(),
        }
    }

    #[test]
    fn convert_ships_only_line_synced_lyrics() {
        let synced = spotify_lyrics(
            SyncType::LineSynced,
            vec![line("1000", "4200", "Never gonna give you up")],
        );
        let LyricsFetch::Synced(lyrics) = convert(synced, "track_a") else {
            panic!("line-synced lyrics must convert");
        };
        assert_eq!(lyrics.lines.len(), 1);
        assert_eq!(lyrics.lines[0].start_ms, 1_000);
        assert_eq!(lyrics.lines[0].end_ms, 4_200);
        assert_ne!(lyrics.lines[0].text, "Never gonna give you up");
        assert!(lyrics.lines[0].text.starts_with("N"));

        let unsynced = spotify_lyrics(SyncType::Unsynced, vec![line("0", "0", "text")]);
        assert!(matches!(
            convert(unsynced, "track_a"),
            LyricsFetch::Unsynced
        ));

        let malformed = spotify_lyrics(SyncType::LineSynced, vec![line("oops", "4200", "text")]);
        assert!(matches!(convert(malformed, "track_a"), LyricsFetch::Error));

        let empty = spotify_lyrics(SyncType::LineSynced, vec![]);
        assert!(matches!(convert(empty, "track_a"), LyricsFetch::Missing));
    }
}
