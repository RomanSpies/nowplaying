use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::Serialize;

/// What kind of playable item a play is. Serialized as the `kind` wire field
/// and stored as `plays.content_type`; only `Track` plays feed the top lists,
/// and `Local` plays (files on the listener's device) are never persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MediaKind {
    Track,
    Episode,
    Local,
}

impl MediaKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Track => "track",
            Self::Episode => "episode",
            Self::Local => "local",
        }
    }

    /// Inverse of [`MediaKind::as_str`] for the `plays.content_type` column.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "track" => Some(Self::Track),
            "episode" => Some(Self::Episode),
            "local" => Some(Self::Local),
            _ => None,
        }
    }

    /// Public open.spotify.com link for an item id. Local files live only on
    /// the listener's device and have no public page.
    pub fn open_url(self, id: &str) -> Option<String> {
        match self {
            Self::Track => Some(format!("https://open.spotify.com/track/{id}")),
            Self::Episode => Some(format!("https://open.spotify.com/episode/{id}")),
            Self::Local => None,
        }
    }

    /// Only these kinds are written to `plays`.
    pub fn is_persisted(self) -> bool {
        !matches!(self, Self::Local)
    }
}

/// Where a play's metadata came from, stored as `plays.metadata_source`.
/// `ClusterMap` marks a degraded record (the cluster map carries no artist
/// names) that the metadata repair pass later upgrades to `Fetch`; `Uri` is
/// a local file described entirely by its URI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataSource {
    Fetch,
    ClusterMap,
    Uri,
}

impl MetadataSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fetch => "fetch",
            Self::ClusterMap => "cluster_map",
            Self::Uri => "uri",
        }
    }
}

/// A single playback, as pushed to WSS clients and stored in Postgres.
/// `lyrics` travels only on the wire (never persisted) and is present iff
/// line-synced, server-side scrambled lyrics exist for the track;
/// `metadata_source` is persisted but never sent. `track_url` is `null` on
/// the wire exactly for local files.
#[derive(Debug, Clone, Serialize)]
pub struct PlayEvent {
    pub track_id: String,
    pub kind: MediaKind,
    pub track_url: Option<String>,
    pub title: String,
    pub artists: Vec<String>,
    pub album: String,
    pub cover_url: Option<String>,
    pub duration_ms: u32,
    pub started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lyrics: Option<Lyrics>,
    #[serde(skip)]
    pub metadata_source: MetadataSource,
}

/// Line-synced, scrambled lyrics: real line structure, lengths and
/// timestamps, but every word reduced to its first letter plus seeded-random
/// replacements — the expressive content never leaves the server (see
/// spotify::lyrics).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Lyrics {
    pub lines: Vec<LyricLine>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LyricLine {
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
}

/// Identity of a play, independent of the wire framing. Used on the WS path
/// to recognize a broadcast frame that duplicates the just-replayed state.
#[derive(Debug, Clone, PartialEq)]
pub struct PlayKey {
    pub track_id: String,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PlaybackState {
    Playing,
    Paused,
    Stopped,
}

/// Live playback as of `as_of` (server clock). Clients extrapolate the
/// position while `state == playing` (`position_ms + (now - as_of)`) and
/// freeze it otherwise. Built exclusively server-side from cluster snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Playback {
    pub state: PlaybackState,
    pub position_ms: u64,
    pub as_of: DateTime<Utc>,
}

/// Which kind of wire frame a broadcast item carries. `Play` covers every
/// frame with full track metadata (`play` and broadcast `now_playing`); the
/// WS connect-race dedupe only ever skips those (see web::ws::should_skip).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    Play,
    State,
}

/// Broadcast payload: kind + play identity plus the pre-serialized frame
/// (serialized once in `publish_play`/`publish_state`; clones are refcount
/// bumps).
#[derive(Debug, Clone)]
pub struct WsFrame {
    pub kind: FrameKind,
    pub key: PlayKey,
    pub bytes: Bytes,
}

/// Wire format for WebSocket messages. `play` is a new playback (full track
/// metadata), `now_playing` is the replay a client receives right after
/// connecting, and `state` is a light live-state delta (pause/resume/seek/
/// stop) correlated via `track_id`. All three carry a `playback` object.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsMessage<'a> {
    Play {
        #[serde(flatten)]
        event: &'a PlayEvent,
        playback: Playback,
    },
    NowPlaying {
        #[serde(flatten)]
        event: &'a PlayEvent,
        playback: Playback,
    },
    State {
        track_id: &'a str,
        playback: Playback,
    },
}

impl PlayEvent {
    pub fn key(&self) -> PlayKey {
        PlayKey {
            track_id: self.track_id.clone(),
            started_at: self.started_at,
        }
    }

    /// Serialization cannot fail: all fields are plain data.
    pub fn to_ws_bytes(&self, as_now_playing: bool, playback: Playback) -> Bytes {
        let msg = if as_now_playing {
            WsMessage::NowPlaying {
                event: self,
                playback,
            }
        } else {
            WsMessage::Play {
                event: self,
                playback,
            }
        };
        Bytes::from(serde_json::to_vec(&msg).expect("PlayEvent serializes"))
    }
}

/// Serialize a light live-state frame.
pub fn state_ws_bytes(track_id: &str, playback: Playback) -> Bytes {
    let msg = WsMessage::State { track_id, playback };
    Bytes::from(serde_json::to_vec(&msg).expect("state frame serializes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> PlayEvent {
        PlayEvent {
            track_id: "4uLU6hMCjMI75M1A2tKUQC".into(),
            kind: MediaKind::Track,
            track_url: Some("https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC".into()),
            title: "Never Gonna Give You Up".into(),
            artists: vec!["Rick Astley".into()],
            album: "Whenever You Need Somebody".into(),
            cover_url: Some("https://i.scdn.co/image/abc".into()),
            duration_ms: 213_000,
            started_at: "2026-07-28T12:00:00Z".parse().unwrap(),
            lyrics: None,
            metadata_source: MetadataSource::Fetch,
        }
    }

    fn sample_playback() -> Playback {
        Playback {
            state: PlaybackState::Paused,
            position_ms: 83_000,
            as_of: "2026-07-28T12:01:23Z".parse().unwrap(),
        }
    }

    #[test]
    fn ws_message_is_tagged_and_stays_flat() {
        let e = sample();
        let play: serde_json::Value =
            serde_json::from_slice(&e.to_ws_bytes(false, sample_playback())).unwrap();
        assert_eq!(play["type"], "play");
        assert_eq!(play["title"], "Never Gonna Give You Up");
        assert_eq!(play["playback"]["state"], "paused");
        assert_eq!(play["playback"]["position_ms"], 83_000);
        assert_eq!(play["playback"]["as_of"], "2026-07-28T12:01:23Z");

        let now: serde_json::Value =
            serde_json::from_slice(&e.to_ws_bytes(true, sample_playback())).unwrap();
        assert_eq!(now["type"], "now_playing");
        assert_eq!(now["duration_ms"], 213_000);
        assert_eq!(now["playback"]["state"], "paused");
        assert_eq!(now["kind"], "track");
        assert!(
            now.get("metadata_source").is_none(),
            "metadata_source is server-internal"
        );
    }

    /// Local files have no public page: the link is an explicit `null`, so
    /// clients can rely on the key being present.
    #[test]
    fn local_play_serializes_null_url() {
        let local = PlayEvent {
            kind: MediaKind::Local,
            track_url: None,
            ..sample()
        };
        let v: serde_json::Value =
            serde_json::from_slice(&local.to_ws_bytes(false, sample_playback())).unwrap();
        assert_eq!(v["kind"], "local");
        assert!(v["track_url"].is_null());
    }

    #[test]
    fn media_kind_links_and_db_round_trip() {
        assert_eq!(
            MediaKind::Episode.open_url("abc").as_deref(),
            Some("https://open.spotify.com/episode/abc")
        );
        assert_eq!(MediaKind::Local.open_url("abc"), None);
        for kind in [MediaKind::Track, MediaKind::Episode, MediaKind::Local] {
            assert_eq!(MediaKind::from_db(kind.as_str()), Some(kind));
        }
        assert_eq!(MediaKind::from_db("ad"), None);
    }

    #[test]
    fn lyrics_are_optional_and_ride_the_play_frames() {
        let bare = sample();
        let play: serde_json::Value =
            serde_json::from_slice(&bare.to_ws_bytes(false, sample_playback())).unwrap();
        assert!(
            play.get("lyrics").is_none(),
            "absent lyrics must not serialize a field"
        );

        let mut with_lyrics = sample();
        with_lyrics.lyrics = Some(Lyrics {
            lines: vec![
                LyricLine {
                    start_ms: 1_000,
                    end_ms: 4_200,
                    text: "Nzqmr gswby lkvv ehm tp".into(),
                },
                LyricLine {
                    start_ms: 4_200,
                    end_ms: 8_000,
                    text: "Nzqmr gswby lkvv ehm dgnn".into(),
                },
            ],
        });
        let play: serde_json::Value =
            serde_json::from_slice(&with_lyrics.to_ws_bytes(false, sample_playback())).unwrap();
        assert_eq!(play["lyrics"]["lines"].as_array().unwrap().len(), 2);
        assert_eq!(play["lyrics"]["lines"][0]["start_ms"], 1_000);
        assert_eq!(
            play["lyrics"]["lines"][1]["text"],
            "Nzqmr gswby lkvv ehm dgnn"
        );
    }

    /// Nothing beyond `type`/`track_id`/`playback` may leak into the light
    /// frame — in particular no device identifiers; those stay in
    /// server-side logs and spans.
    #[test]
    fn state_frame_carries_exactly_type_track_and_playback() {
        let bytes = state_ws_bytes("4uLU6hMCjMI75M1A2tKUQC", sample_playback());
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["type"], "state");
        assert_eq!(v["track_id"], "4uLU6hMCjMI75M1A2tKUQC");
        assert_eq!(v["playback"]["state"], "paused");
        assert_eq!(v["playback"]["position_ms"], 83_000);

        let mut keys: Vec<_> = v.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, ["playback", "track_id", "type"]);
        let mut pb_keys: Vec<_> = v["playback"].as_object().unwrap().keys().cloned().collect();
        pb_keys.sort();
        assert_eq!(pb_keys, ["as_of", "position_ms", "state"]);
    }
}
