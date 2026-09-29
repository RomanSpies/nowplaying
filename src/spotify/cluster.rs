use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use librespot::core::SpotifyUri;
use librespot::protocol::connect::ClusterUpdate;

use crate::events::{MediaKind, Playback, PlaybackState};

/// Plain extraction of the fields we need from a protobuf `ClusterUpdate`.
/// Decoupling from protobuf keeps `PlaybackTracker` fully unit-testable.
#[derive(Debug, Clone)]
pub struct ClusterSnapshot {
    pub track_uri: String,
    pub kind: MediaKind,
    pub metadata: HashMap<String, String>,
    /// Server wall clock (ms) at which `position_ms` was valid.
    pub timestamp_ms: i64,
    /// Track position (ms) as of `timestamp_ms`.
    pub position_ms: i64,
    pub duration_ms: i64,
    pub is_playing: bool,
    pub is_paused: bool,
    pub active_device_id: String,
    /// The active device reports a Spotify private session. Absent device
    /// info reads as `false`.
    pub private_session: bool,
}

/// Why a cluster update yielded no [`ClusterSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoSnapshot {
    /// No cluster, no player state or no track URI at all.
    NoTrack,
    /// A playable-looking URI of a kind this service does not model (ads,
    /// unknown future kinds); carries the URI kind for diagnostics.
    Unsupported(String),
}

/// Classify a cluster track URI. Anything but tracks, podcast episodes and
/// local files is unsupported.
pub fn classify(track_uri: &str) -> Result<MediaKind, NoSnapshot> {
    match SpotifyUri::from_uri(track_uri) {
        Ok(SpotifyUri::Track { .. }) => Ok(MediaKind::Track),
        Ok(SpotifyUri::Episode { .. }) => Ok(MediaKind::Episode),
        Ok(SpotifyUri::Local { .. }) => Ok(MediaKind::Local),
        Ok(SpotifyUri::Unknown { kind, .. }) => Err(NoSnapshot::Unsupported(kind.into_owned())),
        Ok(_) => Err(NoSnapshot::Unsupported("non_playable".into())),
        Err(_) => Err(NoSnapshot::Unsupported("unparsable".into())),
    }
}

impl ClusterSnapshot {
    pub fn from_update(mut update: ClusterUpdate) -> Result<Self, NoSnapshot> {
        let mut cluster = update.cluster.take().ok_or(NoSnapshot::NoTrack)?;
        let private_session = cluster
            .device
            .get(&cluster.active_device_id)
            .is_some_and(|d| d.is_private_session);
        let mut player_state = cluster.player_state.take().ok_or(NoSnapshot::NoTrack)?;
        let track = player_state.track.take().ok_or(NoSnapshot::NoTrack)?;
        if track.uri.is_empty() {
            return Err(NoSnapshot::NoTrack);
        }
        let kind = classify(&track.uri)?;
        Ok(Self {
            track_uri: track.uri,
            kind,
            metadata: track.metadata,
            timestamp_ms: player_state.timestamp,
            position_ms: player_state.position_as_of_timestamp,
            duration_ms: player_state.duration,
            is_playing: player_state.is_playing,
            is_paused: player_state.is_paused,
            active_device_id: cluster.active_device_id,
            private_session,
        })
    }

    /// Wall clock at which position 0 of the current playback was reached.
    pub fn started_at_ms(&self) -> i64 {
        self.timestamp_ms - self.position_ms
    }
}

/// Server clock offset read off the cluster envelope (0 when absent).
pub fn server_timestamp_ms(update: &ClusterUpdate) -> i64 {
    update
        .cluster
        .as_ref()
        .map_or(0, |cluster| cluster.server_timestamp_ms)
}

/// Samples kept for the offset estimate.
const CLOCK_SAMPLES: usize = 8;
/// Offsets beyond this are implausible for an NTP-synced host and are
/// rejected rather than applied.
pub const MAX_PLAUSIBLE_OFFSET_MS: i64 = 300_000;

/// Estimates how far the local wall clock lags Spotify's server clock, so
/// position extrapolation and published `as_of` stamps share the clock of the
/// cluster timestamps they are derived from.
///
/// A sample is `server_timestamp − local receipt time`. The server stamps
/// before sending, so every sample equals the true offset minus that
/// message's network latency: latency only ever pulls a sample *down*. The
/// maximum over a sliding window is therefore the least latency-biased
/// estimate, and the window lets a drifting host clock be tracked.
#[derive(Debug, Default)]
pub struct ServerClock {
    samples: VecDeque<i64>,
    offset_ms: i64,
}

/// Outcome of feeding one server timestamp to the [`ServerClock`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockSample {
    /// No server timestamp on this update.
    Absent,
    Accepted,
    /// Sample beyond [`MAX_PLAUSIBLE_OFFSET_MS`]; carries the raw offset.
    Rejected(i64),
}

impl ServerClock {
    pub fn observe(&mut self, server_timestamp_ms: i64, local_now_ms: i64) -> ClockSample {
        if server_timestamp_ms <= 0 {
            return ClockSample::Absent;
        }
        let sample = server_timestamp_ms - local_now_ms;
        if sample.abs() > MAX_PLAUSIBLE_OFFSET_MS {
            return ClockSample::Rejected(sample);
        }
        if self.samples.len() == CLOCK_SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
        self.offset_ms = self.samples.iter().copied().max().unwrap_or(0);
        ClockSample::Accepted
    }

    pub fn offset_ms(&self) -> i64 {
        self.offset_ms
    }

    /// Local wall clock corrected onto the server clock.
    pub fn now_ms(&self, local_now_ms: i64) -> i64 {
        local_now_ms + self.offset_ms
    }
}

/// A newly detected playback (not yet enriched with metadata).
#[derive(Debug, Clone, PartialEq)]
pub struct NewPlay {
    pub track_uri: String,
    pub kind: MediaKind,
    pub cause: PlayCause,
    pub metadata: HashMap<String, String>,
    /// Wall clock at which position 0 of this playback was reached.
    pub started_at_ms: i64,
    pub duration_ms: i64,
}

/// Why an observed snapshot did NOT produce a new play (for metrics).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Suppressed {
    NotPlaying,
    SameTrack,
    Seek,
}

/// Whether a detected play is a fresh track or a restart of the same one
/// (repeat-one, back-to-back duplicate, manual jump to the start).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayCause {
    NewTrack,
    Restart,
}

#[derive(Debug)]
struct Current {
    track_uri: String,
    /// Wall clock of the last observation, for extrapolating the expected position.
    last_obs_ms: i64,
    last_pos_ms: i64,
    is_paused: bool,
}

/// Position jumps beyond this are treated as seeks rather than drift.
const SEEK_THRESHOLD_MS: i64 = 3_000;
/// A jump back to (near) the start of a track that is — by extrapolation —
/// already this far in counts as a fresh play (repeat-one, manual restart,
/// the same track twice in a row). Extrapolated, not last observed: cluster
/// updates are event-driven, so a track that plays through untouched was last
/// observed near position 0.
const RESTART_POS_MS: i64 = 5_000;
const RESTART_MIN_PREV_MS: i64 = 30_000;

/// Pure state machine turning a stream of cluster snapshots into "new play"
/// decisions. Fed wall-clock time explicitly so tests are deterministic.
#[derive(Debug, Default)]
pub struct PlaybackTracker {
    current: Option<Current>,
}

impl PlaybackTracker {
    /// Forget the current playback, so the next playing snapshot is a new
    /// play (used when playback leaves the observable scope, e.g. a private
    /// session).
    pub fn clear(&mut self) {
        self.current = None;
    }

    pub fn observe(&mut self, snap: &ClusterSnapshot, now_ms: i64) -> Result<NewPlay, Suppressed> {
        if !snap.is_playing || snap.track_uri.is_empty() {
            self.current = None;
            return Err(Suppressed::NotPlaying);
        }

        let pos_ms = if snap.is_paused {
            snap.position_ms
        } else {
            snap.position_ms + (now_ms - snap.timestamp_ms).max(0)
        };

        match &mut self.current {
            Some(cur) if cur.track_uri == snap.track_uri => {
                let expected = if cur.is_paused {
                    cur.last_pos_ms
                } else {
                    cur.last_pos_ms + (now_ms - cur.last_obs_ms)
                };
                let jumped = (pos_ms - expected).abs() > SEEK_THRESHOLD_MS;
                let restarted = jumped && pos_ms < RESTART_POS_MS && expected > RESTART_MIN_PREV_MS;

                if restarted {
                    Ok(self.start(snap, now_ms, pos_ms, PlayCause::Restart))
                } else {
                    cur.last_obs_ms = now_ms;
                    cur.last_pos_ms = pos_ms;
                    cur.is_paused = snap.is_paused;
                    Err(if jumped {
                        Suppressed::Seek
                    } else {
                        Suppressed::SameTrack
                    })
                }
            }
            _ => Ok(self.start(snap, now_ms, pos_ms, PlayCause::NewTrack)),
        }
    }

    fn start(
        &mut self,
        snap: &ClusterSnapshot,
        now_ms: i64,
        pos_ms: i64,
        cause: PlayCause,
    ) -> NewPlay {
        let started_at_ms = snap.started_at_ms();
        self.current = Some(Current {
            track_uri: snap.track_uri.clone(),
            last_obs_ms: now_ms,
            last_pos_ms: pos_ms,
            is_paused: snap.is_paused,
        });
        NewPlay {
            track_uri: snap.track_uri.clone(),
            kind: snap.kind,
            cause,
            metadata: snap.metadata.clone(),
            started_at_ms,
            duration_ms: snap.duration_ms,
        }
    }
}

/// Out-of-range values only occur for absurd clocks; those fall back to now.
fn ms_to_datetime(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
}

impl ClusterSnapshot {
    /// Live playback as of `now_ms`, using the same extrapolation as
    /// `PlaybackTracker::observe`: paused/stopped positions are frozen,
    /// playing positions advance by the wall-clock delta since the server
    /// timestamp. Position clamped to >= 0.
    pub fn playback(&self, now_ms: i64) -> Playback {
        let (state, pos_ms) = if !self.is_playing {
            (PlaybackState::Stopped, self.position_ms)
        } else if self.is_paused {
            (PlaybackState::Paused, self.position_ms)
        } else {
            (
                PlaybackState::Playing,
                self.position_ms + (now_ms - self.timestamp_ms).max(0),
            )
        };
        Playback {
            state,
            position_ms: pos_ms.max(0) as u64,
            as_of: ms_to_datetime(now_ms),
        }
    }
}

/// A publishable change in live playback state (pause/resume/seek/stop).
#[derive(Debug, Clone, PartialEq)]
pub struct StatePatch {
    pub track_uri: String,
    pub playback: Playback,
}

/// Position jumps while paused beyond this are published. A frozen position
/// has no extrapolation error — only the ±1 ms duplicate-update jitter — so
/// the threshold can be much tighter than SEEK_THRESHOLD_MS.
pub const PAUSED_SEEK_MS: i64 = 500;

/// Pure state machine deciding which cluster updates are worth publishing as
/// live-state frames. Additive companion to `PlaybackTracker`: it never
/// influences scrobbling, it only mirrors the cluster state to the website.
#[derive(Debug, Default)]
pub struct LiveStateTracker {
    last: Option<Live>,
}

#[derive(Debug)]
struct Live {
    track_uri: String,
    state: PlaybackState,
    /// Published position as of `as_of_ms` (already extrapolated).
    position_ms: i64,
    as_of_ms: i64,
}

impl LiveStateTracker {
    /// `snap == None` is the no-track path (cluster update without a usable
    /// track): treated as a stop of the last known track — nothing to report
    /// if no track was ever seen.
    ///
    /// Internal state always advances; a patch is returned iff the change is
    /// worth publishing: first observation, state flip, track change, or a
    /// position jump beyond the seek thresholds. Exact duplicate updates
    /// (cluster sends DEVICE_STATE_CHANGED twice) never re-publish.
    pub fn observe(&mut self, snap: Option<&ClusterSnapshot>, now_ms: i64) -> Option<StatePatch> {
        let (track_uri, playback) = match snap {
            Some(s) => (s.track_uri.clone(), s.playback(now_ms)),
            None => {
                let last = self.last.as_ref()?;
                (
                    last.track_uri.clone(),
                    Playback {
                        state: PlaybackState::Stopped,
                        position_ms: last.position_ms.max(0) as u64,
                        as_of: ms_to_datetime(now_ms),
                    },
                )
            }
        };

        let publish = match &self.last {
            None => true,
            Some(last) if last.track_uri != track_uri || last.state != playback.state => true,
            Some(last) => match playback.state {
                PlaybackState::Playing => {
                    let expected = last.position_ms + (now_ms - last.as_of_ms);
                    (playback.position_ms as i64 - expected).abs() > SEEK_THRESHOLD_MS
                }
                PlaybackState::Paused => {
                    (playback.position_ms as i64 - last.position_ms).abs() > PAUSED_SEEK_MS
                }
                PlaybackState::Stopped => false,
            },
        };

        self.last = Some(Live {
            track_uri: track_uri.clone(),
            state: playback.state,
            position_ms: playback.position_ms as i64,
            as_of_ms: now_ms,
        });

        publish.then_some(StatePatch {
            track_uri,
            playback,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(uri: &str, timestamp_ms: i64, position_ms: i64, paused: bool) -> ClusterSnapshot {
        ClusterSnapshot {
            track_uri: uri.into(),
            kind: MediaKind::Track,
            metadata: HashMap::new(),
            timestamp_ms,
            position_ms,
            duration_ms: 200_000,
            is_playing: true,
            is_paused: paused,
            active_device_id: "phone".into(),
            private_session: false,
        }
    }

    #[test]
    fn first_snapshot_is_a_new_play_with_derived_start() {
        let mut t = PlaybackTracker::default();
        let play = t.observe(
            &snap("spotify:track:a", 1_042_000, 42_000, false),
            1_042_100,
        );
        assert_eq!(play.unwrap().started_at_ms, 1_000_000);
    }

    #[test]
    fn track_change_is_a_new_play() {
        let mut t = PlaybackTracker::default();
        t.observe(&snap("spotify:track:a", 1_000_000, 0, false), 1_000_000)
            .unwrap();
        let play = t.observe(&snap("spotify:track:b", 1_180_000, 0, false), 1_180_000);
        let play = play.unwrap();
        assert_eq!(play.track_uri, "spotify:track:b");
        assert_eq!(play.started_at_ms, 1_180_000);
    }

    #[test]
    fn pause_and_resume_do_not_emit() {
        let mut t = PlaybackTracker::default();
        t.observe(&snap("spotify:track:a", 1_000_000, 0, false), 1_000_000)
            .unwrap();
        assert_eq!(
            t.observe(&snap("spotify:track:a", 1_050_000, 50_000, true), 1_050_000),
            Err(Suppressed::SameTrack)
        );
        assert_eq!(
            t.observe(
                &snap("spotify:track:a", 1_080_000, 50_000, false),
                1_080_000
            ),
            Err(Suppressed::SameTrack)
        );
    }

    #[test]
    fn seek_forward_is_suppressed_and_does_not_shift_start() {
        let mut t = PlaybackTracker::default();
        let play = t
            .observe(&snap("spotify:track:a", 1_000_000, 0, false), 1_000_000)
            .unwrap();
        assert_eq!(play.started_at_ms, 1_000_000);
        assert_eq!(
            t.observe(
                &snap("spotify:track:a", 1_020_000, 120_000, false),
                1_020_000
            ),
            Err(Suppressed::Seek)
        );
    }

    #[test]
    fn restart_after_30s_is_a_new_play() {
        let mut t = PlaybackTracker::default();
        t.observe(&snap("spotify:track:a", 1_000_000, 0, false), 1_000_000)
            .unwrap();
        t.observe(
            &snap("spotify:track:a", 1_040_000, 40_000, false),
            1_040_000,
        )
        .unwrap_err();
        let play = t.observe(&snap("spotify:track:a", 1_041_000, 0, false), 1_041_000);
        assert_eq!(play.unwrap().started_at_ms, 1_041_000);
    }

    #[test]
    fn early_seek_back_is_not_a_restart() {
        let mut t = PlaybackTracker::default();
        t.observe(&snap("spotify:track:a", 1_000_000, 0, false), 1_000_000)
            .unwrap();
        assert_eq!(
            t.observe(&snap("spotify:track:a", 1_010_000, 0, false), 1_010_000),
            Err(Suppressed::Seek)
        );
    }

    #[test]
    fn stop_clears_state_so_replay_is_new() {
        let mut t = PlaybackTracker::default();
        t.observe(&snap("spotify:track:a", 1_000_000, 0, false), 1_000_000)
            .unwrap();
        let mut stopped = snap("spotify:track:a", 1_050_000, 50_000, false);
        stopped.is_playing = false;
        assert_eq!(t.observe(&stopped, 1_050_000), Err(Suppressed::NotPlaying));
        let play = t.observe(&snap("spotify:track:a", 1_500_000, 0, false), 1_500_000);
        assert_eq!(play.unwrap().started_at_ms, 1_500_000);
    }

    /// Real clusters send DEVICE_STATE_CHANGED twice, sometimes with a
    /// timestamp differing by 1 ms — that must never count as a second play.
    #[test]
    fn exact_duplicate_update_is_same_track() {
        let mut t = PlaybackTracker::default();
        t.observe(
            &snap("spotify:track:a", 1_785_245_273_696, 170_791, true),
            1_785_245_273_700,
        )
        .unwrap();
        assert_eq!(
            t.observe(
                &snap("spotify:track:a", 1_785_245_273_695, 170_791, true),
                1_785_245_273_800
            ),
            Err(Suppressed::SameTrack)
        );
    }

    /// App startup while a track has been paused for hours: the snapshot
    /// carries a stale timestamp, but the play is still reported (it IS the
    /// current track) with `started_at` in the past — and the later resume
    /// must not produce another play.
    #[test]
    fn startup_into_paused_track_derives_past_start() {
        let mut t = PlaybackTracker::default();
        let paused_since = 1_785_235_636_611;
        let play = t
            .observe(
                &snap("spotify:track:a", paused_since, 166_050, true),
                1_785_245_268_000,
            )
            .unwrap();
        assert_eq!(play.started_at_ms, paused_since - 166_050);
        assert_eq!(
            t.observe(
                &snap("spotify:track:a", 1_785_245_268_861, 166_050, false),
                1_785_245_268_900
            ),
            Err(Suppressed::SameTrack)
        );
    }

    #[test]
    fn snapshot_extraction_from_protobuf() {
        use librespot::protocol::connect::{Cluster, ClusterUpdate};
        use librespot::protocol::player::{PlayerState, ProvidedTrack};

        let mut track = ProvidedTrack {
            uri: "spotify:track:3dxiWIBVJRlqh9xk144rf4".to_string(),
            ..Default::default()
        };
        track
            .metadata
            .insert("title".to_string(), "Breaking the Habit".to_string());
        let mut ps = PlayerState {
            timestamp: 1_785_235_636_611,
            position_as_of_timestamp: 166_050,
            duration: 196_133,
            is_playing: true,
            is_paused: true,
            ..Default::default()
        };
        ps.track.0 = Some(Box::new(track));
        let mut cluster = Cluster {
            active_device_id: "dfa4e1c7".to_string(),
            ..Default::default()
        };
        cluster.player_state.0 = Some(Box::new(ps));
        let mut update = ClusterUpdate::default();
        update.cluster.0 = Some(Box::new(cluster));

        let snap = ClusterSnapshot::from_update(update).unwrap();
        assert_eq!(snap.kind, MediaKind::Track);
        assert!(!snap.private_session);
        assert_eq!(snap.track_uri, "spotify:track:3dxiWIBVJRlqh9xk144rf4");
        assert_eq!(snap.metadata["title"], "Breaking the Habit");
        assert_eq!(snap.timestamp_ms, 1_785_235_636_611);
        assert_eq!(snap.position_ms, 166_050);
        assert_eq!(snap.duration_ms, 196_133);
        assert!(snap.is_playing && snap.is_paused);
        assert_eq!(snap.active_device_id, "dfa4e1c7");
    }

    #[test]
    fn snapshot_extraction_rejects_incomplete_updates() {
        use librespot::protocol::connect::{Cluster, ClusterUpdate};
        use librespot::protocol::player::{PlayerState, ProvidedTrack};

        assert_eq!(
            ClusterSnapshot::from_update(ClusterUpdate::default()).unwrap_err(),
            NoSnapshot::NoTrack
        );

        let mut update = ClusterUpdate::default();
        update.cluster.0 = Some(Box::new(Cluster::default()));
        assert_eq!(
            ClusterSnapshot::from_update(update).unwrap_err(),
            NoSnapshot::NoTrack
        );

        let mut ps = PlayerState::default();
        ps.track.0 = Some(Box::new(ProvidedTrack::default()));
        let mut cluster = Cluster::default();
        cluster.player_state.0 = Some(Box::new(ps));
        let mut update = ClusterUpdate::default();
        update.cluster.0 = Some(Box::new(cluster));
        assert_eq!(
            ClusterSnapshot::from_update(update).unwrap_err(),
            NoSnapshot::NoTrack
        );
    }

    fn update_with(uri: &str, private_session: Option<bool>) -> ClusterUpdate {
        use librespot::protocol::connect::{Cluster, DeviceInfo};
        use librespot::protocol::player::{PlayerState, ProvidedTrack};

        let mut ps = PlayerState {
            is_playing: true,
            ..Default::default()
        };
        ps.track.0 = Some(Box::new(ProvidedTrack {
            uri: uri.to_string(),
            ..Default::default()
        }));
        let mut cluster = Cluster {
            active_device_id: "phone".to_string(),
            server_timestamp_ms: 1_785_000_000_123,
            ..Default::default()
        };
        if let Some(private) = private_session {
            cluster.device.insert(
                "phone".to_string(),
                DeviceInfo {
                    is_private_session: private,
                    ..Default::default()
                },
            );
        }
        cluster.player_state.0 = Some(Box::new(ps));
        let mut update = ClusterUpdate::default();
        update.cluster.0 = Some(Box::new(cluster));
        update
    }

    #[test]
    fn snapshot_classifies_media_kinds() {
        let kind = |uri| ClusterSnapshot::from_update(update_with(uri, None)).map(|s| s.kind);
        assert_eq!(
            kind("spotify:track:4uLU6hMCjMI75M1A2tKUQC"),
            Ok(MediaKind::Track)
        );
        assert_eq!(
            kind("spotify:episode:512ojhOuo1ktJprKbVcKyQ"),
            Ok(MediaKind::Episode)
        );
        assert_eq!(
            kind("spotify:local:David+Wise:Donkey+Kong:Snomads+Island:127"),
            Ok(MediaKind::Local)
        );
        assert_eq!(
            kind("spotify:ad:5sWHDYs0csV6RS48xBl0tH"),
            Err(NoSnapshot::Unsupported("ad".into()))
        );
    }

    #[test]
    fn snapshot_reads_private_session_of_the_active_device() {
        let uri = "spotify:track:4uLU6hMCjMI75M1A2tKUQC";
        let private = |flag| {
            ClusterSnapshot::from_update(update_with(uri, flag))
                .unwrap()
                .private_session
        };
        assert!(private(Some(true)));
        assert!(!private(Some(false)));
        assert!(!private(None), "missing device info must read as public");
        assert_eq!(
            server_timestamp_ms(&update_with(uri, None)),
            1_785_000_000_123
        );
        assert_eq!(server_timestamp_ms(&ClusterUpdate::default()), 0);
    }

    /// Regression: cluster updates are event-driven, so a track that plays
    /// through untouched was last observed at position ~0. Its repeat-one
    /// restart must still count, judged by the extrapolated position.
    #[test]
    fn repeat_one_after_uninterrupted_playthrough_is_a_new_play() {
        let mut t = PlaybackTracker::default();
        t.observe(&snap("spotify:track:a", 1_000_000, 0, false), 1_000_000)
            .unwrap();
        let play = t
            .observe(&snap("spotify:track:a", 1_200_500, 0, false), 1_200_500)
            .unwrap();
        assert_eq!(play.cause, PlayCause::Restart);
        assert_eq!(play.started_at_ms, 1_200_500);
    }

    /// The same track queued twice in a row: the only visible signal is the
    /// position falling back to ~0 after a full play-through.
    #[test]
    fn back_to_back_duplicate_track_counts_twice() {
        let mut t = PlaybackTracker::default();
        let first = t
            .observe(&snap("spotify:track:a", 1_000_000, 0, false), 1_000_000)
            .unwrap();
        assert_eq!(first.cause, PlayCause::NewTrack);
        let second = t
            .observe(&snap("spotify:track:a", 1_201_000, 800, false), 1_201_100)
            .unwrap();
        assert_eq!(second.cause, PlayCause::Restart);
    }

    #[test]
    fn server_clock_takes_the_least_latency_biased_sample() {
        let mut c = ServerClock::default();
        assert_eq!(c.observe(0, 1_000), ClockSample::Absent);
        assert_eq!(c.now_ms(1_000), 1_000);

        assert_eq!(c.observe(10_450, 10_000), ClockSample::Accepted);
        assert_eq!(c.observe(20_380, 20_000), ClockSample::Accepted);
        assert_eq!(c.offset_ms(), 450);
        assert_eq!(c.now_ms(30_000), 30_450);

        assert_eq!(
            c.observe(10_000 + MAX_PLAUSIBLE_OFFSET_MS + 1, 10_000),
            ClockSample::Rejected(MAX_PLAUSIBLE_OFFSET_MS + 1)
        );
        assert_eq!(c.offset_ms(), 450);
    }

    #[test]
    fn server_clock_window_tracks_drift() {
        let mut c = ServerClock::default();
        c.observe(5_000, 0);
        for i in 1..=CLOCK_SAMPLES as i64 {
            c.observe(i * 1_000 + 200, i * 1_000);
        }
        assert_eq!(c.offset_ms(), 200, "old outlier must age out of the window");
    }

    #[test]
    fn drift_within_threshold_is_same_track() {
        let mut t = PlaybackTracker::default();
        t.observe(&snap("spotify:track:a", 1_000_000, 0, false), 1_000_000)
            .unwrap();
        assert_eq!(
            t.observe(
                &snap("spotify:track:a", 1_060_000, 61_500, false),
                1_060_000
            ),
            Err(Suppressed::SameTrack)
        );
    }

    #[test]
    fn playback_extrapolates_playing_and_freezes_paused() {
        let p = snap("spotify:track:a", 1_000_000, 42_000, false).playback(1_000_100);
        assert_eq!((p.state, p.position_ms), (PlaybackState::Playing, 42_100));

        let p = snap("spotify:track:a", 1_000_000, 42_000, true).playback(5_000_000);
        assert_eq!((p.state, p.position_ms), (PlaybackState::Paused, 42_000));

        let mut s = snap("spotify:track:a", 1_000_000, 42_000, false);
        s.is_playing = false;
        let p = s.playback(5_000_000);
        assert_eq!((p.state, p.position_ms), (PlaybackState::Stopped, 42_000));

        let mut s = snap("spotify:track:a", 2_000_000, -500, false);
        s.is_paused = true;
        assert_eq!(s.playback(1_000_000).position_ms, 0);
    }

    fn observe(
        t: &mut LiveStateTracker,
        uri: &str,
        timestamp_ms: i64,
        position_ms: i64,
        paused: bool,
        now_ms: i64,
    ) -> Option<StatePatch> {
        t.observe(Some(&snap(uri, timestamp_ms, position_ms, paused)), now_ms)
    }

    #[test]
    fn live_pause_and_resume_emit_once_each() {
        let mut t = LiveStateTracker::default();
        let p = observe(&mut t, "spotify:track:a", 1_000_000, 0, false, 1_000_000).unwrap();
        assert_eq!(p.playback.state, PlaybackState::Playing);

        let p = observe(
            &mut t,
            "spotify:track:a",
            1_050_000,
            50_000,
            true,
            1_050_000,
        )
        .unwrap();
        assert_eq!(
            (p.playback.state, p.playback.position_ms),
            (PlaybackState::Paused, 50_000)
        );

        assert!(
            observe(
                &mut t,
                "spotify:track:a",
                1_049_999,
                50_000,
                true,
                1_050_100
            )
            .is_none()
        );

        let p = observe(
            &mut t,
            "spotify:track:a",
            1_080_000,
            50_000,
            false,
            1_080_000,
        )
        .unwrap();
        assert_eq!(p.playback.state, PlaybackState::Playing);
    }

    #[test]
    fn live_seek_thresholds() {
        let mut t = LiveStateTracker::default();
        observe(&mut t, "spotify:track:a", 1_000_000, 0, false, 1_000_000).unwrap();

        assert!(
            observe(
                &mut t,
                "spotify:track:a",
                1_060_000,
                61_500,
                false,
                1_060_000
            )
            .is_none()
        );

        let p = observe(
            &mut t,
            "spotify:track:a",
            1_070_000,
            120_000,
            false,
            1_070_000,
        )
        .unwrap();
        assert_eq!(p.playback.position_ms, 120_000);

        observe(
            &mut t,
            "spotify:track:a",
            1_080_000,
            130_000,
            true,
            1_080_000,
        )
        .unwrap();
        assert!(
            observe(
                &mut t,
                "spotify:track:a",
                1_080_001,
                130_001,
                true,
                1_080_100
            )
            .is_none()
        );
        let p = observe(
            &mut t,
            "spotify:track:a",
            1_081_000,
            132_000,
            true,
            1_081_000,
        )
        .unwrap();
        assert_eq!(
            (p.playback.state, p.playback.position_ms),
            (PlaybackState::Paused, 132_000)
        );
    }

    #[test]
    fn live_stop_emits_once_and_track_change_emits() {
        let mut t = LiveStateTracker::default();
        observe(&mut t, "spotify:track:a", 1_000_000, 0, false, 1_000_000).unwrap();

        let mut stopped = snap("spotify:track:a", 1_050_000, 50_000, false);
        stopped.is_playing = false;
        let p = t.observe(Some(&stopped), 1_050_000).unwrap();
        assert_eq!(p.playback.state, PlaybackState::Stopped);
        assert!(t.observe(Some(&stopped), 1_051_000).is_none());

        let p = observe(&mut t, "spotify:track:b", 1_100_000, 0, false, 1_100_000).unwrap();
        assert_eq!(p.track_uri, "spotify:track:b");
    }

    #[test]
    fn live_no_track_updates_stop_the_last_known_track() {
        let mut t = LiveStateTracker::default();
        assert!(t.observe(None, 1_000_000).is_none());

        observe(
            &mut t,
            "spotify:track:a",
            1_000_000,
            30_000,
            true,
            1_000_000,
        )
        .unwrap();
        let p = t.observe(None, 1_010_000).unwrap();
        assert_eq!(p.track_uri, "spotify:track:a");
        assert_eq!(
            (p.playback.state, p.playback.position_ms),
            (PlaybackState::Stopped, 30_000)
        );
        assert!(t.observe(None, 1_020_000).is_none());
    }

    mod properties {
        use proptest::prelude::*;

        use super::*;

        const T0: i64 = 1_785_000_000_000;

        #[derive(Debug, Clone, Copy)]
        enum Action {
            Pause,
            Resume,
            Duplicate,
        }

        fn action() -> impl Strategy<Value = Action> {
            prop_oneof![
                Just(Action::Pause),
                Just(Action::Resume),
                Just(Action::Duplicate)
            ]
        }

        fn at(uri: &str, now: i64, pos: i64, paused: bool, playing: bool) -> ClusterSnapshot {
            let mut s = snap(uri, now, pos, paused);
            s.is_playing = playing;
            s
        }

        proptest! {
            /// Pause/resume/duplicate updates with a physically consistent
            /// position never produce a second play of the same playback.
            #[test]
            fn consistent_same_track_updates_yield_exactly_one_play(
                steps in proptest::collection::vec((0i64..20_000, action()), 1..40),
            ) {
                let mut t = PlaybackTracker::default();
                let (mut now, mut pos, mut paused) = (T0, 0i64, false);
                prop_assert!(t.observe(&at("spotify:track:a", now, pos, paused, true), now).is_ok());
                for (dt, action) in steps {
                    now += dt;
                    if !paused {
                        pos += dt;
                    }
                    match action {
                        Action::Pause => paused = true,
                        Action::Resume => paused = false,
                        Action::Duplicate => {}
                    }
                    let seen = t.observe(&at("spotify:track:a", now, pos, paused, true), now);
                    prop_assert_eq!(seen, Err(Suppressed::SameTrack));
                }
            }

            /// Seeks anywhere except back to the very start never count as a
            /// new play, so `started_at` of the playback stays fixed.
            #[test]
            fn seeks_never_restart_the_playback(
                seeks in proptest::collection::vec((0i64..20_000, RESTART_POS_MS..600_000), 1..30),
            ) {
                let mut t = PlaybackTracker::default();
                let mut now = T0;
                let first = t.observe(&at("spotify:track:a", now, 0, false, true), now).unwrap();
                prop_assert_eq!(first.started_at_ms, T0);
                for (dt, target) in seeks {
                    now += dt;
                    prop_assert!(t.observe(&at("spotify:track:a", now, target, false, true), now).is_err());
                }
            }

            /// A full untouched play-through followed by a jump to the start
            /// is exactly two plays, however long the track and however the
            /// restart update is delayed.
            #[test]
            fn playthrough_then_restart_is_two_plays(
                duration in (RESTART_MIN_PREV_MS + 1)..900_000,
                lag in 0i64..3_000,
                restart_pos in 0i64..RESTART_POS_MS,
            ) {
                let mut t = PlaybackTracker::default();
                prop_assert!(t.observe(&at("spotify:track:a", T0, 0, false, true), T0).is_ok());
                let now = T0 + duration + lag;
                let second = t.observe(&at("spotify:track:a", now, restart_pos, false, true), now);
                prop_assert_eq!(second.map(|p| p.cause), Ok(PlayCause::Restart));
            }

            /// Live state publishes on the first observation and on every
            /// state flip, never for repeats of an unchanged state.
            #[test]
            fn live_state_publishes_exactly_the_flips(
                steps in proptest::collection::vec((0i64..20_000, 0u8..3), 1..40),
            ) {
                let mut t = LiveStateTracker::default();
                let (mut now, mut pos) = (T0, 0i64);
                let mut prev: Option<u8> = None;
                for (dt, state) in steps {
                    now += dt;
                    if prev == Some(0) {
                        pos += dt;
                    }
                    let (playing, paused) = match state {
                        0 => (true, false),
                        1 => (true, true),
                        _ => (false, false),
                    };
                    let patch = t.observe(Some(&at("spotify:track:a", now, pos, paused, playing)), now);
                    prop_assert_eq!(patch.is_some(), prev != Some(state));
                    prev = Some(state);
                }
            }
        }
    }
}
