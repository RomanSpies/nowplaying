use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::Serialize;

/// A single playback of a track, as pushed to WSS clients and stored in Postgres.
#[derive(Debug, Clone, Serialize)]
pub struct PlayEvent {
    pub track_id: String,
    pub track_url: String,
    pub title: String,
    pub artists: Vec<String>,
    pub album: String,
    pub cover_url: Option<String>,
    pub duration_ms: u32,
    pub started_at: DateTime<Utc>,
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

/// Which kind of wire frame a broadcast item carries. The WS connect-race
/// dedupe only ever skips `Play` frames (see web::ws::should_skip).
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
            track_url: "https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC".into(),
            title: "Never Gonna Give You Up".into(),
            artists: vec!["Rick Astley".into()],
            album: "Whenever You Need Somebody".into(),
            cover_url: Some("https://i.scdn.co/image/abc".into()),
            duration_ms: 213_000,
            started_at: "2026-07-28T12:00:00Z".parse().unwrap(),
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
