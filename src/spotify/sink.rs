use librespot::playback::audio_backend::{Sink, SinkResult};
use librespot::playback::convert::Converter;
use librespot::playback::decoder::AudioPacket;

/// Discards all audio. This app never renders sound — it only observes the
/// Connect cluster — but librespot's Player requires a sink.
pub struct NoopSink;

impl Sink for NoopSink {
    fn write(&mut self, _packet: AudioPacket, _converter: &mut Converter) -> SinkResult<()> {
        Ok(())
    }
}
