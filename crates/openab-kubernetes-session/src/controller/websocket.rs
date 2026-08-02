use crate::wire::{MAX_ACP_FRAME_BYTES, MAX_CONTROL_FRAME_BYTES};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

pub(super) fn relay_websocket_config() -> WebSocketConfig {
    WebSocketConfig {
        write_buffer_size: 0,
        max_write_buffer_size: MAX_ACP_FRAME_BYTES + MAX_CONTROL_FRAME_BYTES,
        max_message_size: Some(MAX_ACP_FRAME_BYTES),
        max_frame_size: Some(MAX_ACP_FRAME_BYTES),
        accept_unmasked_frames: false,
        ..WebSocketConfig::default()
    }
}
