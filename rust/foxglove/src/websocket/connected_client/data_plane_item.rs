use bytes::Bytes;
use tokio_tungstenite::tungstenite::Message;

use crate::websocket::ws_protocol::server::MessageData;

/// An item queued on a [`ConnectedClient`](super::ConnectedClient)'s data plane channel.
///
/// Most data plane traffic is already a fully-framed [`Message`] by the time it's queued (e.g. an
/// info-level [`Status`](crate::websocket::Status)). Log data is the exception: rather than
/// framing a full, owned wire message for every connected client synchronously on the calling
/// (usually application/ROS logging) thread, [`ConnectedClient::log_shared`](
/// super::ConnectedClient::log_shared) queues the small, per-client header alongside a cheaply
/// cloned, shared [`Bytes`] payload. The final per-client frame (header + payload) is assembled
/// here, in [`DataPlaneItem::into_message`], which is only ever called from the client's own
/// poller task immediately before writing to its socket -- never on the logging thread.
pub(in crate::websocket::connected_client) enum DataPlaneItem {
    /// A message that has already been framed and just needs to be sent as-is.
    Framed(Message),
    /// Log data awaiting final, per-client frame assembly (subscription id + log time + shared
    /// payload).
    LogData {
        subscription_id: u32,
        log_time: u64,
        payload: Bytes,
    },
}

impl DataPlaneItem {
    /// Assembles the final wire message, framing the per-client header together with the (cheaply
    /// shared, but here finally copied into the outgoing frame buffer) payload.
    ///
    /// This is the same single copy that the SDK has always performed to build the outgoing
    /// WebSocket frame; the fix this type is part of is about *where* that copy happens (the
    /// receiving client's own task) and ensuring the payload it copies from was itself only
    /// copied out of the caller-provided buffer once, not once per connected client.
    pub(in crate::websocket::connected_client) fn into_message(self) -> Message {
        match self {
            DataPlaneItem::Framed(message) => message,
            DataPlaneItem::LogData {
                subscription_id,
                log_time,
                payload,
            } => Message::from(&MessageData::new(
                subscription_id,
                log_time,
                payload.as_ref(),
            )),
        }
    }
}

impl From<Message> for DataPlaneItem {
    fn from(message: Message) -> Self {
        DataPlaneItem::Framed(message)
    }
}
