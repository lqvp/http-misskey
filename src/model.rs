use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct Context {
    pub previous_id: usize,
    pub current_id: usize,
    pub connection_count: usize,
}

#[derive(Debug, Clone)]
pub struct MisskeyData {
    pub html: Bytes,
}
