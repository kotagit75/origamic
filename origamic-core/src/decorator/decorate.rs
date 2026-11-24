use tokio::io::ReadBuf;

pub trait Decorate: Unpin + Sync + Send + 'static + Copy {
    fn from_upper(buf: &[u8]) -> Vec<u8>;
    fn from_lower(buf: &mut ReadBuf<'_>);
}
