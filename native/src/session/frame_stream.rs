pub(crate) const MAGIC: &[u8; 8] = b"YLXFRM0\n";
pub(crate) const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FrameStreamError {
    InvalidLength,
}

pub(crate) fn encode(payload: &[u8]) -> Result<Vec<u8>, FrameStreamError> {
    if payload.is_empty() || payload.len() > MAX_FRAME_BYTES {
        return Err(FrameStreamError::InvalidLength);
    }
    let length = u32::try_from(payload.len()).map_err(|_| FrameStreamError::InvalidLength)?;
    let mut encoded = Vec::with_capacity(4 + payload.len());
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(payload);
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::{FrameStreamError, MAGIC, MAX_FRAME_BYTES, encode};

    #[test]
    fn encoding_is_big_endian_and_bounded() {
        assert_eq!(encode(b"jpeg").unwrap(), b"\0\0\0\x04jpeg");
        assert_eq!(encode(b""), Err(FrameStreamError::InvalidLength));
        assert_eq!(
            encode(&vec![0; MAX_FRAME_BYTES + 1]),
            Err(FrameStreamError::InvalidLength)
        );
        assert_eq!(MAGIC, b"YLXFRM0\n");
    }
}
