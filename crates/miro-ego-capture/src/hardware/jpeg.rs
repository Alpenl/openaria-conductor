const SOI: [u8; 2] = [0xff, 0xd8];

pub fn ranges(payload: &[u8]) -> Vec<(usize, usize)> {
    let mut result = Vec::new();
    let mut cursor = 0;
    while cursor + 1 < payload.len() {
        let Some(start_offset) = payload[cursor..]
            .windows(2)
            .position(|window| window == SOI)
        else {
            break;
        };
        let start = cursor + start_offset;
        match find_jpeg_end(payload, start) {
            Ok(end) => {
                result.push((start, end));
                cursor = end;
            }
            Err(resume_at) => {
                cursor = resume_at.max(start + 2);
            }
        }
    }
    result
}

/// Locate the outer EOI (`FF D9`) matching `SOI` at `start`, skipping 16-bit
/// length-prefixed marker segments (such as `APP1` EXIF thumbnails containing
/// nested `FF D8 ... FF D9` images) before `SOS` (`FF DA`).
fn find_jpeg_end(payload: &[u8], start: usize) -> Result<usize, usize> {
    let mut cursor = start + 2;
    while cursor < payload.len() {
        if payload[cursor] != 0xff {
            cursor += 1;
            continue;
        }
        let ff_start = cursor;
        while cursor < payload.len() && payload[cursor] == 0xff {
            cursor += 1;
        }
        let Some(&marker) = payload.get(cursor) else {
            return Err(payload.len());
        };
        cursor += 1;
        match marker {
            // Byte-stuffed 0xFF 0x00, TEM (0x01), or RST0..RST7: standalone, no length.
            0x00 | 0x01 | 0xd0..=0xd7 => continue,
            // Outer End of Image.
            0xd9 => return Ok(cursor),
            // Unescaped Start of Image outside a length-prefixed segment: prior JPEG had no EOI.
            0xd8 => return Err(ff_start),
            // Start of Scan: skip 16-bit SOS header, then scan entropy-coded data.
            0xda => {
                let (Some(&hi), Some(&lo)) = (payload.get(cursor), payload.get(cursor + 1)) else {
                    return Err(payload.len());
                };
                let len = usize::from(u16::from_be_bytes([hi, lo]));
                if len < 2 || cursor + len > payload.len() {
                    return Err(payload.len());
                }
                cursor += len;
            }
            // Length-prefixed marker segment (APP0..APP15, DQT, DHT, SOFn, COM, etc.).
            _ => {
                let (Some(&hi), Some(&lo)) = (payload.get(cursor), payload.get(cursor + 1)) else {
                    return Err(payload.len());
                };
                let len = usize::from(u16::from_be_bytes([hi, lo]));
                if len < 2 || cursor + len > payload.len() {
                    return Err(payload.len());
                }
                cursor += len;
            }
        }
    }
    Err(payload.len())
}

pub fn dimensions(payload: &[u8]) -> Option<(u16, u16)> {
    if !payload.starts_with(&SOI) {
        return None;
    }
    let mut cursor = 2;
    while cursor + 4 <= payload.len() {
        if payload[cursor] != 0xff {
            cursor += 1;
            continue;
        }
        while cursor < payload.len() && payload[cursor] == 0xff {
            cursor += 1;
        }
        let marker = *payload.get(cursor)?;
        cursor += 1;
        if matches!(marker, 0xd8 | 0xd9) {
            continue;
        }
        if marker == 0xda {
            return None;
        }
        let length = u16::from_be_bytes([*payload.get(cursor)?, *payload.get(cursor + 1)?]);
        let length = usize::from(length);
        if length < 2 || cursor + length > payload.len() {
            return None;
        }
        if matches!(
            marker,
            0xc0 | 0xc1
                | 0xc2
                | 0xc3
                | 0xc5
                | 0xc6
                | 0xc7
                | 0xc9
                | 0xca
                | 0xcb
                | 0xcd
                | 0xce
                | 0xcf
        ) {
            if length < 7 {
                return None;
            }
            let height = u16::from_be_bytes([payload[cursor + 3], payload[cursor + 4]]);
            let width = u16::from_be_bytes([payload[cursor + 5], payload[cursor + 6]]);
            return Some((width, height));
        }
        cursor += length;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{dimensions, ranges};

    #[test]
    fn marker_ranges_tolerate_padding_and_reject_incomplete_tail() {
        let payload = b"pad\xff\xd8left\xff\xd9\0\xff\xd8right\xff\xd9\xff\xd8bad";
        assert_eq!(ranges(payload), vec![(3, 11), (12, 21)]);
    }

    #[test]
    fn sof_dimensions_are_read_without_decoding() {
        let payload =
            b"\xff\xd8\xff\xc0\0\x11\x08\x04\x38\x07\x80\x03\x01\x11\0\x02\x11\0\x03\x11\0\xff\xd9";
        assert_eq!(dimensions(payload), Some((1920, 1080)));
        assert_eq!(dimensions(b"not-jpeg"), None);
    }

    #[test]
    fn ranges_skip_embedded_app1_exif_thumbnail_eoi_markers() {
        // APP1 segment containing an embedded thumbnail JPEG (\xff\xd8...\xff\xd9)
        let thumb = b"Exif\0\0\xff\xd8thumb\xff\xd9";
        let app1_len = (2 + thumb.len()) as u16;
        let mut jpeg = Vec::new();
        jpeg.extend_from_slice(b"\xff\xd8\xff\xe1");
        jpeg.extend_from_slice(&app1_len.to_be_bytes());
        jpeg.extend_from_slice(thumb);
        jpeg.extend_from_slice(
            b"\xff\xc0\0\x11\x08\x04\x38\x0f\x00\x03\x01\x11\0\x02\x11\0\x03\x11\0",
        );
        jpeg.extend_from_slice(b"\xff\xda\0\x08\x01\x01\0\0\x3f\0\x12\xff\x00\x34\xff\xd9");

        assert_eq!(ranges(&jpeg), vec![(0, jpeg.len())]);
        assert_eq!(dimensions(&jpeg), Some((3840, 1080)));

        // Truncated outer scan must NOT fall back to extracting the embedded thumbnail
        let truncated = &jpeg[..jpeg.len() - 2];
        assert!(ranges(truncated).is_empty());
    }
}
