const SOI: [u8; 2] = [0xff, 0xd8];
const EOI: [u8; 2] = [0xff, 0xd9];

pub(crate) fn ranges(payload: &[u8]) -> Vec<(usize, usize)> {
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
        let Some(end_offset) = payload[start + 2..]
            .windows(2)
            .position(|window| window == EOI)
        else {
            break;
        };
        let end = start + 2 + end_offset + 2;
        result.push((start, end));
        cursor = end;
    }
    result
}

pub(crate) fn dimensions(payload: &[u8]) -> Option<(u16, u16)> {
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
}
