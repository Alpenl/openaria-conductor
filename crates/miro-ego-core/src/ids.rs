//! UUIDv4/v7, SHA-256, and bearer-token validators and generators.

use uuid::Uuid;

pub const CUSTOMER_BEARER_TOKEN_MIN_LENGTH: usize = 8;
pub const CUSTOMER_BEARER_TOKEN_MAX_LENGTH: usize = 512;

/// Generate a canonical lowercase hyphenated UUIDv4 string.
pub fn new_uuid_v4() -> String {
    Uuid::new_v4().hyphenated().to_string()
}

/// Generate a canonical lowercase hyphenated UUIDv7 string.
pub fn new_uuid_v7() -> String {
    Uuid::now_v7().hyphenated().to_string()
}

/// Return `true` if `value` is a strictly lowercase canonical UUIDv4 string.
pub fn is_valid_uuid_v4(value: &str) -> bool {
    is_valid_uuid_version(value, b'4')
}

/// Return `true` if `value` is a strictly lowercase canonical UUIDv7 string.
pub fn is_valid_uuid_v7(value: &str) -> bool {
    is_valid_uuid_version(value, b'7')
}

fn is_valid_uuid_version(value: &str, version_char: u8) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (idx, &byte) in bytes.iter().enumerate() {
        match idx {
            8 | 13 | 18 | 23 => {
                if byte != b'-' {
                    return false;
                }
            }
            14 => {
                if byte != version_char {
                    return false;
                }
            }
            19 => {
                if !matches!(byte, b'8' | b'9' | b'a' | b'b') {
                    return false;
                }
            }
            _ => {
                if !is_lower_hex(byte) {
                    return false;
                }
            }
        }
    }
    true
}

/// Return `true` if `value` is a 64-character lowercase hexadecimal SHA-256 string.
pub fn is_valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(is_lower_hex)
}

/// Return `true` if `value` is a valid customer bearer token (8..=512 visible ASCII chars).
pub fn is_valid_bearer_token(value: &str) -> bool {
    (CUSTOMER_BEARER_TOKEN_MIN_LENGTH..=CUSTOMER_BEARER_TOKEN_MAX_LENGTH).contains(&value.len())
        && value.bytes().all(|b| (0x21..=0x7E).contains(&b))
}

/// Encode bytes into lowercase hexadecimal string.
pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_uuid_v4_and_v7_strictly() {
        let v4 = new_uuid_v4();
        let v7 = new_uuid_v7();
        assert!(is_valid_uuid_v4(&v4));
        assert!(!is_valid_uuid_v7(&v4));
        assert!(is_valid_uuid_v7(&v7));
        assert!(!is_valid_uuid_v4(&v7));

        // Uppercase rejected
        assert!(!is_valid_uuid_v4(&v4.to_uppercase()));
        assert!(!is_valid_uuid_v7(&v7.to_uppercase()));
        // Wrong variant nibble rejected
        assert!(!is_valid_uuid_v4("550e8400-e29b-41d4-0716-446655440000"));
        assert!(!is_valid_uuid_v7("01989f6a-2c00-7a1b-c000-3e4f50617283"));
    }

    #[test]
    fn validates_sha256_and_bearer_tokens() {
        let digest = hex_encode(&[0xab_u8; 32]);
        assert!(is_valid_sha256(&digest));
        assert!(!is_valid_sha256(&digest.to_uppercase()));
        assert!(!is_valid_sha256("abcd"));

        assert!(is_valid_bearer_token("secret-token-1234"));
        assert!(!is_valid_bearer_token("short"));
        assert!(!is_valid_bearer_token("has space inside"));
    }
}
