use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};

pub const NULL: u8 = 0x00;

/// Encode a signed 32 bit integer to `encode_to` in big-endian byte order.
pub fn encode_i32(encode_to: &mut Vec<u8>, value: i32) {
    encode_to.put_i32(value);
}

/// Decode a signed 32 bit integer from `decode_from` in big-endian byte order.
pub fn decode_i32(decode_from: &mut Bytes) -> i32 {
    decode_from.get_i32()
}

/// Encode a bool to `encode_to` in big-endian byte order.
pub fn encode_bool(encode_to: &mut Vec<u8>, value: bool) {
    if value {
        encode_to.put_u8(1)
    } else {
        encode_to.put_u8(0)
    }
}

/// Decode a bool from `decode_from` in big-endian byte order.
pub fn decode_bool(decode_from: &mut Bytes) -> bool {
    let v = decode_from.get_u8();
    if v == 1 {
        true
    } else {
        false
    }
}

/// Encode a UTF-8 string to `encode_to`.
pub fn encode_string(encode_to: &mut Vec<u8>, value: &str) {
    let value = value.as_bytes();
    encode_to.put_u32(value.len() as u32);
    encode_to.extend_from_slice(value);
}

/// Decode a UTF-8 string from `decode_from`. If the string inside `decode_from` is not a valid
/// UTF-8 an error is returned.
pub fn decode_string(decode_from: &mut Bytes) -> Result<String> {
    let len = decode_from.get_u32();

    let mut value = vec![0; len as usize];
    decode_from.copy_to_slice(&mut value);

    let value = String::from_utf8(value)?;
    Ok(value)
}

/// Encode a null value to `encode_to`
pub fn encode_null(encode_to: &mut Vec<u8>) {
    encode_to.put_u8(NULL);
}

/// Decode a NULL value from `decode_from`.
///
/// The u8 value returned should be the same as NULL const.
pub fn decode_null(decode_from: &mut Bytes) -> u8 {
    decode_from.get_u8()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_string() -> Result<()> {
        let mut encode_to = Vec::new();
        let value = String::from("encode_decode_string");

        encode_string(&mut encode_to, &value);

        let mut bytes = Bytes::from(encode_to);
        let value_decoded = decode_string(&mut bytes)?;

        assert_eq!(
            value, value_decoded,
            "Expected equal values when encoding and decoding"
        );

        assert!(bytes.is_empty(), "Expected empty bytes array");

        Ok(())
    }

    #[test]
    fn test_encode_decode_i32() -> Result<()> {
        let mut encode_to = Vec::new();
        let value: i32 = 1000;

        encode_i32(&mut encode_to, value);

        let mut bytes = Bytes::from(encode_to);
        let value_decoded = decode_i32(&mut bytes);

        assert_eq!(
            value, value_decoded,
            "Expected equal values when encoding and decoding"
        );

        assert!(bytes.is_empty(), "Expected empty bytes array");

        Ok(())
    }

    #[test]
    fn test_encode_decode_bool_true() -> Result<()> {
        let mut encode_to = Vec::new();
        let value = true;

        encode_bool(&mut encode_to, value);

        let mut bytes = Bytes::from(encode_to);
        let value_decoded = decode_bool(&mut bytes);

        assert_eq!(
            value, value_decoded,
            "Expected equal values when encoding and decoding"
        );

        assert!(bytes.is_empty(), "Expected empty bytes array");

        Ok(())
    }

    #[test]
    fn test_encode_decode_bool_false() -> Result<()> {
        let mut encode_to = Vec::new();
        let value = false;

        encode_bool(&mut encode_to, value);

        let mut bytes = Bytes::from(encode_to);
        let value_decoded = decode_bool(&mut bytes);

        assert_eq!(
            value, value_decoded,
            "Expected equal values when encoding and decoding"
        );

        assert!(bytes.is_empty(), "Expected empty bytes array");

        Ok(())
    }

    #[test]
    fn test_encode_decode_null() -> Result<()> {
        let mut encode_to = Vec::new();

        encode_null(&mut encode_to);

        let mut bytes = Bytes::from(encode_to);
        let value_decoded = decode_null(&mut bytes);

        assert_eq!(
            NULL, value_decoded,
            "Expected equal values when encoding and decoding"
        );
        assert!(bytes.is_empty(), "Expected empty bytes array");

        Ok(())
    }
}
