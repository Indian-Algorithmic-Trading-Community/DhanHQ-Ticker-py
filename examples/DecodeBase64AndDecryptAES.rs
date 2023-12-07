use sha1::Sha1;
use hex_literal::hex;
use pbkdf2::pbkdf2_hmac_array;
use base64::engine::general_purpose;
use aes::cipher::{block_padding::Pkcs7, BlockDecryptMut, KeyIvInit};

pub type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;
pub const BUFFER_SIZE: usize = 2000;

#[inline]
pub fn bytes_to_str<'b>(bytes: &'b [u8]) -> Result<&'b str, Box<dyn std::error::Error>> {
    Ok(std::str::from_utf8(bytes)?.trim_end_matches('\0'))
}

pub struct Aes128DecryptionParameters {
    pub initial_vector: [u8; 16],
    pub password: &'static [u8],
    pub iterations: u32,
    pub salt: [u8; 16],
    pub key: [u8; 16],
}

impl Aes128DecryptionParameters {
    const KEY_SIZE: usize = 16;
    const PASSWORD: &'static [u8] = b"password";
    const SALT: [u8; 16] = hex!("salt_hex");
    const INITIAL_VECTOR: [u8; 16] = hex!("iv_hex");
    const ITERATIONS: u32 = 1_000;

    pub fn new() -> Self {
        let key = Self::derive_key(Self::PASSWORD, Self::SALT, Self::ITERATIONS);
        Self {
            initial_vector: Self::INITIAL_VECTOR,
            password: Self::PASSWORD,
            iterations: Self::ITERATIONS,
            salt: Self::SALT,
            key,
        }
    }

    #[inline]
    pub fn decrypt<'b>(
        &self,
        encoded_bytes: &[u8],
        buffer: &'b mut [u8; BUFFER_SIZE],
    ) -> Result<&'b [u8], Box<dyn std::error::Error>> {
        let decryptor = Aes128CbcDec::new(&self.key.into(), &self.initial_vector.into());
        decryptor
            .decrypt_padded_b2b_mut::<Pkcs7>(encoded_bytes, buffer)
            .map_err(|_| <&str as Into<Box<dyn std::error::Error>>>::into("Unpad Error"))?;
        Ok(buffer)
    }

    #[inline]
    fn derive_key(password: &'static [u8], salt: [u8; 16], iterations: u32) -> [u8; 16] {
        pbkdf2_hmac_array::<Sha1, { Self::KEY_SIZE }>(password, &salt, iterations)
    }
}

pub struct UserDataDecryptor<'a> {
    pub params: &'a Aes128DecryptionParameters,
    pub buffer: [u8; BUFFER_SIZE],
}

impl<'a> UserDataDecryptor<'a> {
    pub fn new(params: &'a Aes128DecryptionParameters) -> Self {
        Self {
            params,
            buffer: [0; BUFFER_SIZE],
        }
    }
    pub fn decode_base64<E: base64::Engine>(
        &'a mut self,
        base64_input: &'a str,
        engine: E,
    ) -> Result<&'a [u8], Box<dyn std::error::Error>> {
        let decoded = engine.decode(base64_input)?;
        Ok(self.params.decrypt(&decoded, &mut self.buffer)?)
    }

    pub fn decode_user_data(
        &'a mut self,
        base64_encoded_data: &'a str,
    ) -> Result<&'a str, Box<dyn std::error::Error>> {
        let decoded_bytes =
            self.decode_base64(base64_encoded_data, general_purpose::STANDARD.clone())?;
        bytes_to_str(decoded_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_user_data_test() {
        const CIPHER_TEXT: &str = "an_insanely_large_aes_encrypted_base64_encoded_string";
        let params = Aes128DecryptionParameters::new();
        let mut decoder = UserDataDecryptor::new(&params);
        let start = std::time::Instant::now();
        match decoder.decode_user_data(CIPHER_TEXT) {
            Ok(value) => println!("{}", value),
            Err(error) => println!("Error: {}", error),
        }
        println!(
            "Took {} Micro Seconds To Completely Decode `AES Encrypted Base64 Encoded Data`",
            start.elapsed().as_micros()
        );
    }
}
