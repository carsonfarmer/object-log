//! Single-repository HTTP policy. No identity or storage authority lives here.
use base64::{Engine as _, engine::general_purpose::STANDARD};
use subtle::ConstantTimeEq;

pub(crate) struct AuthConfig {
    disabled: bool,
    read: Option<[u8; 32]>,
    write: Option<[u8; 32]>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Denied {
    Unauthorized,
    Forbidden,
}

impl AuthConfig {
    pub(crate) fn parse(mode: &str, read: &str, write: &str) -> Result<Self, &'static str> {
        let invalid = "invalid authentication configuration";
        if mode == "disabled" && read.is_empty() && write.is_empty() {
            return Ok(Self {
                disabled: true,
                read: None,
                write: None,
            });
        }
        if mode != "basic" || (read.is_empty() && write.is_empty()) {
            return Err(invalid);
        }
        let token = |value: &str| {
            if value.is_empty() {
                Ok(None)
            } else {
                decode_token(value.as_bytes()).map(Some).ok_or(invalid)
            }
        };
        let read = token(read)?;
        let write = token(write)?;
        if let (Some(read), Some(write)) = (&read, &write)
            && bool::from(read.ct_eq(write))
        {
            return Err(invalid);
        }
        Ok(Self {
            disabled: false,
            read,
            write,
        })
    }

    /// Call before opening storage or consuming the request body.
    pub(crate) fn authorize<'a>(
        &self,
        mut headers: impl Iterator<Item = &'a [u8]>,
        write_scope: bool,
        read_only: bool,
    ) -> Result<(), Denied> {
        if !self.disabled {
            let value = headers.next().ok_or(Denied::Unauthorized)?;
            if headers.next().is_some() || value.len() > 128 {
                return Err(Denied::Unauthorized);
            }
            let candidate = credentials(value).ok_or(Denied::Unauthorized)?;
            // Always perform both fixed-length comparisons before inspecting roles.
            // Absent roles are masked so a zero token never grants an absent role.
            let reader = candidate.ct_eq(&self.read.unwrap_or([0; 32]))
                & subtle::Choice::from(u8::from(self.read.is_some()));
            let writer = candidate.ct_eq(&self.write.unwrap_or([0; 32]))
                & subtle::Choice::from(u8::from(self.write.is_some()));
            if !bool::from(reader | writer) {
                return Err(Denied::Unauthorized);
            }
            if write_scope && !bool::from(writer) {
                return Err(Denied::Forbidden);
            }
        }
        if write_scope && read_only {
            return Err(Denied::Forbidden);
        }
        Ok(())
    }
}

fn credentials(value: &[u8]) -> Option<[u8; 32]> {
    let (scheme, encoded) = value.split_at_checked(6)?;
    if !scheme.eq_ignore_ascii_case(b"Basic ") {
        return None;
    }
    let mut scratch = [0; 96];
    let length = STANDARD.decode_slice(encoded, &mut scratch).ok()?;
    let decoded = scratch.get(..length)?;
    decode_token(decoded.strip_prefix(b"git:")?)
}

fn decode_token(value: &[u8]) -> Option<[u8; 32]> {
    if value.len() != 64 {
        return None;
    }
    let mut token = [0; 32];
    for (byte, digits) in token.iter_mut().zip(value.chunks_exact(2)) {
        let hex = |digit: u8| match digit {
            b'0'..=b'9' => Some(digit - b'0'),
            b'a'..=b'f' => Some(digit - b'a' + 10),
            b'A'..=b'F' => Some(digit - b'A' + 10),
            _ => None,
        };
        *byte = hex(digits[0])? * 16 + hex(digits[1])?;
    }
    Some(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn basic(user: &str, token: &str) -> Vec<u8> {
        format!("Basic {}", STANDARD.encode(format!("{user}:{token}"))).into_bytes()
    }

    #[test]
    fn configuration_is_fail_closed() {
        for (mode, read, write) in [
            ("basic", String::new(), String::new()),
            ("unknown", "11".repeat(32), String::new()),
            ("disabled", "11".repeat(32), String::new()),
            ("basic", "11".repeat(31), String::new()),
            ("basic", "zz".repeat(32), String::new()),
            ("basic", "ab".repeat(32), "AB".repeat(32)),
        ] {
            assert!(AuthConfig::parse(mode, &read, &write).is_err());
        }
    }

    #[test]
    fn scopes_and_read_only() -> Result<(), &'static str> {
        let config = AuthConfig::parse("basic", &"11".repeat(32), &"22".repeat(32))?;
        let reader = basic("git", &"11".repeat(32));
        let writer = basic("git", &"22".repeat(32));
        for read_only in [false, true] {
            assert_eq!(
                config.authorize([reader.as_slice()].into_iter(), false, read_only),
                Ok(())
            );
            assert_eq!(
                config.authorize([writer.as_slice()].into_iter(), false, read_only),
                Ok(())
            );
            assert_eq!(
                config.authorize([reader.as_slice()].into_iter(), true, read_only),
                Err(Denied::Forbidden)
            );
            assert_eq!(
                config.authorize([writer.as_slice()].into_iter(), true, read_only),
                if read_only {
                    Err(Denied::Forbidden)
                } else {
                    Ok(())
                }
            );
        }
        let local = AuthConfig::parse("disabled", "", "")?;
        assert_eq!(local.authorize(std::iter::empty(), true, false), Ok(()));
        assert_eq!(
            local.authorize(std::iter::empty(), true, true),
            Err(Denied::Forbidden)
        );
        Ok(())
    }

    #[test]
    fn rejects_untrusted_headers() -> Result<(), &'static str> {
        let config = AuthConfig::parse("basic", "", &"22".repeat(32))?;
        assert_eq!(
            config.authorize(std::iter::empty(), true, false),
            Err(Denied::Unauthorized)
        );
        let valid = basic("git", &"22".repeat(32));
        assert_eq!(
            config.authorize(
                [valid.as_slice(), valid.as_slice()].into_iter(),
                true,
                false
            ),
            Err(Denied::Unauthorized)
        );
        for value in [
            basic("git", &"00".repeat(32)),
            basic("other", &"22".repeat(32)),
            basic("git", &"22".repeat(31)),
            basic("git", &"zz".repeat(32)),
            b"Basic !!!".to_vec(),
            b"Bearer abc".to_vec(),
            vec![b'x'; 129],
            [valid.clone(), b", Basic bad".to_vec()].concat(),
            vec![255],
        ] {
            assert_eq!(
                config.authorize([value.as_slice()].into_iter(), false, false),
                Err(Denied::Unauthorized)
            );
        }
        Ok(())
    }
}
