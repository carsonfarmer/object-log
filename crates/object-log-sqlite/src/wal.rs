use std::ffi::{c_int, c_void};
use std::ptr;

use bytes::Bytes;
use rusqlite::{Connection, ffi};

use crate::SqliteError;

pub(crate) const WAL_HEADER_BYTES: usize = 32;
pub(crate) const WAL_FRAME_HEADER_BYTES: usize = 24;
const WAL_FORMAT: u32 = 3_007_000;
const WAL_MAGIC: u32 = 0x377f_0682;
const MAX_PAGE_NUMBER: u32 = 0xffff_fffe;
const WAL_FRAME_BYTES: usize = 4_096 + WAL_FRAME_HEADER_BYTES;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalPosition {
    pub(crate) header: Option<[u8; WAL_HEADER_BYTES]>,
    pub(crate) frames: u32,
    pub(crate) checksum: [u32; 2],
}

#[derive(Debug)]
pub(crate) struct WalCapture {
    pub(crate) position: WalPosition,
    pub(crate) bytes: Bytes,
}

pub(crate) fn committed(
    conn: &Connection,
    page_size: usize,
    prior: &WalPosition,
) -> Result<WalCapture, SqliteError> {
    if page_size != 4_096 {
        return Err(invalid("WAL page size is not 4096 bytes"));
    }
    let mut frames: c_int = 0;
    let mut checkpointed: c_int = 0;
    // SAFETY: `conn` stays alive and no other SQLite call runs in this scope.
    let db = unsafe { conn.handle() };
    // SAFETY: The database name is static. SQLite writes two integer outputs.
    check(unsafe {
        ffi::sqlite3_wal_checkpoint_v2(
            db,
            c"main".as_ptr(),
            ffi::SQLITE_CHECKPOINT_NOOP,
            &raw mut frames,
            &raw mut checkpointed,
        )
    })?;
    let frames =
        u32::try_from(frames).map_err(|_| invalid("SQLite returned a negative frame count"))?;
    if frames == 0 {
        if prior.frames != 0 {
            return Err(invalid("WAL reset without a durable checkpoint"));
        }
        return Ok(WalCapture {
            position: WalPosition::default(),
            bytes: Bytes::new(),
        });
    }

    let mut file: *mut ffi::sqlite3_file = ptr::null_mut();
    // SAFETY: SQLite writes one borrowed file pointer. It is not retained.
    check(unsafe {
        ffi::sqlite3_file_control(
            db,
            c"main".as_ptr(),
            ffi::SQLITE_FCNTL_JOURNAL_POINTER,
            ptr::from_mut(&mut file).cast::<c_void>(),
        )
    })?;
    if file.is_null() {
        return Err(invalid("SQLite returned no WAL file"));
    }
    // SAFETY: SQLite owns the live file and its method table for this call.
    let methods = unsafe { (*file).pMethods.as_ref() }
        .ok_or_else(|| invalid("the WAL has no VFS methods"))?;
    let size = methods
        .xFileSize
        .ok_or_else(|| invalid("the WAL VFS has no size method"))?;
    let read = methods
        .xRead
        .ok_or_else(|| invalid("the WAL VFS has no read method"))?;
    let mut physical_len = 0;
    // SAFETY: The file and output pointer are valid for this VFS call.
    check(unsafe { size(file, &raw mut physical_len) })?;

    let header: [u8; WAL_HEADER_BYTES] = read_exact(file, read, 0, WAL_HEADER_BYTES)?
        .as_ref()
        .try_into()
        .map_err(|_| invalid("the WAL header is truncated"))?;
    validate_header(&header)?;
    if prior.frames > frames || (prior.frames != 0 && prior.header != Some(header)) {
        return Err(invalid("WAL reset without a durable checkpoint"));
    }
    let offset = usize::try_from(prior.frames)?
        .checked_mul(WAL_FRAME_BYTES)
        .and_then(|value| value.checked_add(WAL_HEADER_BYTES))
        .ok_or_else(|| invalid("WAL read offset overflow"))?;
    let len = usize::try_from(frames - prior.frames)?
        .checked_mul(WAL_FRAME_BYTES)
        .ok_or_else(|| invalid("WAL read length overflow"))?;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| invalid("WAL read length overflow"))?;
    if physical_len < i64::try_from(end)? {
        return Err(invalid(
            "the physical WAL is shorter than its committed prefix",
        ));
    }
    let bytes = read_exact(file, read, i64::try_from(offset)?, len)?;
    let position = if bytes.is_empty() {
        *prior
    } else if prior.frames == 0 {
        validate_complete(&header, &bytes)?
    } else {
        validate_record(&header, &bytes, *prior)?
    };
    if position.frames != frames {
        return Err(invalid("captured WAL boundary does not match SQLite"));
    }
    Ok(WalCapture { position, bytes })
}

pub(crate) fn validate_record(
    header: &[u8; WAL_HEADER_BYTES],
    frames: &[u8],
    prior: WalPosition,
) -> Result<WalPosition, SqliteError> {
    validate_range(header, frames, prior, false)
}

pub(crate) fn validate_complete(
    header: &[u8; WAL_HEADER_BYTES],
    frames: &[u8],
) -> Result<WalPosition, SqliteError> {
    validate_range(header, frames, WalPosition::default(), true)
}

fn validate_header(header: &[u8; WAL_HEADER_BYTES]) -> Result<([u32; 2], bool), SqliteError> {
    let magic = be_u32(&header[..4]);
    if magic & !1 != WAL_MAGIC
        || be_u32(&header[4..8]) != WAL_FORMAT
        || be_u32(&header[8..12]) != 4_096
    {
        return Err(invalid("invalid WAL header"));
    }
    let calculated = checksum([0, 0], &header[..24], magic & 1 != 0);
    let stored = [be_u32(&header[24..28]), be_u32(&header[28..32])];
    if calculated != stored {
        return Err(invalid("invalid WAL header checksum"));
    }
    Ok((stored, magic & 1 != 0))
}

fn validate_range(
    header: &[u8; WAL_HEADER_BYTES],
    frames: &[u8],
    prior: WalPosition,
    accumulated: bool,
) -> Result<WalPosition, SqliteError> {
    let (header_checksum, big_endian) = validate_header(header)?;
    if prior.frames != 0 && prior.header != Some(*header) {
        return Err(invalid("WAL range crosses a reset"));
    }
    if frames.is_empty() || !frames.len().is_multiple_of(WAL_FRAME_BYTES) {
        return Err(invalid("invalid WAL frame alignment"));
    }
    let frame_count = frames.len() / WAL_FRAME_BYTES;
    let count = u32::try_from(frame_count)?;
    let mut rolling = if prior.frames == 0 {
        header_checksum
    } else {
        prior.checksum
    };
    for (index, frame) in frames.chunks_exact(WAL_FRAME_BYTES).enumerate() {
        let page = be_u32(&frame[..4]);
        let database_size = be_u32(&frame[4..8]);
        let final_frame = index + 1 == frame_count;
        if page == 0 || page > MAX_PAGE_NUMBER || database_size > MAX_PAGE_NUMBER {
            return Err(invalid("invalid WAL page number"));
        }
        if frame[8..16] != header[16..24] {
            return Err(invalid("WAL frame salt does not match its header"));
        }
        if database_size != 0 && !final_frame && !accumulated {
            return Err(invalid("WAL record has an early commit marker"));
        }
        if final_frame && database_size == 0 {
            return Err(invalid("WAL range has no final commit marker"));
        }
        rolling = checksum(rolling, &frame[..8], big_endian);
        rolling = checksum(rolling, &frame[WAL_FRAME_HEADER_BYTES..], big_endian);
        if rolling != [be_u32(&frame[16..20]), be_u32(&frame[20..24])] {
            return Err(invalid("invalid WAL frame checksum"));
        }
    }
    Ok(WalPosition {
        header: Some(*header),
        frames: prior
            .frames
            .checked_add(count)
            .ok_or_else(|| invalid("WAL frame count overflow"))?,
        checksum: rolling,
    })
}

fn checksum(mut value: [u32; 2], bytes: &[u8], big_endian: bool) -> [u32; 2] {
    for pair in bytes.chunks_exact(8) {
        let first = word(&pair[..4], big_endian);
        value[0] = value[0].wrapping_add(first).wrapping_add(value[1]);
        let second = word(&pair[4..], big_endian);
        value[1] = value[1].wrapping_add(second).wrapping_add(value[0]);
    }
    value
}

fn word(bytes: &[u8], big_endian: bool) -> u32 {
    let value = [bytes[0], bytes[1], bytes[2], bytes[3]];
    if big_endian {
        u32::from_be_bytes(value)
    } else {
        u32::from_le_bytes(value)
    }
}

fn be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

type ReadFn = unsafe extern "C" fn(*mut ffi::sqlite3_file, *mut c_void, c_int, i64) -> c_int;

fn read_exact(
    file: *mut ffi::sqlite3_file,
    read: ReadFn,
    mut offset: i64,
    len: usize,
) -> Result<Bytes, SqliteError> {
    let mut bytes = vec![0_u8; len];
    for chunk in bytes.chunks_mut(c_int::MAX as usize) {
        let amount = c_int::try_from(chunk.len())?;
        // SAFETY: The chunk is writable. SQLite owns the live file and method
        // pointer. Checked arithmetic keeps the offset in range.
        check(unsafe { read(file, chunk.as_mut_ptr().cast(), amount, offset) })?;
        offset = offset
            .checked_add(i64::from(amount))
            .ok_or_else(|| invalid("WAL read offset overflow"))?;
    }
    Ok(Bytes::from(bytes))
}

fn invalid(message: &str) -> SqliteError {
    SqliteError::InvalidWal(message.into())
}

fn check(code: c_int) -> Result<(), SqliteError> {
    if code == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(rusqlite::Error::SqliteFailure(ffi::Error::new(code), None).into())
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::validate_header;

    #[test]
    fn header_checksum_word_order_follows_the_magic() -> Result<(), Box<dyn Error>> {
        for encoded in [
            "377f0682002de21800001000000000000000000100000002d5e703138fcedab8",
            "377f0683002de218000010000000000000000001000000021604e6d8bcddce93",
        ] {
            let bytes = hex::decode(encoded)?;
            let header: [u8; 32] = bytes.try_into().map_err(|_| "invalid test header")?;
            validate_header(&header)?;
        }
        Ok(())
    }
}
