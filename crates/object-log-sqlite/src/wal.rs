use std::ffi::{c_int, c_void};
use std::ptr;

use bytes::Bytes;
use rusqlite::{Connection, ffi};

use crate::SqliteError;

pub(crate) const WAL_HEADER_BYTES: usize = 32;
pub(crate) const WAL_FRAME_HEADER_BYTES: usize = 24;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct WalPosition {
    pub(crate) header: Option<[u8; WAL_HEADER_BYTES]>,
    pub(crate) frames: u32,
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
    let frames = u32::try_from(frames)
        .map_err(|_| SqliteError::InvalidWal("SQLite returned a negative frame count".into()))?;
    if frames == 0 {
        return Ok(WalCapture {
            position: WalPosition::default(),
            bytes: Bytes::new(),
        });
    }

    let frame_size = page_size
        .checked_add(WAL_FRAME_HEADER_BYTES)
        .ok_or_else(|| SqliteError::InvalidWal("WAL frame size overflow".into()))?;
    let mut file = ptr::null_mut();
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
        return Err(SqliteError::InvalidWal(
            "SQLite returned no WAL file".into(),
        ));
    }
    // SAFETY: SQLite owns the live file and its method table for this call.
    let methods = unsafe { (*file).pMethods.as_ref() }
        .ok_or_else(|| SqliteError::InvalidWal("the WAL has no VFS methods".into()))?;
    let size = methods
        .xFileSize
        .ok_or_else(|| SqliteError::InvalidWal("the WAL VFS has no size method".into()))?;
    let read = methods
        .xRead
        .ok_or_else(|| SqliteError::InvalidWal("the WAL VFS has no read method".into()))?;
    let mut physical_len = 0;
    // SAFETY: The file and output pointer are valid for this VFS call.
    check(unsafe { size(file, &raw mut physical_len) })?;

    let header: [u8; WAL_HEADER_BYTES] = read_exact(file, read, 0, WAL_HEADER_BYTES)?
        .as_ref()
        .try_into()
        .map_err(|_| SqliteError::InvalidWal("the WAL header is truncated".into()))?;
    if prior.frames > frames || (prior.frames > 0 && prior.header != Some(header)) {
        return Err(SqliteError::InvalidWal(
            "WAL reset without a durable checkpoint".into(),
        ));
    }
    let frame_offset = usize::try_from(prior.frames)?
        .checked_mul(frame_size)
        .and_then(|value| value.checked_add(WAL_HEADER_BYTES))
        .ok_or_else(|| SqliteError::InvalidWal("WAL read offset overflow".into()))?;
    let len = usize::try_from(frames - prior.frames)?
        .checked_mul(frame_size)
        .ok_or_else(|| SqliteError::InvalidWal("WAL read length overflow".into()))?;
    let end = frame_offset
        .checked_add(len)
        .ok_or_else(|| SqliteError::InvalidWal("WAL read length overflow".into()))?;
    if physical_len < i64::try_from(end)? {
        return Err(SqliteError::InvalidWal(
            "the physical WAL is shorter than its committed prefix".into(),
        ));
    }
    Ok(WalCapture {
        position: WalPosition {
            header: Some(header),
            frames,
        },
        bytes: read_exact(file, read, i64::try_from(frame_offset)?, len)?,
    })
}

pub(crate) fn validate_record(header: &[u8; 32], frames: &[u8]) -> Result<(), SqliteError> {
    let page_size = usize::try_from(u32::from_be_bytes(copy4(&header[8..12])?))?;
    let frame_size = page_size
        .checked_add(WAL_FRAME_HEADER_BYTES)
        .ok_or_else(|| SqliteError::InvalidWal("WAL frame size overflow".into()))?;
    let magic = u32::from_be_bytes(copy4(&header[..4])?);
    if !matches!(magic, 0x377f_0682 | 0x377f_0683)
        || u32::from_be_bytes(copy4(&header[4..8])?) != 3_007_000
        || page_size != 4_096
        || frames.is_empty()
        || !frames.len().is_multiple_of(frame_size)
    {
        return Err(SqliteError::InvalidWal("invalid WAL record shape".into()));
    }
    let count = frames.len() / frame_size;
    for (index, frame) in frames.chunks_exact(frame_size).enumerate() {
        let page = u32::from_be_bytes(copy4(&frame[..4])?);
        let database_size = u32::from_be_bytes(copy4(&frame[4..8])?);
        if page == 0
            || frame[8..16] != header[16..24]
            || (index + 1 == count) != (database_size != 0)
        {
            return Err(SqliteError::InvalidWal(
                "invalid WAL transaction frame".into(),
            ));
        }
    }
    Ok(())
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
            .ok_or_else(|| SqliteError::InvalidWal("WAL read offset overflow".into()))?;
    }
    Ok(Bytes::from(bytes))
}

fn copy4(bytes: &[u8]) -> Result<[u8; 4], SqliteError> {
    bytes
        .try_into()
        .map_err(|_| SqliteError::InvalidWal("WAL field is truncated".into()))
}

fn check(code: c_int) -> Result<(), SqliteError> {
    if code == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(rusqlite::Error::SqliteFailure(ffi::Error::new(code), None).into())
    }
}
