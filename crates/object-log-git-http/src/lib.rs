#![doc = include_str!("../README.md")]
#![deny(missing_docs, unsafe_code)]

use std::path::{Path, PathBuf};

use bytes::Bytes;
use gix_packetline::{PacketLineRef, decode};
use object_log::{CommitStatus, Log, Resolution, TransactionId};
use object_log_git::{ObjectFormat, ObjectId, RefUpdate, Repository};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const MAX_COMMANDS: usize = 1_024;
const MAX_HAVES: usize = 65_536;
const MAX_UPLOAD_CONTROL_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECEIVE_CONTROL_BYTES: usize = 1024 * 1024;
const MAX_PACK_BYTES: u64 = 512 * 1024 * 1024;
const ZERO_ID: &str = "0000000000000000000000000000000000000000";
const CACHE_CONTROL: &str = "no-cache, max-age=0, must-revalidate";

/// One supported Git smart HTTP service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Service {
    /// Fetch negotiation and pack transfer.
    UploadPack,
    /// Atomic ref updates and optional pack transfer.
    ReceivePack,
}

impl Service {
    /// Returns the service name used by Git HTTP routing.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::UploadPack => "git-upload-pack",
            Self::ReceivePack => "git-receive-pack",
        }
    }

    /// Returns the exact media type for an `info/refs` response.
    #[must_use]
    pub const fn advertisement_content_type(self) -> &'static str {
        match self {
            Self::UploadPack => "application/x-git-upload-pack-advertisement",
            Self::ReceivePack => "application/x-git-receive-pack-advertisement",
        }
    }

    /// Returns the exact media type for a service POST response.
    #[must_use]
    pub const fn result_content_type(self) -> &'static str {
        match self {
            Self::UploadPack => "application/x-git-upload-pack-result",
            Self::ReceivePack => "application/x-git-receive-pack-result",
        }
    }

    /// Returns the required smart Git cache policy.
    #[must_use]
    pub const fn cache_control(self) -> &'static str {
        CACHE_CONTROL
    }
}

/// The durable result of a receive-pack request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReceiveOutcome {
    /// Every ref update is durable.
    Committed,
    /// The transaction was definitely rejected.
    Rejected,
    /// The result remains uncertain. Retain this token for later recovery.
    Pending(Bytes),
    /// The store no longer retains enough evidence to classify the result.
    Expired,
}

/// A Git protocol or local I/O failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The request is malformed or exceeds a fixed limit.
    #[error("invalid Git protocol: {0}")]
    Protocol(&'static str),
    /// A local scratch operation failed.
    #[error("Git HTTP I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// The repository rejected a request or durable state.
    #[error(transparent)]
    Git(#[from] object_log_git::Error),
    /// The object log rejected a recovery operation.
    #[error(transparent)]
    ObjectLog(#[from] object_log::Error),
}

/// A stateless Git smart HTTP protocol endpoint.
#[derive(Clone, Debug)]
pub struct SmartHttp {
    log: Log,
    scratch: PathBuf,
}

impl SmartHttp {
    /// Creates an endpoint. Each operation uses a new directory under `scratch`.
    #[must_use]
    pub fn new(log: Log, scratch: impl Into<PathBuf>) -> Self {
        Self {
            log,
            scratch: scratch.into(),
        }
    }

    /// Writes one protocol v0 service advertisement.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid state or local I/O.
    pub async fn advertise(
        &self,
        service: Service,
        output: &mut (impl AsyncWrite + Unpin),
    ) -> Result<(), Error> {
        let scratch = self.open_scratch().await?;
        let repo =
            Repository::open(&self.log, scratch.path().join("repo"), ObjectFormat::Sha1).await?;
        write_packet(output, format!("# service={}\n", service.name()).as_bytes()).await?;
        write_flush(output).await?;
        write_advertisement(output, service, &repo).await?;
        output.flush().await?;
        Ok(())
    }

    /// Serves one bounded upload-pack POST body.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, state, or local I/O.
    pub async fn upload_pack(
        &self,
        input: &mut (impl AsyncRead + Unpin),
        output: &mut (impl AsyncWrite + Unpin),
    ) -> Result<(), Error> {
        let request = parse_upload(input).await?;
        if !request.done {
            write_packet(output, b"NAK\n").await?;
            output.flush().await?;
            return Ok(());
        }
        let scratch = self.open_scratch().await?;
        let repo =
            Repository::open(&self.log, scratch.path().join("repo"), ObjectFormat::Sha1).await?;
        let pack = scratch.path().join("fetch.pack");
        repo.write_fetch_pack(&request.wants, &pack).await?;
        let mut pack = tokio::fs::File::open(pack).await?;
        write_packet(output, b"NAK\n").await?;
        tokio::io::copy(&mut pack, output).await?;
        output.flush().await?;
        Ok(())
    }

    /// Serves one bounded receive-pack POST body.
    /// It reports success only after durable publication.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, state, or local I/O.
    pub async fn receive_pack(
        &self,
        input: &mut (impl AsyncRead + Unpin),
        output: &mut (impl AsyncWrite + Unpin),
    ) -> Result<ReceiveOutcome, Error> {
        let scratch = self.open_scratch().await?;
        let (updates, names) = parse_receive(input).await?;
        let pack = scratch.path().join("receive.pack");
        let pack_bytes = spool_pack(input, &pack).await?;
        let repo =
            Repository::open(&self.log, scratch.path().join("repo"), ObjectFormat::Sha1).await?;
        let prepared = match repo
            .prepare_push(
                TransactionId::new(),
                updates,
                (pack_bytes != 0).then_some(pack.as_path()),
            )
            .await
        {
            Ok(prepared) => prepared,
            Err(error) if is_client_rejection(&error) => {
                write_report(output, &names, false, b"rejected").await?;
                return Ok(ReceiveOutcome::Rejected);
            }
            Err(error) => return Err(Error::Git(error)),
        };
        let token = prepared.recovery_token().clone();
        let outcome = match prepared.publish().await? {
            CommitStatus::Committed(_) => ReceiveOutcome::Committed,
            CommitStatus::Conflict(_) => ReceiveOutcome::Rejected,
            CommitStatus::Pending(_) => match self.log.resume(&token).await? {
                Resolution::Committed(_) => ReceiveOutcome::Committed,
                Resolution::NotCommitted(_) => ReceiveOutcome::Rejected,
                Resolution::StillPending(_) => ReceiveOutcome::Pending(token),
                Resolution::Expired(_) => ReceiveOutcome::Expired,
            },
        };
        match &outcome {
            ReceiveOutcome::Committed => write_report(output, &names, true, b"").await?,
            ReceiveOutcome::Rejected => write_report(output, &names, false, b"conflict").await?,
            ReceiveOutcome::Pending(_) => write_report(output, &names, false, b"pending").await?,
            ReceiveOutcome::Expired => write_report(output, &names, false, b"expired").await?,
        }
        output.flush().await?;
        Ok(outcome)
    }

    async fn open_scratch(&self) -> Result<tempfile::TempDir, Error> {
        tokio::fs::create_dir_all(&self.scratch).await?;
        tempfile::tempdir_in(&self.scratch).map_err(Error::Io)
    }
}

struct UploadRequest {
    wants: Vec<ObjectId>,
    done: bool,
}

async fn parse_upload(input: &mut (impl AsyncRead + Unpin)) -> Result<UploadRequest, Error> {
    let mut control = 0;
    let mut wants = Vec::new();
    loop {
        match read_packet(input, &mut control, MAX_UPLOAD_CONTROL_BYTES).await? {
            Packet::Flush => break,
            Packet::Data(line) => {
                if wants.len() == MAX_COMMANDS {
                    return Err(Error::Protocol("too many wants"));
                }
                let line = trim_newline(&line);
                let value = line
                    .strip_prefix(b"want ")
                    .ok_or(Error::Protocol("expected want"))?;
                let (id, capabilities) = split_once(value, b' ');
                let id = parse_id(id)?;
                if !wants.is_empty() && capabilities.is_some() {
                    return Err(Error::Protocol(
                        "capabilities are only valid on the first want",
                    ));
                }
                if wants.is_empty() && !valid_upload_capabilities(capabilities.unwrap_or_default())
                {
                    return Err(Error::Protocol("unsupported upload capability"));
                }
                wants.push(id);
            }
        }
    }
    if wants.is_empty() {
        return Err(Error::Protocol("upload has no wants"));
    }
    let mut haves = 0;
    let mut done = false;
    loop {
        match read_packet_optional(input, &mut control, MAX_UPLOAD_CONTROL_BYTES).await? {
            None | Some(Packet::Flush) => break,
            Some(Packet::Data(line)) if trim_newline(&line) == b"done" => {
                done = true;
                break;
            }
            Some(Packet::Data(line)) => {
                let value = trim_newline(&line)
                    .strip_prefix(b"have ")
                    .ok_or(Error::Protocol("expected have or done"))?;
                parse_id(value)?;
                haves += 1;
                if haves > MAX_HAVES {
                    return Err(Error::Protocol("too many haves"));
                }
            }
        }
    }
    let mut trailing = [0];
    if input.read(&mut trailing).await? != 0 {
        return Err(Error::Protocol("trailing upload data"));
    }
    Ok(UploadRequest { wants, done })
}

fn valid_upload_capabilities(capabilities: &[u8]) -> bool {
    capabilities.is_empty()
        || capabilities.split(|byte| *byte == b' ').all(|capability| {
            capability == b"object-format=sha1" || capability.starts_with(b"agent=")
        })
}

fn is_client_rejection(error: &object_log_git::Error) -> bool {
    matches!(
        error,
        object_log_git::Error::InvalidRefName
            | object_log_git::Error::InvalidRecord(_)
            | object_log_git::Error::InvalidPack(_)
            | object_log_git::Error::InvalidReference
            | object_log_git::Error::StaleReference
            | object_log_git::Error::NonFastForward
            | object_log_git::Error::InvalidObjectGraph(_)
    )
}

async fn parse_receive(
    input: &mut (impl AsyncRead + Unpin),
) -> Result<(Vec<RefUpdate>, Vec<Vec<u8>>), Error> {
    let mut control = 0;
    let mut updates = Vec::new();
    let mut names = Vec::new();
    let mut report_status = false;
    loop {
        match read_packet(input, &mut control, MAX_RECEIVE_CONTROL_BYTES).await? {
            Packet::Flush => break,
            Packet::Data(line) => {
                if updates.len() == MAX_COMMANDS {
                    return Err(Error::Protocol("too many ref commands"));
                }
                let line = trim_newline(&line);
                let command = if updates.is_empty() {
                    let separator = line
                        .iter()
                        .position(|byte| *byte == 0)
                        .ok_or(Error::Protocol("first command has no capabilities"))?;
                    let (command, capabilities) = line.split_at(separator);
                    for capability in capabilities[1..]
                        .split(|byte| *byte == b' ')
                        .filter(|value| !value.is_empty())
                    {
                        match capability {
                            b"report-status" => report_status = true,
                            b"atomic" | b"ofs-delta" | b"object-format=sha1" => {}
                            value if value.starts_with(b"agent=") => {}
                            _ => return Err(Error::Protocol("unsupported receive capability")),
                        }
                    }
                    command
                } else {
                    if line.contains(&0) {
                        return Err(Error::Protocol(
                            "capabilities are only valid on the first command",
                        ));
                    }
                    line
                };
                let mut fields = command.split(|byte| *byte == b' ');
                let old = fields.next().ok_or(Error::Protocol("missing old ID"))?;
                let new = fields.next().ok_or(Error::Protocol("missing new ID"))?;
                let name = fields.next().ok_or(Error::Protocol("missing ref name"))?;
                if fields.next().is_some() {
                    return Err(Error::Protocol("invalid ref command"));
                }
                let expected = parse_optional_id(old)?;
                let target = parse_optional_id(new)?;
                let update = RefUpdate::new(Bytes::copy_from_slice(name), expected, target)?;
                names.push(name.to_vec());
                updates.push(update);
            }
        }
    }
    if updates.is_empty() || !report_status {
        return Err(Error::Protocol(
            "receive requires commands and report-status",
        ));
    }
    Ok((updates, names))
}

async fn spool_pack(input: &mut (impl AsyncRead + Unpin), path: &Path) -> Result<u64, Error> {
    let mut header = [0; 4];
    if input.read(&mut header[..1]).await? == 0 {
        return Ok(0);
    }
    input.read_exact(&mut header[1..]).await?;
    if &header != b"PACK" {
        return Err(Error::Protocol("data after commands is not a Git pack"));
    }
    let mut output = tokio::fs::File::create(path).await?;
    output.write_all(&header).await?;
    let mut buffer = vec![0; 64 * 1024].into_boxed_slice();
    let mut bytes = 4_u64;
    loop {
        let read = input.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(u64::try_from(read).map_err(|_| Error::Protocol("pack is too large"))?)
            .filter(|bytes| *bytes <= MAX_PACK_BYTES)
            .ok_or(Error::Protocol("pack is too large"))?;
        output.write_all(&buffer[..read]).await?;
    }
    output.flush().await?;
    Ok(bytes)
}

async fn write_advertisement(
    output: &mut (impl AsyncWrite + Unpin),
    service: Service,
    repo: &Repository,
) -> Result<(), Error> {
    let refs = repo.refs();
    let peeled = repo.peeled_tags()?;
    let capabilities = match service {
        Service::UploadPack => "object-format=sha1 agent=object-log symref=HEAD:refs/heads/main",
        Service::ReceivePack => {
            "report-status delete-refs atomic ofs-delta object-format=sha1 agent=object-log"
        }
    };
    let mut first = true;
    if service == Service::UploadPack
        && let Some(main) = refs.get(&b"refs/heads/main"[..])
    {
        let line = format!("{main} HEAD\0{capabilities}\n");
        write_packet(output, line.as_bytes()).await?;
        first = false;
    }
    for (name, target) in refs {
        let separator = if first { "\0" } else { "" };
        let caps = if first { capabilities } else { "" };
        let line = format!(
            "{} {}{separator}{caps}\n",
            target,
            String::from_utf8_lossy(name)
        );
        write_packet(output, line.as_bytes()).await?;
        first = false;
        if let Some(peeled) = peeled.get(name) {
            let line = format!("{peeled} {}^{{}}\n", String::from_utf8_lossy(name));
            write_packet(output, line.as_bytes()).await?;
        }
    }
    if first {
        let line = format!("{ZERO_ID} capabilities^{{}}\0{capabilities}\n");
        write_packet(output, line.as_bytes()).await?;
    }
    write_flush(output).await?;
    Ok(())
}

async fn write_report(
    output: &mut (impl AsyncWrite + Unpin),
    names: &[Vec<u8>],
    success: bool,
    reason: &[u8],
) -> Result<(), Error> {
    if success {
        write_packet(output, b"unpack ok\n").await?;
        for name in names {
            let mut line = b"ok ".to_vec();
            line.extend_from_slice(name);
            line.push(b'\n');
            write_packet(output, &line).await?;
        }
    } else {
        let mut unpack = b"unpack ".to_vec();
        unpack.extend_from_slice(reason);
        unpack.push(b'\n');
        write_packet(output, &unpack).await?;
        for name in names {
            let mut line = b"ng ".to_vec();
            line.extend_from_slice(name);
            line.push(b' ');
            line.extend_from_slice(reason);
            line.push(b'\n');
            write_packet(output, &line).await?;
        }
    }
    write_flush(output).await?;
    Ok(())
}

enum Packet {
    Data(Vec<u8>),
    Flush,
}

async fn read_packet(
    input: &mut (impl AsyncRead + Unpin),
    total: &mut usize,
    limit: usize,
) -> Result<Packet, Error> {
    read_packet_optional(input, total, limit)
        .await?
        .ok_or(Error::Protocol("unexpected end of request"))
}

async fn read_packet_optional(
    input: &mut (impl AsyncRead + Unpin),
    total: &mut usize,
    limit: usize,
) -> Result<Option<Packet>, Error> {
    let mut prefix = [0; 4];
    let first = input.read(&mut prefix[..1]).await?;
    if first == 0 {
        return Ok(None);
    }
    input.read_exact(&mut prefix[1..]).await?;
    let packet =
        match decode::hex_prefix(&prefix).map_err(|_| Error::Protocol("invalid packet line"))? {
            decode::PacketLineOrWantedSize::Line(PacketLineRef::Flush) => Packet::Flush,
            decode::PacketLineOrWantedSize::Line(_) => {
                return Err(Error::Protocol("unsupported packet delimiter"));
            }
            decode::PacketLineOrWantedSize::Wanted(length) => {
                let mut data = vec![0; usize::from(length)];
                input.read_exact(&mut data).await?;
                decode::to_data_line(&data).map_err(|_| Error::Protocol("invalid packet line"))?;
                Packet::Data(data)
            }
        };
    *total = total
        .checked_add(match &packet {
            Packet::Data(data) => data.len() + 4,
            Packet::Flush => 4,
        })
        .filter(|total| *total <= limit)
        .ok_or(Error::Protocol("control data exceeds the byte limit"))?;
    Ok(Some(packet))
}

async fn write_packet(output: &mut (impl AsyncWrite + Unpin), data: &[u8]) -> Result<(), Error> {
    gix_packetline::decode::to_data_line(data)
        .map_err(|_| Error::Protocol("response packet is too large"))?;
    let length = u16::try_from(data.len() + 4)
        .map_err(|_| Error::Protocol("response packet is too large"))?;
    output.write_all(format!("{length:04x}").as_bytes()).await?;
    output.write_all(data).await?;
    Ok(())
}

async fn write_flush(output: &mut (impl AsyncWrite + Unpin)) -> Result<(), Error> {
    output.write_all(b"0000").await?;
    Ok(())
}

fn parse_optional_id(value: &[u8]) -> Result<Option<ObjectId>, Error> {
    if value == ZERO_ID.as_bytes() {
        Ok(None)
    } else {
        parse_id(value).map(Some)
    }
}

fn parse_id(value: &[u8]) -> Result<ObjectId, Error> {
    if value.len() != 40
        || !value
            .iter()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(Error::Protocol("invalid SHA-1 object ID"));
    }
    let value = std::str::from_utf8(value).map_err(|_| Error::Protocol("object ID is not text"))?;
    ObjectId::parse(ObjectFormat::Sha1, value).map_err(Error::Git)
}

fn trim_newline(value: &[u8]) -> &[u8] {
    value.strip_suffix(b"\n").unwrap_or(value)
}

fn split_once(value: &[u8], delimiter: u8) -> (&[u8], Option<&[u8]>) {
    value
        .iter()
        .position(|byte| *byte == delimiter)
        .map_or((value, None), |index| {
            (&value[..index], Some(&value[index + 1..]))
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_log::{LogId, Options, ValidatedBackend};
    use object_store::{memory::InMemory, path::Path as StorePath};

    use super::*;

    #[tokio::test]
    async fn receive_requires_capabilities_and_writes_no_false_success() -> Result<(), Error> {
        let id = "11".repeat(20);
        let mut valid = packet(
            format!("{ZERO_ID} {id} refs/heads/main\0 report-status object-format=sha1").as_bytes(),
        );
        valid.extend_from_slice(b"0000");
        let (updates, names) = parse_receive(&mut valid.as_slice()).await?;
        assert_eq!(updates.len(), 1);
        assert_eq!(names[0], b"refs/heads/main");

        let mut missing = packet(format!("{ZERO_ID} {id} refs/heads/main\0").as_bytes());
        missing.extend_from_slice(b"0000");
        assert!(parse_receive(&mut missing.as_slice()).await.is_err());

        let mut report = Vec::new();
        write_report(&mut report, &names, false, b"pending").await?;
        assert!(
            !report
                .windows(b"unpack ok".len())
                .any(|part| part == b"unpack ok")
        );
        assert!(
            report
                .windows(b"ng refs/heads/main pending".len())
                .any(|part| { part == b"ng refs/heads/main pending" })
        );
        Ok(())
    }

    #[tokio::test]
    async fn upload_distinguishes_negotiation_from_done() -> Result<(), Error> {
        let id = "11".repeat(20);
        let mut body = packet(format!("want {id} object-format=sha1\n").as_bytes());
        body.extend_from_slice(b"0000");
        body.extend_from_slice(&packet(format!("have {id}\n").as_bytes()));
        body.extend_from_slice(b"0000");
        let request = parse_upload(&mut body.as_slice()).await?;
        assert_eq!(request.wants.len(), 1);
        assert!(!request.done);

        let mut body = packet(format!("want {id}\n").as_bytes());
        body.extend_from_slice(b"0000");
        body.extend_from_slice(&packet(b"done\n"));
        let request = parse_upload(&mut body.as_slice()).await?;
        assert!(request.done);

        let mut body = packet(format!("want {id} side-band-64k\n").as_bytes());
        body.extend_from_slice(b"00000000");
        assert!(parse_upload(&mut body.as_slice()).await.is_err());

        let mut body = packet(format!("want {id}\n").as_bytes());
        body.extend_from_slice(b"0000");
        body.extend_from_slice(&packet(b"done\n"));
        body.push(b'x');
        assert!(parse_upload(&mut body.as_slice()).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn negotiation_round_returns_only_nak() -> Result<(), Box<dyn std::error::Error>> {
        let backend = ValidatedBackend::new(
            Arc::new(InMemory::new()),
            StorePath::from("upload-negotiation-test"),
        )
        .await?;
        let log = Log::open(backend.scope(&LogId::new("repo")?), Options::default()).await?;
        let scratch = tempfile::tempdir()?;
        let endpoint = SmartHttp::new(log, scratch.path());
        let id = "11".repeat(20);
        let mut body = packet(format!("want {id}\n").as_bytes());
        body.extend_from_slice(b"00000000");
        let mut output = Vec::new();
        endpoint
            .upload_pack(&mut body.as_slice(), &mut output)
            .await?;
        assert_eq!(output, b"0008NAK\n");

        let mut body = packet(format!("want {id}\n").as_bytes());
        body.extend_from_slice(b"0000");
        body.extend_from_slice(&packet(b"done\n"));
        output.clear();
        assert!(
            endpoint
                .upload_pack(&mut body.as_slice(), &mut output)
                .await
                .is_err()
        );
        assert!(output.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn fragmented_pack_header_is_accepted() -> Result<(), Box<dyn std::error::Error>> {
        let bytes = b"PACKfragmented".to_vec();
        let expected = bytes.clone();
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let writing = tokio::spawn(async move {
            for byte in bytes {
                writer.write_all(&[byte]).await?;
            }
            Ok::<_, std::io::Error>(())
        });
        let scratch = tempfile::tempdir()?;
        let path = scratch.path().join("pack");

        assert_eq!(
            spool_pack(&mut reader, &path).await?,
            u64::try_from(expected.len())?
        );
        writing.await??;
        assert_eq!(tokio::fs::read(path).await?, expected);
        Ok(())
    }

    #[test]
    fn service_metadata_is_exact() {
        assert_eq!(
            Service::UploadPack.advertisement_content_type(),
            "application/x-git-upload-pack-advertisement"
        );
        assert_eq!(
            Service::ReceivePack.result_content_type(),
            "application/x-git-receive-pack-result"
        );
        assert_eq!(Service::UploadPack.cache_control(), CACHE_CONTROL);
    }

    fn packet(data: &[u8]) -> Vec<u8> {
        let mut packet = format!("{:04x}", data.len() + 4).into_bytes();
        packet.extend_from_slice(data);
        packet
    }
}
