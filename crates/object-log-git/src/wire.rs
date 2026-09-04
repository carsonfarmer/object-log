use std::io::{self, Write};

use gix_packetline::{Channel, PacketLineRef, blocking_io::encode, decode};

use crate::{ObjectFormat, ObjectId, RefUpdate, pack::MAX_RECEIVE_PACK_BYTES};

const MAX_UPLOAD_BYTES: usize = 9 * 1024 * 1024;
const MAX_RECEIVE_BYTES: usize = 1024 * 1024;
const MAX_FETCH_PACK_BYTES: usize = 9_437_184;
const MAX_FETCH_RESPONSE_BYTES: usize = 9_437_926;
const MAX_COMMANDS: usize = 1_024;
const MAX_ITEMS: usize = 32_768;
const MAX_PACKET_PAYLOAD: usize = 65_515;
const UPLOAD_SHA1: &[u8] = b"000eversion 2\n0015agent=object-log\n0013ls-refs=unborn\n000afetch\n0017object-format=sha1\n0000";
const UPLOAD_SHA256: &[u8] = b"000eversion 2\n0015agent=object-log\n0013ls-refs=unborn\n000afetch\n0019object-format=sha256\n0000";

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invalid Git protocol: {0}")]
    Protocol(&'static str),
    #[error("Git protocol limit exceeded: {0}")]
    Limit(&'static str),
    #[error("Git protocol output failed: {0}")]
    Io(#[from] io::Error),
}

pub(crate) enum UploadRequest<'a> {
    LsRefs {
        peel: bool,
        symrefs: bool,
        unborn: bool,
        prefixes: Box<[&'a [u8]]>,
    },
    Fetch {
        wants: Box<[ObjectId]>,
        haves: Box<[ObjectId]>,
        done: bool,
        thin_pack: bool,
        ofs_delta: bool,
        include_tag: bool,
    },
}

pub(crate) struct ReceiveRequest<'a> {
    pub(crate) updates: Box<[RefUpdate]>,
    pub(crate) pack: &'a [u8],
    pub(crate) report_status: bool,
}

pub(crate) struct AdvertisedRef<'a> {
    pub(crate) name: &'a [u8],
    pub(crate) target: Option<ObjectId>,
    pub(crate) peeled: Option<ObjectId>,
    pub(crate) symref_target: Option<&'a [u8]>,
}

#[derive(Clone, Copy)]
pub(crate) enum FetchReply<'a> {
    Acknowledgments(&'a [ObjectId]),
    Pack(&'a [u8]),
}

#[derive(Clone, Copy)]
pub(crate) enum ReceiveStatus<'a> {
    Success,
    Rejected(&'a [u8]),
    InvalidPack(&'a [u8]),
}

pub(crate) fn write_upload_advertisement(
    output: &mut impl Write,
    format: ObjectFormat,
) -> Result<(), Error> {
    output.write_all(match format {
        ObjectFormat::Sha1 => UPLOAD_SHA1,
        ObjectFormat::Sha256 => UPLOAD_SHA256,
    })?;
    Ok(())
}

pub(crate) fn parse_upload(input: &[u8], format: ObjectFormat) -> Result<UploadRequest<'_>, Error> {
    within(input.len(), MAX_UPLOAD_BYTES, "upload control bytes")?;
    let mut packets = input;
    let command = match packet(&mut packets)? {
        PacketLineRef::Data(line) => text(line),
        _ => return Err(Error::Protocol("missing command")),
    }
    .strip_prefix(b"command=")
    .ok_or(Error::Protocol("missing command"))?;
    let mut capabilities = 0_u8;
    while let Some(line) = data_until(&mut packets, PacketLineRef::Delimiter)? {
        let line = text(line);
        let bit = if line == format_capability(format) {
            1
        } else if valid_agent(line) {
            2
        } else {
            return Err(Error::Protocol("unsupported command capability"));
        };
        if capabilities & bit != 0 {
            return Err(Error::Protocol("duplicate command capability"));
        }
        capabilities |= bit;
    }
    if format == ObjectFormat::Sha256 && capabilities & 1 == 0 {
        return Err(Error::Protocol("missing object-format capability"));
    }
    let request = match command {
        b"ls-refs" => parse_ls_refs(&mut packets)?,
        b"fetch" => parse_fetch(&mut packets, format)?,
        _ => return Err(Error::Protocol("unsupported command")),
    };
    if !packets.is_empty() {
        return Err(Error::Protocol("trailing upload data"));
    }
    Ok(request)
}

fn parse_ls_refs<'a>(packets: &mut &'a [u8]) -> Result<UploadRequest<'a>, Error> {
    let (mut peel, mut symrefs, mut unborn) = (false, false, false);
    let mut prefixes = Vec::new();
    while let Some(line) = data_until(packets, PacketLineRef::Flush)? {
        match text(line) {
            b"peel" if !peel => peel = true,
            b"symrefs" if !symrefs => symrefs = true,
            b"unborn" if !unborn => unborn = true,
            line if line.starts_with(b"ref-prefix ") => {
                within(prefixes.len() + 1, MAX_COMMANDS, "ref prefixes")?;
                prefixes.push(&line[b"ref-prefix ".len()..]);
            }
            _ => return Err(Error::Protocol("unsupported ls-refs argument")),
        }
    }
    prefixes.sort_unstable();
    prefixes.dedup();
    Ok(UploadRequest::LsRefs {
        peel,
        symrefs,
        unborn,
        prefixes: prefixes.into_boxed_slice(),
    })
}

fn parse_fetch<'a>(
    packets: &mut &'a [u8],
    format: ObjectFormat,
) -> Result<UploadRequest<'a>, Error> {
    let (mut wants, mut haves) = (Vec::new(), Vec::new());
    let (mut done, mut options) = (false, 0_u8);
    while let Some(line) = data_until(packets, PacketLineRef::Flush)? {
        let line = text(line);
        if done {
            return Err(Error::Protocol("fetch argument follows done"));
        }
        let option = match line {
            b"thin-pack" => Some(1),
            b"ofs-delta" => Some(2),
            b"include-tag" => Some(4),
            b"no-progress" => Some(8),
            _ => None,
        };
        if let Some(bit) = option {
            if options & bit != 0 {
                return Err(Error::Protocol("duplicate fetch option"));
            }
            options |= bit;
        } else if line == b"done" && !wants.is_empty() {
            done = true;
        } else if let Some(id) = line.strip_prefix(b"want ") {
            within(wants.len() + 1, MAX_COMMANDS, "wants")?;
            wants.push(parse_id(id, format)?);
        } else if let Some(id) = line.strip_prefix(b"have ") {
            within(haves.len() + 1, MAX_ITEMS, "haves")?;
            haves.push(parse_id(id, format)?);
        } else {
            return Err(Error::Protocol("unsupported fetch argument"));
        }
    }
    if wants.is_empty() {
        return Err(Error::Protocol("fetch has no wants"));
    }
    wants.sort_unstable();
    wants.dedup();
    haves.sort_unstable();
    haves.dedup();
    Ok(UploadRequest::Fetch {
        wants: wants.into_boxed_slice(),
        haves: haves.into_boxed_slice(),
        done,
        thin_pack: options & 1 != 0,
        ofs_delta: options & 2 != 0,
        include_tag: options & 4 != 0,
    })
}

pub(crate) fn write_ls_refs(
    output: &mut impl Write,
    format: ObjectFormat,
    refs: &[AdvertisedRef<'_>],
) -> Result<(), Error> {
    within(refs.len(), MAX_ITEMS, "advertised refs")?;
    for advertised in refs {
        validate_ref(advertised, format, true)?;
        packet_size(&[
            advertised.target.map_or(6, |_| format.digest_len() * 2),
            1,
            advertised.name.len(),
            advertised
                .symref_target
                .map_or(0, |target| 15 + target.len()),
            advertised.peeled.map_or(0, |_| 8 + format.digest_len() * 2),
        ])?;
    }
    let mut line = Vec::with_capacity(128);
    for advertised in refs {
        match advertised.target {
            Some(target) => push_id(&mut line, target),
            None => line.extend_from_slice(b"unborn"),
        }
        line.push(b' ');
        line.extend_from_slice(advertised.name);
        if let Some(target) = advertised.symref_target {
            line.extend_from_slice(b" symref-target:");
            line.extend_from_slice(target);
        }
        if let Some(peeled) = advertised.peeled {
            line.extend_from_slice(b" peeled:");
            push_id(&mut line, peeled);
        }
        write_text(output, &mut line)?;
    }
    flush(output)
}

pub(crate) fn write_fetch(
    output: &mut impl Write,
    format: ObjectFormat,
    reply: FetchReply<'_>,
) -> Result<(), Error> {
    match reply {
        FetchReply::Acknowledgments(ids) => {
            within(ids.len(), MAX_ITEMS, "acknowledgments")?;
            ids.iter().try_for_each(|id| validate_id(*id, format))?;
            encode::text_to_write(b"acknowledgments", &mut *output)?;
            if ids.is_empty() {
                encode::text_to_write(b"NAK", &mut *output)?;
            } else {
                let mut line = Vec::with_capacity(69);
                for id in ids {
                    line.extend_from_slice(b"ACK ");
                    push_id(&mut line, *id);
                    write_text(output, &mut line)?;
                }
            }
        }
        FetchReply::Pack(pack) => {
            within(pack.len(), MAX_FETCH_PACK_BYTES, "pack bytes")?;
            within(
                fetch_response_len(pack.len())?,
                MAX_FETCH_RESPONSE_BYTES,
                "fetch response bytes",
            )?;
            write_pack(output, pack)?;
        }
    }
    flush(output)
}

pub(crate) fn write_receive_advertisement(
    output: &mut impl Write,
    format: ObjectFormat,
    refs: &[AdvertisedRef<'_>],
) -> Result<(), Error> {
    within(refs.len(), MAX_ITEMS, "advertised refs")?;
    validate_receive_advertisement(format, refs)?;
    let mut line = Vec::with_capacity(192);
    if refs.is_empty() {
        line.extend(std::iter::repeat_n(b'0', format.digest_len() * 2));
        line.extend_from_slice(b" capabilities^{}");
        push_receive_capabilities(&mut line, format);
        write_text(output, &mut line)?;
    } else {
        for (index, advertised) in refs.iter().enumerate() {
            let target = advertised
                .target
                .ok_or(Error::Protocol("receive advertisement has an unborn ref"))?;
            push_id(&mut line, target);
            line.push(b' ');
            line.extend_from_slice(advertised.name);
            if index == 0 {
                push_receive_capabilities(&mut line, format);
            }
            write_text(output, &mut line)?;
            if let Some(peeled) = advertised.peeled {
                push_id(&mut line, peeled);
                line.push(b' ');
                line.extend_from_slice(advertised.name);
                line.extend_from_slice(b"^{}");
                write_text(output, &mut line)?;
            }
        }
    }
    flush(output)
}

pub(crate) fn parse_receive(
    input: &[u8],
    format: ObjectFormat,
) -> Result<ReceiveRequest<'_>, Error> {
    let mut packets = input;
    let mut updates = Vec::new();
    let mut capabilities = 0_u8;
    while let Some(line) = data_until(&mut packets, PacketLineRef::Flush)? {
        within(
            input.len() - packets.len(),
            MAX_RECEIVE_BYTES,
            "control bytes",
        )?;
        within(updates.len() + 1, MAX_COMMANDS, "ref commands")?;
        let line = text(line);
        let command = if updates.is_empty() {
            let nul = line
                .iter()
                .position(|byte| *byte == 0)
                .ok_or(Error::Protocol("first ref command has no capabilities"))?;
            for capability in line[nul + 1..]
                .split(|byte| *byte == b' ')
                .filter(|capability| !capability.is_empty())
            {
                let bit = match capability {
                    b"report-status" => 1,
                    value if value == format_capability(format) => 2,
                    b"atomic" => 4,
                    b"ofs-delta" => 8,
                    value if valid_agent(value) => 16,
                    _ => return Err(Error::Protocol("unsupported receive capability")),
                };
                if capabilities & bit != 0 {
                    return Err(Error::Protocol("duplicate receive capability"));
                }
                capabilities |= bit;
            }
            &line[..nul]
        } else if line.contains(&0) {
            return Err(Error::Protocol("late receive capabilities"));
        } else {
            line
        };
        updates.push(parse_update(command, format)?);
    }
    within(
        input.len() - packets.len(),
        MAX_RECEIVE_BYTES,
        "control bytes",
    )?;
    if updates.is_empty() || (format == ObjectFormat::Sha256 && capabilities & 2 == 0) {
        return Err(Error::Protocol("missing receive requirements"));
    }
    let mut names: Vec<_> = updates
        .iter()
        .map(|update| update.name.as_slice())
        .collect();
    names.sort_unstable();
    if names.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(Error::Protocol("duplicate ref command"));
    }
    let pack = packets;
    within(pack.len(), MAX_RECEIVE_PACK_BYTES, "pack bytes")?;
    let needs_pack = updates.iter().any(|update| update.target.is_some());
    if needs_pack && (pack.is_empty() || !pack.starts_with(b"PACK")) {
        return Err(Error::Protocol("create or update has no Git pack"));
    }
    if !needs_pack && !pack.is_empty() {
        return Err(Error::Protocol("delete-only request has a Git pack"));
    }
    Ok(ReceiveRequest {
        updates: updates.into_boxed_slice(),
        pack,
        report_status: capabilities & 1 != 0,
    })
}

pub(crate) fn write_receive_status(
    output: &mut impl Write,
    updates: &[RefUpdate],
    status: ReceiveStatus<'_>,
) -> Result<(), Error> {
    within(updates.len(), MAX_COMMANDS, "ref statuses")?;
    if updates.is_empty() {
        return Err(Error::Protocol("receive status has no ref commands"));
    }
    let (unpack, rejection) = match status {
        ReceiveStatus::Success => (b"ok".as_slice(), None),
        ReceiveStatus::Rejected(reason) => (b"ok".as_slice(), Some(reason)),
        ReceiveStatus::InvalidPack(reason) => (reason, Some(reason)),
    };
    if let Some(reason) = rejection {
        validate_reason(reason)?;
    }
    packet_size(&[7, unpack.len()])?;
    for update in updates {
        packet_size(&[
            3,
            update.name.len(),
            rejection.map_or(0, |reason| 1 + reason.len()),
        ])?;
    }
    let mut line = b"unpack ".to_vec();
    line.extend_from_slice(unpack);
    write_text(output, &mut line)?;
    for update in updates {
        if let Some(reason) = rejection {
            line.extend_from_slice(b"ng ");
            line.extend_from_slice(&update.name);
            line.push(b' ');
            line.extend_from_slice(reason);
        } else {
            line.extend_from_slice(b"ok ");
            line.extend_from_slice(&update.name);
        }
        write_text(output, &mut line)?;
    }
    flush(output)
}

fn validate_receive_advertisement(
    format: ObjectFormat,
    refs: &[AdvertisedRef<'_>],
) -> Result<(), Error> {
    let capability_bytes = receive_capabilities_len(format);
    if refs.is_empty() {
        return packet_size(&[
            format.digest_len() * 2,
            1,
            b"capabilities^{}".len(),
            capability_bytes,
        ]);
    }
    for (index, advertised) in refs.iter().enumerate() {
        validate_ref(advertised, format, false)?;
        if (advertised.name == b"HEAD" && index != 0)
            || index > 0 && refs[index - 1].name >= advertised.name
        {
            return Err(Error::Protocol("receive refs are not in C-locale order"));
        }
        packet_size(&[
            format.digest_len() * 2,
            1,
            advertised.name.len(),
            if index == 0 { capability_bytes } else { 0 },
        ])?;
        if advertised.peeled.is_some() {
            packet_size(&[format.digest_len() * 2, 1, advertised.name.len(), 3])?;
        }
    }
    Ok(())
}

fn validate_ref(
    advertised: &AdvertisedRef<'_>,
    format: ObjectFormat,
    allow_unborn: bool,
) -> Result<(), Error> {
    if advertised.name != b"HEAD" && !crate::is_valid_ref_name(advertised.name) {
        return Err(Error::Protocol("invalid advertised ref name"));
    }
    if advertised.target.is_none() && (!allow_unborn || advertised.peeled.is_some()) {
        return Err(Error::Protocol("receive advertisement has an unborn ref"));
    }
    for id in [advertised.target, advertised.peeled].into_iter().flatten() {
        validate_id(id, format)?;
    }
    if let Some(target) = advertised.symref_target
        && !crate::is_valid_ref_name(target)
    {
        return Err(Error::Protocol("invalid symbolic ref target"));
    }
    Ok(())
}

fn validate_id(id: ObjectId, format: ObjectFormat) -> Result<(), Error> {
    (id.format() == format)
        .then_some(())
        .ok_or(Error::Protocol("object ID uses the wrong format"))
}

fn validate_reason(reason: &[u8]) -> Result<(), Error> {
    if reason.is_empty() || reason == b"ok" || reason.contains(&b'\n') || reason.contains(&0) {
        return Err(Error::Protocol("invalid receive status reason"));
    }
    Ok(())
}

fn packet_size(parts: &[usize]) -> Result<(), Error> {
    let size = parts
        .iter()
        .try_fold(0_usize, |total, part| total.checked_add(*part))
        .ok_or(Error::Limit("packet text bytes"))?;
    within(size, MAX_PACKET_PAYLOAD, "packet text bytes")
}

fn receive_capabilities_len(format: ObjectFormat) -> usize {
    b"\0report-status delete-refs atomic ofs-delta ".len()
        + format_capability(format).len()
        + b" agent=object-log".len()
}

fn parse_update(command: &[u8], format: ObjectFormat) -> Result<RefUpdate, Error> {
    let mut fields = command.split(|byte| *byte == b' ');
    let (Some(old), Some(new), Some(name), None) =
        (fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return Err(Error::Protocol("invalid ref command"));
    };
    RefUpdate::new(
        name,
        parse_optional_id(old, format)?,
        parse_optional_id(new, format)?,
    )
    .map_err(|_| Error::Protocol("invalid ref command"))
}

fn parse_optional_id(value: &[u8], format: ObjectFormat) -> Result<Option<ObjectId>, Error> {
    if value.len() == format.digest_len() * 2 && value.iter().all(|byte| *byte == b'0') {
        Ok(None)
    } else {
        parse_id(value, format).map(Some)
    }
}

fn parse_id(value: &[u8], format: ObjectFormat) -> Result<ObjectId, Error> {
    ObjectId::parse(
        format,
        std::str::from_utf8(value).map_err(|_| Error::Protocol("invalid object ID"))?,
    )
    .map_err(|_| Error::Protocol("invalid object ID"))
}

fn within(value: usize, maximum: usize, label: &'static str) -> Result<(), Error> {
    (value <= maximum).then_some(()).ok_or(Error::Limit(label))
}

fn write_text(output: &mut impl Write, line: &mut Vec<u8>) -> Result<(), Error> {
    encode::text_to_write(line, output)?;
    line.clear();
    Ok(())
}

fn write_pack(output: &mut impl Write, pack: &[u8]) -> Result<(), Error> {
    encode::text_to_write(b"packfile", &mut *output)?;
    for chunk in pack.chunks(MAX_PACKET_PAYLOAD) {
        encode::band_to_write(Channel::Data, chunk, &mut *output)?;
    }
    Ok(())
}

fn fetch_response_len(pack: usize) -> Result<usize, Error> {
    let chunks = pack.div_ceil(MAX_PACKET_PAYLOAD);
    pack.checked_add(17 + chunks * 5)
        .ok_or(Error::Limit("fetch response bytes"))
}

fn flush(output: &mut impl Write) -> Result<(), Error> {
    encode::flush_to_write(output)?;
    Ok(())
}

fn push_receive_capabilities(line: &mut Vec<u8>, format: ObjectFormat) {
    line.extend_from_slice(b"\0report-status delete-refs atomic ofs-delta ");
    line.extend_from_slice(format_capability(format));
    line.extend_from_slice(b" agent=object-log");
}

fn push_id(line: &mut Vec<u8>, id: ObjectId) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in id.as_bytes() {
        line.push(HEX[usize::from(byte >> 4)]);
        line.push(HEX[usize::from(byte & 0x0f)]);
    }
}

const fn format_capability(format: ObjectFormat) -> &'static [u8] {
    match format {
        ObjectFormat::Sha1 => b"object-format=sha1",
        ObjectFormat::Sha256 => b"object-format=sha256",
    }
}

fn valid_agent(capability: &[u8]) -> bool {
    capability
        .strip_prefix(b"agent=")
        .is_some_and(|agent| !agent.is_empty() && agent.iter().all(u8::is_ascii_graphic))
}

fn text(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\n").unwrap_or(line)
}

fn data_until<'a>(
    input: &mut &'a [u8],
    delimiter: PacketLineRef<'static>,
) -> Result<Option<&'a [u8]>, Error> {
    match packet(input)? {
        line if line == delimiter => Ok(None),
        PacketLineRef::Data(data) => Ok(Some(data)),
        _ => Err(Error::Protocol("unexpected packet delimiter")),
    }
}

fn packet<'a>(input: &mut &'a [u8]) -> Result<PacketLineRef<'a>, Error> {
    let line = decode::all_at_once(input).map_err(|_| Error::Protocol("invalid packet line"))?;
    let bytes = line.as_slice().map_or(4, |data| data.len() + 4);
    *input = &input[bytes..];
    Ok(line)
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

    const SHA1_A: &str = "1cca161013c9b0595b0a4637cbed4eb259f9973a";
    const SHA1_B: &str = "792b551e898164e75e5b7abf04612881a8f478c3";
    const SHA256_A: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const EMPTY_SHA1_PACK: &[u8] = b"PACK\0\0\0\x02\0\0\0\0\x02\x9d\x08\x82\x3b\xd8\xa8\xea\xb5\x10\xad\x6a\xc7\x5c\x82\x3c\xfd\x3e\xd3\x1e";
    const EMPTY_SHA256_PACK: &[u8] = b"PACK\0\0\0\x02\0\0\0\0\x7e\xd8\x90\xd8\xa4\x57\x60\xf3\xee\xcf\x73\x04\x5b\x1d\x10\x47\x08\x5a\xf4\x77\x6d\xc6\x83\xd7\x8e\xac\x82\x20\x3d\xf1\x99\x3f";

    #[test]
    fn upload_advertisement_matches_protocol_v2() -> TestResult {
        let mut output = Vec::new();
        write_upload_advertisement(&mut output, ObjectFormat::Sha1)?;
        assert_eq!(output, UPLOAD_SHA1);
        output.clear();
        write_upload_advertisement(&mut output, ObjectFormat::Sha256)?;
        assert_eq!(output, UPLOAD_SHA256);
        Ok(())
    }

    #[test]
    fn parses_git_254_ls_refs_fixture_without_copying_prefixes() -> TestResult {
        let input = concat!(
            "0014command=ls-refs\n",
            "001cagent=git/2.54.0-Darwin\n",
            "0017object-format=sha1\n",
            "0001",
            "0009peel\n",
            "000csymrefs\n",
            "000bunborn\n",
            "001bref-prefix refs/heads/\n",
            "001aref-prefix refs/tags/\n",
            "0014ref-prefix HEAD\n",
            "0000"
        )
        .as_bytes();
        let UploadRequest::LsRefs {
            peel,
            symrefs,
            unborn,
            prefixes,
        } = parse_upload(input, ObjectFormat::Sha1)?
        else {
            return Err("expected ls-refs".into());
        };
        assert!(peel && symrefs && unborn);
        assert_eq!(
            prefixes.as_ref(),
            [b"HEAD".as_slice(), b"refs/heads/", b"refs/tags/"]
        );
        assert!(
            prefixes
                .iter()
                .all(|prefix| input.as_ptr_range().contains(&prefix.as_ptr()))
        );
        Ok(())
    }

    #[test]
    fn parses_fetch_flags_and_deduplicates_negotiation_ids() -> TestResult {
        let input = concat!(
            "0012command=fetch\n",
            "001cagent=git/2.54.0-Darwin\n",
            "0017object-format=sha1\n",
            "0001",
            "000ethin-pack\n",
            "0032want 1cca161013c9b0595b0a4637cbed4eb259f9973a\n",
            "0010no-progress\n",
            "000eofs-delta\n",
            "0032want 1cca161013c9b0595b0a4637cbed4eb259f9973a\n",
            "0032have 792b551e898164e75e5b7abf04612881a8f478c3\n",
            "0010include-tag\n",
            "0032want 792b551e898164e75e5b7abf04612881a8f478c3\n",
            "0009done\n",
            "0000"
        )
        .as_bytes();
        let UploadRequest::Fetch {
            wants,
            haves,
            done,
            thin_pack,
            ofs_delta,
            include_tag,
        } = parse_upload(input, ObjectFormat::Sha1)?
        else {
            return Err("expected fetch".into());
        };
        assert_eq!(
            wants.as_ref(),
            [
                id(ObjectFormat::Sha1, SHA1_A)?,
                id(ObjectFormat::Sha1, SHA1_B)?
            ]
        );
        assert_eq!(haves.as_ref(), [id(ObjectFormat::Sha1, SHA1_B)?]);
        assert!(done && thin_pack && ofs_delta && include_tag);

        let input = upload(
            b"fetch",
            ObjectFormat::Sha256,
            &[format!("want {SHA256_A}").as_bytes()],
        )?;
        let UploadRequest::Fetch {
            thin_pack,
            ofs_delta,
            ..
        } = parse_upload(&input, ObjectFormat::Sha256)?
        else {
            return Err("expected fetch".into());
        };
        assert!(!thin_pack && !ofs_delta);
        Ok(())
    }

    #[test]
    fn rejects_invalid_upload_framing_and_arguments() -> TestResult {
        let valid = upload(b"ls-refs", ObjectFormat::Sha1, &[])?;
        protocol(parse_upload(&valid[..valid.len() - 1], ObjectFormat::Sha1));
        invalid_upload(b"unknown", &[])?;
        invalid_upload(b"ls-refs", &[b"peel", b"peel"])?;
        invalid_upload(b"fetch", &[b"thin-pack"])?;
        invalid_upload(b"fetch", &[b"want 00".as_slice(), b"done"])?;
        invalid_upload(
            b"fetch",
            &[format!("want {SHA1_A}").as_bytes(), b"done", b"ofs-delta"],
        )?;
        for option in [
            b"thin-pack".as_slice(),
            b"ofs-delta",
            b"include-tag",
            b"no-progress",
        ] {
            invalid_upload(
                b"fetch",
                &[format!("want {SHA1_A}").as_bytes(), option, option],
            )?;
        }
        let missing_format = upload_with(b"ls-refs", None, &[], false)?;
        assert!(parse_upload(&missing_format, ObjectFormat::Sha1).is_ok());
        protocol(parse_upload(&missing_format, ObjectFormat::Sha256));
        Ok(())
    }

    #[test]
    fn accepts_optional_line_feeds_and_empty_prefixes() -> TestResult {
        for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
            for line_feed in [false, true] {
                let input = upload_with(b"ls-refs", Some(format), &[b"ref-prefix "], line_feed)?;
                let UploadRequest::LsRefs { prefixes, .. } = parse_upload(&input, format)? else {
                    return Err("expected ls-refs".into());
                };
                assert_eq!(prefixes.as_ref(), [b"".as_slice()]);
            }
        }
        Ok(())
    }

    #[test]
    fn enforces_upload_count_and_byte_limits() -> TestResult {
        let want = format!("want {SHA1_A}");
        let mut wants = vec![want.as_bytes(); MAX_COMMANDS];
        assert!(
            parse_upload(
                &upload(b"fetch", ObjectFormat::Sha1, &wants)?,
                ObjectFormat::Sha1
            )
            .is_ok()
        );
        wants.push(want.as_bytes());
        limit(parse_upload(
            &upload(b"fetch", ObjectFormat::Sha1, &wants)?,
            ObjectFormat::Sha1,
        ));

        let have = format!("have {SHA1_B}");
        let mut args = vec![want.as_bytes()];
        args.extend(std::iter::repeat_n(have.as_bytes(), MAX_ITEMS));
        assert!(
            parse_upload(
                &upload(b"fetch", ObjectFormat::Sha1, &args)?,
                ObjectFormat::Sha1
            )
            .is_ok()
        );
        args.push(have.as_bytes());
        limit(parse_upload(
            &upload(b"fetch", ObjectFormat::Sha1, &args)?,
            ObjectFormat::Sha1,
        ));
        let mut prefixes = vec![b"ref-prefix refs/".as_slice(); MAX_COMMANDS];
        assert!(
            parse_upload(
                &upload(b"ls-refs", ObjectFormat::Sha1, &prefixes)?,
                ObjectFormat::Sha1,
            )
            .is_ok()
        );
        prefixes.push(b"ref-prefix refs/");
        limit(parse_upload(
            &upload(b"ls-refs", ObjectFormat::Sha1, &prefixes)?,
            ObjectFormat::Sha1,
        ));
        let exact = sized_upload(MAX_UPLOAD_BYTES)?;
        assert_eq!(exact.len(), MAX_UPLOAD_BYTES);
        assert!(parse_upload(&exact, ObjectFormat::Sha1).is_ok());
        let excessive = sized_upload(MAX_UPLOAD_BYTES + 1)?;
        limit(parse_upload(&excessive, ObjectFormat::Sha1));
        Ok(())
    }

    #[test]
    fn writes_exact_ls_refs_fixture() -> TestResult {
        let a = id(ObjectFormat::Sha1, SHA1_A)?;
        let b = id(ObjectFormat::Sha1, SHA1_B)?;
        let refs = [
            AdvertisedRef {
                name: b"HEAD",
                target: Some(a),
                peeled: None,
                symref_target: Some(b"refs/heads/main"),
            },
            AdvertisedRef {
                name: b"refs/tags/v1",
                target: Some(a),
                peeled: Some(b),
                symref_target: None,
            },
            AdvertisedRef {
                name: b"refs/heads/new",
                target: None,
                peeled: None,
                symref_target: Some(b"refs/heads/main"),
            },
        ];
        let mut output = Vec::new();
        write_ls_refs(&mut output, ObjectFormat::Sha1, &refs)?;
        assert_eq!(
            output,
            concat!(
                "00501cca161013c9b0595b0a4637cbed4eb259f9973a HEAD symref-target:refs/heads/main\n",
                "006a1cca161013c9b0595b0a4637cbed4eb259f9973a refs/tags/v1 peeled:792b551e898164e75e5b7abf04612881a8f478c3\n",
                "0038unborn refs/heads/new symref-target:refs/heads/main\n",
                "0000"
            )
            .as_bytes()
        );
        Ok(())
    }

    #[test]
    fn writes_sha256_ids_and_rejects_mixed_formats_before_output() -> TestResult {
        let sha1 = id(ObjectFormat::Sha1, SHA1_A)?;
        let sha256 = id(ObjectFormat::Sha256, SHA256_A)?;
        let refs = [AdvertisedRef {
            name: b"refs/heads/main",
            target: Some(sha256),
            peeled: None,
            symref_target: None,
        }];
        let mut output = Vec::new();
        write_ls_refs(&mut output, ObjectFormat::Sha256, &refs)?;
        assert_eq!(
            output,
            format!("0055{SHA256_A} refs/heads/main\n0000").as_bytes()
        );
        output.clear();
        write_fetch(
            &mut output,
            ObjectFormat::Sha256,
            FetchReply::Acknowledgments(&[sha256]),
        )?;
        assert_eq!(
            output,
            format!("0014acknowledgments\n0049ACK {SHA256_A}\n0000").as_bytes()
        );

        output.clear();
        protocol(write_fetch(
            &mut output,
            ObjectFormat::Sha256,
            FetchReply::Acknowledgments(&[sha256, sha1]),
        ));
        assert!(output.is_empty());
        Ok(())
    }

    #[test]
    fn writes_fetch_sections_and_maximum_sideband_chunks() -> TestResult {
        let a = id(ObjectFormat::Sha1, SHA1_A)?;
        let mut output = Vec::new();
        write_fetch(
            &mut output,
            ObjectFormat::Sha1,
            FetchReply::Acknowledgments(&[]),
        )?;
        assert_eq!(output, b"0014acknowledgments\n0008NAK\n0000");

        let pack = vec![7; MAX_PACKET_PAYLOAD * 2 + 1];
        output.clear();
        write_fetch(
            &mut output,
            ObjectFormat::Sha1,
            FetchReply::Acknowledgments(&[a]),
        )?;
        assert_eq!(
            output,
            format!("0014acknowledgments\n0031ACK {SHA1_A}\n0000").as_bytes()
        );
        output.clear();
        write_fetch(&mut output, ObjectFormat::Sha1, FetchReply::Pack(&pack))?;
        assert!(output.starts_with(b"000dpackfile\n"));
        let mut packets = output.as_slice();
        assert_eq!(text(data(packet(&mut packets)?)?), b"packfile");
        let mut rebuilt = Vec::new();
        let mut chunks = 0;
        loop {
            match packet(&mut packets)? {
                PacketLineRef::Flush => break,
                PacketLineRef::Data(line) => {
                    if line.first() != Some(&(Channel::Data as u8)) {
                        return Err("missing data sideband".into());
                    }
                    let band = &line[1..];
                    assert_eq!(band, &pack[rebuilt.len()..rebuilt.len() + band.len()]);
                    assert!(band.len() <= MAX_PACKET_PAYLOAD);
                    rebuilt.extend_from_slice(band);
                    chunks += 1;
                }
                _ => return Err("unexpected fetch packet".into()),
            }
        }
        assert_eq!(chunks, 3);
        assert_eq!(rebuilt, pack);
        assert!(packets.is_empty());
        assert_eq!(sideband_chunks(MAX_PACKET_PAYLOAD)?, 1);
        assert_eq!(sideband_chunks(MAX_PACKET_PAYLOAD + 1)?, 2);
        assert_eq!(
            fetch_response_len(MAX_FETCH_PACK_BYTES)?,
            MAX_FETCH_RESPONSE_BYTES
        );

        let pack = vec![0; MAX_FETCH_PACK_BYTES + 1];
        write_fetch(
            &mut io::sink(),
            ObjectFormat::Sha1,
            FetchReply::Pack(&pack[..MAX_FETCH_PACK_BYTES]),
        )?;
        output.clear();
        limit(write_fetch(
            &mut output,
            ObjectFormat::Sha1,
            FetchReply::Pack(&pack),
        ));
        assert!(output.is_empty());
        Ok(())
    }

    #[test]
    fn writes_classic_receive_advertisements() -> TestResult {
        let a = id(ObjectFormat::Sha1, SHA1_A)?;
        let b = id(ObjectFormat::Sha1, SHA1_B)?;
        let refs = [AdvertisedRef {
            name: b"refs/tags/v1",
            target: Some(a),
            peeled: Some(b),
            symref_target: None,
        }];
        let mut output = Vec::new();
        write_receive_advertisement(&mut output, ObjectFormat::Sha1, &refs)?;
        assert_eq!(
            output,
            concat!(
                "00891cca161013c9b0595b0a4637cbed4eb259f9973a refs/tags/v1\0report-status delete-refs atomic ofs-delta object-format=sha1 agent=object-log\n",
                "003d792b551e898164e75e5b7abf04612881a8f478c3 refs/tags/v1^{}\n",
                "0000"
            )
            .as_bytes()
        );

        output.clear();
        write_receive_advertisement(&mut output, ObjectFormat::Sha256, &[])?;
        assert_eq!(
            output,
            concat!(
                "00a60000000000000000000000000000000000000000000000000000000000000000 capabilities^{}\0report-status delete-refs atomic ofs-delta object-format=sha256 agent=object-log\n",
                "0000"
            )
            .as_bytes()
        );

        let excessive: Vec<_> = (0..=MAX_ITEMS)
            .map(|_| AdvertisedRef {
                name: b"refs/heads/main",
                target: Some(a),
                peeled: None,
                symref_target: None,
            })
            .collect();
        limit(write_ls_refs(
            &mut Vec::new(),
            ObjectFormat::Sha1,
            &excessive,
        ));
        limit(write_receive_advertisement(
            &mut Vec::new(),
            ObjectFormat::Sha1,
            &excessive,
        ));
        Ok(())
    }

    #[test]
    fn validates_complete_advertisements_before_output() -> TestResult {
        let sha1 = id(ObjectFormat::Sha1, SHA1_A)?;
        let sha256 = id(ObjectFormat::Sha256, SHA256_A)?;
        let valid = |name: &'static [u8], target| AdvertisedRef {
            name,
            target,
            peeled: None,
            symref_target: None,
        };
        let cases = [
            vec![
                valid(b"refs/heads/b", Some(sha1)),
                valid(b"refs/heads/a", Some(sha1)),
            ],
            vec![
                valid(b"refs/heads/a", Some(sha1)),
                valid(b"refs/heads/a", Some(sha1)),
            ],
            vec![
                valid(b"refs/heads/a", Some(sha1)),
                valid(b"HEAD", Some(sha1)),
            ],
            vec![
                valid(b"refs/heads/a", Some(sha1)),
                valid(b"refs/heads/b", None),
            ],
            vec![
                valid(b"refs/heads/a", Some(sha1)),
                valid(b"refs/heads/b", Some(sha256)),
            ],
        ];
        for refs in cases {
            let mut output = Vec::new();
            protocol(write_receive_advertisement(
                &mut output,
                ObjectFormat::Sha1,
                &refs,
            ));
            assert!(output.is_empty());
        }

        let mixed = [AdvertisedRef {
            name: b"refs/tags/v1",
            target: Some(sha1),
            peeled: Some(sha256),
            symref_target: None,
        }];
        let mut output = Vec::new();
        protocol(write_ls_refs(&mut output, ObjectFormat::Sha1, &mixed));
        assert!(output.is_empty());
        Ok(())
    }

    #[test]
    fn parses_git_254_receive_fixture_and_borrows_pack() -> TestResult {
        let input = concat!(
            "009f",
            "1cca161013c9b0595b0a4637cbed4eb259f9973a ",
            "792b551e898164e75e5b7abf04612881a8f478c3 ",
            "refs/heads/main\0 report-status object-format=sha1 agent=git/2.54.0-Darwin",
            "0000PACKfixture"
        )
        .as_bytes();
        let request = parse_receive(input, ObjectFormat::Sha1)?;
        assert!(request.report_status);
        assert_eq!(request.updates.len(), 1);
        assert_eq!(request.updates[0].name, b"refs/heads/main");
        assert_eq!(
            request.updates[0].expected,
            Some(id(ObjectFormat::Sha1, SHA1_A)?)
        );
        assert_eq!(
            request.updates[0].target,
            Some(id(ObjectFormat::Sha1, SHA1_B)?)
        );
        assert_eq!(request.pack, b"PACKfixture");
        assert_eq!(
            request.pack.as_ptr(),
            input[input.len() - request.pack.len()..].as_ptr()
        );

        let command = format!(
            "{SHA256_A} {} refs/heads/main\0report-status object-format=sha256",
            "0".repeat(64)
        );
        let mut input = Vec::new();
        encode::data_to_write(command.as_bytes(), &mut input)?;
        encode::flush_to_write(&mut input)?;
        let request = parse_receive(&input, ObjectFormat::Sha256)?;
        assert!(request.report_status);
        assert_eq!(
            request.updates[0].expected,
            Some(id(ObjectFormat::Sha256, SHA256_A)?)
        );
        assert_eq!(request.updates[0].target, None);
        Ok(())
    }

    #[test]
    fn receive_defaults_sha1_accepts_line_feeds_and_enforces_pack_shape() -> TestResult {
        for line_feed in [false, true] {
            let zeros = "0".repeat(40);
            let input = receive_one(
                ObjectFormat::Sha1,
                &zeros,
                SHA1_A,
                b"",
                EMPTY_SHA1_PACK,
                line_feed,
            )?;
            let request = parse_receive(&input, ObjectFormat::Sha1)?;
            assert!(!request.report_status);
            assert_eq!(request.pack, EMPTY_SHA1_PACK);

            let zeros = "0".repeat(64);
            let input = receive_one(
                ObjectFormat::Sha256,
                &zeros,
                SHA256_A,
                b"object-format=sha256",
                EMPTY_SHA256_PACK,
                line_feed,
            )?;
            let request = parse_receive(&input, ObjectFormat::Sha256)?;
            assert!(!request.report_status);
            assert_eq!(request.pack, EMPTY_SHA256_PACK);
        }

        let zeros = "0".repeat(40);
        protocol(parse_receive(
            &receive_one(ObjectFormat::Sha1, &zeros, SHA1_A, b"", &[], false)?,
            ObjectFormat::Sha1,
        ));
        let delete = receive_one(ObjectFormat::Sha1, SHA1_A, &zeros, b"", &[], false)?;
        assert!(parse_receive(&delete, ObjectFormat::Sha1)?.pack.is_empty());
        protocol(parse_receive(
            &receive_one(
                ObjectFormat::Sha1,
                SHA1_A,
                &zeros,
                b"",
                EMPTY_SHA1_PACK,
                false,
            )?,
            ObjectFormat::Sha1,
        ));
        let missing_sha256 = receive_one(
            ObjectFormat::Sha256,
            &"0".repeat(64),
            SHA256_A,
            b"",
            EMPTY_SHA256_PACK,
            false,
        )?;
        protocol(parse_receive(&missing_sha256, ObjectFormat::Sha256));
        Ok(())
    }

    #[test]
    fn rejects_invalid_receive_capabilities_commands_and_pack() -> TestResult {
        invalid_receive(b"report-status-v2 object-format=sha1", b"PACK")?;
        invalid_receive(b"report-status report-status object-format=sha1", b"PACK")?;
        invalid_receive(b"report-status delete-refs object-format=sha1", b"PACK")?;
        invalid_receive(b"report-status object-format=sha1", b"junk")?;
        invalid_receive(b"report-status object-format=sha256", b"PACK")?;
        let mut duplicate = receive(b"report-status object-format=sha1", 1, b"PACK")?;
        let offset = duplicate.len() - 8;
        let command = format!("{SHA1_A} {SHA1_B} refs/heads/r0");
        let mut packet = Vec::new();
        encode::data_to_write(command.as_bytes(), &mut packet)?;
        duplicate.splice(offset..offset, packet);
        protocol(parse_receive(&duplicate, ObjectFormat::Sha1));
        let truncated = receive(b"report-status object-format=sha1", 1, &[])?;
        protocol(parse_receive(
            &truncated[..truncated.len() - 1],
            ObjectFormat::Sha1,
        ));
        Ok(())
    }

    #[test]
    fn enforces_receive_control_command_and_pack_limits() -> TestResult {
        let input = receive(b"report-status object-format=sha1", MAX_COMMANDS, b"PACK")?;
        assert_eq!(
            parse_receive(&input, ObjectFormat::Sha1)?.updates.len(),
            MAX_COMMANDS
        );
        limit(parse_receive(
            &receive(
                b"report-status object-format=sha1",
                MAX_COMMANDS + 1,
                b"PACK",
            )?,
            ObjectFormat::Sha1,
        ));

        let exact = sized_receive(MAX_RECEIVE_BYTES, EMPTY_SHA1_PACK)?;
        assert_eq!(exact.len() - EMPTY_SHA1_PACK.len(), MAX_RECEIVE_BYTES);
        assert!(parse_receive(&exact, ObjectFormat::Sha1).is_ok());
        let excessive = sized_receive(MAX_RECEIVE_BYTES + 1, EMPTY_SHA1_PACK)?;
        limit(parse_receive(&excessive, ObjectFormat::Sha1));

        let mut pack = vec![0; MAX_RECEIVE_PACK_BYTES];
        pack[..4].copy_from_slice(b"PACK");
        let input = receive(b"report-status object-format=sha1", 1, &pack)?;
        assert_eq!(
            parse_receive(&input, ObjectFormat::Sha1)?.pack.len(),
            MAX_RECEIVE_PACK_BYTES
        );
        pack.push(0);
        limit(parse_receive(
            &receive(b"report-status object-format=sha1", 1, &pack)?,
            ObjectFormat::Sha1,
        ));
        Ok(())
    }

    #[test]
    fn writes_exact_receive_statuses() -> TestResult {
        let update = RefUpdate::new(
            b"refs/heads/main".as_slice(),
            Some(id(ObjectFormat::Sha1, SHA1_A)?),
            Some(id(ObjectFormat::Sha1, SHA1_B)?),
        )?;
        status(
            &update,
            ReceiveStatus::Success,
            b"000eunpack ok\n0017ok refs/heads/main\n0000",
        )?;
        status(
            &update,
            ReceiveStatus::Rejected(b"rejected"),
            b"000eunpack ok\n0020ng refs/heads/main rejected\n0000",
        )?;
        status(
            &update,
            ReceiveStatus::InvalidPack(b"corrupt"),
            b"0013unpack corrupt\n001fng refs/heads/main corrupt\n0000",
        )?;
        Ok(())
    }

    #[test]
    fn enforces_output_count_limits_without_partial_output() -> TestResult {
        let sha1 = id(ObjectFormat::Sha1, SHA1_A)?;
        let names: Vec<_> = (0..=MAX_ITEMS)
            .map(|index| format!("refs/heads/{index:05}").into_bytes())
            .collect();
        let refs: Vec<_> = names
            .iter()
            .map(|name| AdvertisedRef {
                name,
                target: Some(sha1),
                peeled: None,
                symref_target: None,
            })
            .collect();
        write_ls_refs(&mut io::sink(), ObjectFormat::Sha1, &refs[..MAX_ITEMS])?;
        write_receive_advertisement(&mut io::sink(), ObjectFormat::Sha1, &refs[..MAX_ITEMS])?;
        let mut output = Vec::new();
        limit(write_receive_advertisement(
            &mut output,
            ObjectFormat::Sha1,
            &refs,
        ));
        assert!(output.is_empty());

        let acknowledgments = vec![sha1; MAX_ITEMS + 1];
        write_fetch(
            &mut io::sink(),
            ObjectFormat::Sha1,
            FetchReply::Acknowledgments(&acknowledgments[..MAX_ITEMS]),
        )?;
        limit(write_fetch(
            &mut output,
            ObjectFormat::Sha1,
            FetchReply::Acknowledgments(&acknowledgments),
        ));
        assert!(output.is_empty());

        let updates: Result<Vec<_>, _> = (0..=MAX_COMMANDS)
            .map(|index| RefUpdate::new(format!("refs/heads/r{index}"), None, Some(sha1)))
            .collect();
        let updates = updates?;
        write_receive_status(
            &mut io::sink(),
            &updates[..MAX_COMMANDS],
            ReceiveStatus::Success,
        )?;
        limit(write_receive_status(
            &mut output,
            &updates,
            ReceiveStatus::Success,
        ));
        assert!(output.is_empty());
        protocol(write_receive_status(
            &mut output,
            &[],
            ReceiveStatus::Success,
        ));
        assert!(output.is_empty());
        Ok(())
    }

    #[test]
    fn enforces_packet_and_reason_limits_before_output() -> TestResult {
        let sha1 = id(ObjectFormat::Sha1, SHA1_A)?;
        let mut name = b"refs/heads/".to_vec();
        name.resize(MAX_PACKET_PAYLOAD - 41, b'a');
        let exact = [AdvertisedRef {
            name: &name,
            target: Some(sha1),
            peeled: None,
            symref_target: None,
        }];
        write_ls_refs(&mut io::sink(), ObjectFormat::Sha1, &exact)?;
        name.push(b'a');
        let excessive = [AdvertisedRef {
            name: &name,
            target: Some(sha1),
            peeled: None,
            symref_target: None,
        }];
        let mut output = Vec::new();
        limit(write_ls_refs(&mut output, ObjectFormat::Sha1, &excessive));
        assert!(output.is_empty());

        let update = RefUpdate::new("refs/heads/main", None, Some(sha1))?;
        let mut reason = vec![b'a'; MAX_PACKET_PAYLOAD - update.name.len() - 4];
        write_receive_status(
            &mut io::sink(),
            std::slice::from_ref(&update),
            ReceiveStatus::Rejected(&reason),
        )?;
        reason.push(b'a');
        limit(write_receive_status(
            &mut output,
            std::slice::from_ref(&update),
            ReceiveStatus::Rejected(&reason),
        ));
        assert!(output.is_empty());
        for invalid in [b"".as_slice(), b"bad\nreason", b"bad\0reason"] {
            protocol(write_receive_status(
                &mut output,
                std::slice::from_ref(&update),
                ReceiveStatus::Rejected(invalid),
            ));
            assert!(output.is_empty());
        }
        for status in [
            ReceiveStatus::Rejected(b"ok"),
            ReceiveStatus::InvalidPack(b"ok"),
        ] {
            protocol(write_receive_status(
                &mut output,
                std::slice::from_ref(&update),
                status,
            ));
            assert!(output.is_empty());
        }
        Ok(())
    }

    fn id(format: ObjectFormat, value: &str) -> Result<ObjectId, crate::Error> {
        ObjectId::parse(format, value)
    }

    fn upload(command: &[u8], format: ObjectFormat, args: &[&[u8]]) -> io::Result<Vec<u8>> {
        upload_with(command, Some(format), args, true)
    }

    fn upload_with(
        command: &[u8],
        format: Option<ObjectFormat>,
        args: &[&[u8]],
        line_feed: bool,
    ) -> io::Result<Vec<u8>> {
        let mut input = Vec::new();
        let mut header = b"command=".to_vec();
        header.extend_from_slice(command);
        encode_line(&header, line_feed, &mut input)?;
        if let Some(format) = format {
            encode_line(format_capability(format), line_feed, &mut input)?;
        }
        encode::delim_to_write(&mut input)?;
        for argument in args {
            encode_line(argument, line_feed, &mut input)?;
        }
        encode::flush_to_write(&mut input)?;
        Ok(input)
    }

    fn sized_upload(bytes: usize) -> io::Result<Vec<u8>> {
        let mut input = Vec::new();
        encode::data_to_write(b"command=ls-refs", &mut input)?;
        encode::delim_to_write(&mut input)?;
        while input.len() + 4 < bytes {
            let remaining = bytes - input.len() - 4;
            let packet_bytes = sized_packet(remaining, b"ref-prefix ".len() + 4);
            let mut line = b"ref-prefix ".to_vec();
            line.resize(packet_bytes - 4, b'a');
            encode::data_to_write(&line, &mut input)?;
        }
        encode::flush_to_write(&mut input)?;
        Ok(input)
    }

    fn receive(capabilities: &[u8], count: usize, pack: &[u8]) -> io::Result<Vec<u8>> {
        receive_with_name(capabilities, count, pack, b"refs/heads/r")
    }

    fn receive_one(
        format: ObjectFormat,
        old: &str,
        new: &str,
        capabilities: &[u8],
        pack: &[u8],
        line_feed: bool,
    ) -> io::Result<Vec<u8>> {
        if old.len() != format.digest_len() * 2 || new.len() != format.digest_len() * 2 {
            return Err(io::Error::other("test object ID length is wrong"));
        }
        let mut command = format!("{old} {new} refs/heads/main").into_bytes();
        command.push(0);
        command.extend_from_slice(capabilities);
        let mut input = Vec::new();
        encode_line(&command, line_feed, &mut input)?;
        encode::flush_to_write(&mut input)?;
        input.extend_from_slice(pack);
        Ok(input)
    }

    fn sized_receive(control_bytes: usize, pack: &[u8]) -> io::Result<Vec<u8>> {
        let mut input = Vec::new();
        let mut index = 0;
        while input.len() + 4 < control_bytes {
            let remaining = control_bytes - input.len() - 4;
            let packet_bytes = sized_packet(remaining, 128);
            let mut command = format!("{SHA1_A} {SHA1_B} refs/heads/r{index}/").into_bytes();
            let capability = usize::from(index == 0);
            command.resize(packet_bytes - 4 - capability, b'a');
            if index == 0 {
                command.push(0);
            }
            encode::data_to_write(&command, &mut input)?;
            index += 1;
        }
        encode::flush_to_write(&mut input)?;
        input.extend_from_slice(pack);
        Ok(input)
    }

    fn sized_packet(remaining: usize, minimum: usize) -> usize {
        if remaining <= 65_520 {
            remaining
        } else if remaining - 65_520 < minimum {
            remaining - minimum
        } else {
            65_520
        }
    }

    fn encode_line(line: &[u8], line_feed: bool, output: &mut Vec<u8>) -> io::Result<()> {
        if line_feed {
            encode::text_to_write(line, output).map(|_| ())
        } else {
            encode::data_to_write(line, output).map(|_| ())
        }
    }

    fn sideband_chunks(bytes: usize) -> TestResult<usize> {
        let pack = vec![0; bytes];
        let mut output = Vec::new();
        write_fetch(&mut output, ObjectFormat::Sha1, FetchReply::Pack(&pack))?;
        let mut packets = output.as_slice();
        packet(&mut packets)?;
        let mut chunks = 0;
        while !matches!(packet(&mut packets)?, PacketLineRef::Flush) {
            chunks += 1;
        }
        Ok(chunks)
    }

    fn invalid_upload(command: &[u8], args: &[&[u8]]) -> TestResult {
        protocol(parse_upload(
            &upload(command, ObjectFormat::Sha1, args)?,
            ObjectFormat::Sha1,
        ));
        Ok(())
    }

    fn invalid_receive(capabilities: &[u8], pack: &[u8]) -> TestResult {
        protocol(parse_receive(
            &receive(capabilities, 1, pack)?,
            ObjectFormat::Sha1,
        ));
        Ok(())
    }

    fn status(update: &RefUpdate, status: ReceiveStatus<'_>, expected: &[u8]) -> TestResult {
        let mut output = Vec::new();
        write_receive_status(&mut output, std::slice::from_ref(update), status)?;
        assert_eq!(output, expected);
        Ok(())
    }

    fn receive_with_name(
        capabilities: &[u8],
        count: usize,
        pack: &[u8],
        name: &[u8],
    ) -> io::Result<Vec<u8>> {
        let mut input = Vec::new();
        for index in 0..count {
            let mut command = format!("{SHA1_A} {SHA1_B} ").into_bytes();
            command.extend_from_slice(name);
            command.extend_from_slice(index.to_string().as_bytes());
            if index == 0 {
                command.push(0);
                command.extend_from_slice(capabilities);
            }
            encode::data_to_write(&command, &mut input)?;
        }
        encode::flush_to_write(&mut input)?;
        input.extend_from_slice(pack);
        Ok(input)
    }

    fn data(line: PacketLineRef<'_>) -> Result<&[u8], &'static str> {
        if let PacketLineRef::Data(data) = line {
            Ok(data)
        } else {
            Err("expected data packet")
        }
    }

    fn protocol<T>(result: Result<T, Error>) {
        assert!(matches!(result.err(), Some(Error::Protocol(_))));
    }

    fn limit<T>(result: Result<T, Error>) {
        assert!(matches!(result.err(), Some(Error::Limit(_))));
    }
}
