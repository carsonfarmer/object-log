//! Opt-in same-process memory-store acceptance, not remote-store performance.
#[path = "shared_performance/fixture.rs"]
mod fixture;
#[path = "shared_performance/timed_store.rs"]
mod timed_store;

use bytes::Bytes;
use fixture::{Fixture, git, hash_name};
use object_log::{Log, LogId, Options, Resolution, TransactionId, ValidatedBackend};
use object_log_git::{ObjectFormat, Repository};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Write,
    path::Path,
    sync::Arc,
    time::Instant,
};
use timed_store::{TimedStore, serial_depth};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
const MIB: usize = 1024 * 1024;

fn packet(output: &mut Vec<u8>, line: &[u8]) -> Result {
    write!(output, "{:04x}", line.len() + 4)?;
    output.extend_from_slice(line);
    Ok(())
}
fn request(fixture: &Fixture) -> Result<Bytes> {
    let mut out = Vec::new();
    packet(&mut out, b"command=fetch\n")?;
    packet(
        &mut out,
        format!("object-format={}\n", hash_name(fixture.format)).as_bytes(),
    )?;
    out.extend_from_slice(b"0001");
    packet(&mut out, format!("want {}\n", fixture.tip).as_bytes())?;
    if let Some(have) = &fixture.have {
        packet(&mut out, format!("have {have}\n").as_bytes())?;
    }
    packet(&mut out, b"done\n")?;
    out.extend_from_slice(b"0000");
    Ok(out.into())
}
fn receive(fixture: &Fixture, target: &str, expected: Option<&str>, pack: &[u8]) -> Result<Bytes> {
    let mut out = Vec::new();
    let zero = "0".repeat(fixture.tip.len());
    packet(
        &mut out,
        format!(
            "{} {target} refs/heads/main\0report-status object-format={}\n",
            expected.unwrap_or(&zero),
            hash_name(fixture.format)
        )
        .as_bytes(),
    )?;
    out.extend_from_slice(b"0000");
    out.extend_from_slice(pack);
    Ok(out.into())
}
fn unpack(response: &[u8]) -> Result<Vec<u8>> {
    let mut remaining = response;
    let mut raw = Vec::new();
    let mut pack_section = false;
    while !remaining.is_empty() {
        let header = remaining.get(..4).ok_or("truncated packet")?;
        let length = usize::from_str_radix(std::str::from_utf8(header)?, 16)?;
        remaining = &remaining[4..];
        if length <= 2 {
            continue;
        }
        let payload = remaining.get(..length - 4).ok_or("truncated payload")?;
        remaining = &remaining[length - 4..];
        if payload == b"packfile\n" {
            pack_section = true;
            continue;
        }
        if pack_section {
            assert_eq!(payload.first(), Some(&1));
            raw.extend_from_slice(&payload[1..]);
        }
    }
    assert!(raw.starts_with(b"PACK"));
    Ok(raw)
}
async fn new_log() -> Result<(Log, TimedStore)> {
    let store = TimedStore::new();
    let backend = ValidatedBackend::new(
        Arc::new(store.clone()),
        object_store::path::Path::from("shared-performance"),
    )
    .await?;
    let log = Log::open(&backend, &LogId::new("repository")?, Options::default()).await?;
    Ok((log, store))
}
async fn publish(
    log: &Log,
    fixture: &Fixture,
    target: &str,
    expected: Option<&str>,
    pack: &[u8],
) -> Result<Bytes> {
    let prepared = Repository::open(log, fixture.format)
        .await?
        .prepare_receive(
            TransactionId::new(),
            receive(fixture, target, expected, pack)?,
        )
        .await?;
    let (resolution, reply) = prepared.publish_receive().await?;
    assert!(matches!(resolution, Resolution::Committed(_)));
    assert!(String::from_utf8_lossy(&reply).contains("unpack ok"));
    Ok(reply)
}
fn init_receiver(path: &Path, fixture: &Fixture) -> Result {
    git(
        path,
        &[
            "init",
            "-q",
            "--bare",
            &format!("--object-format={}", hash_name(fixture.format)),
        ],
        &[],
    )?;
    git(path, &["config", "pack.threads", "1"], &[])?;
    Ok(())
}
fn verify_pack(fixture: &Fixture, pack: &[u8], incremental: bool) -> Result {
    let standalone = tempfile::tempdir()?;
    init_receiver(standalone.path(), fixture)?;
    // Index without graph checking in an EMPTY repository: any external delta base fails.
    git(standalone.path(), &["index-pack", "--stdin"], pack)?;
    let pack_dir = standalone.path().join("objects/pack");
    let index = fs::read_dir(pack_dir)?
        .filter_map(std::result::Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|ext| ext == "idx"))
        .ok_or("missing pack index")?;
    let listing = git(
        standalone.path(),
        &["verify-pack", "-v", index.to_str().ok_or("index path")?],
        &[],
    )?;
    let actual: BTreeSet<_> = std::str::from_utf8(&listing)?
        .lines()
        .filter_map(|line| {
            let oid = line.split_whitespace().next()?;
            (oid.len() == fixture.tip.len() && oid.bytes().all(|b| b.is_ascii_hexdigit()))
                .then(|| oid.to_owned())
        })
        .collect();
    let revs = if incremental {
        fixture.revisions()
    } else {
        format!("{}\n", fixture.tip).into_bytes()
    };
    let expected = git(
        &fixture.source,
        &["rev-list", "--objects", "--stdin"],
        &revs,
    )?;
    let expected: BTreeSet<_> = std::str::from_utf8(&expected)?
        .lines()
        .filter_map(|line| line.split_whitespace().next().map(str::to_owned))
        .collect();
    assert_eq!(actual, expected, "fetch OID selection differs from Git");
    let receiver = tempfile::tempdir()?;
    init_receiver(receiver.path(), fixture)?;
    if incremental {
        git(
            receiver.path(),
            &["index-pack", "--stdin", "--strict"],
            &fixture.seed_pack,
        )?;
    }
    let mut args = vec!["index-pack", "--stdin", "--strict"];
    if !incremental {
        args.push("--check-self-contained-and-connected");
    }
    git(receiver.path(), &args, pack)?;
    git(
        receiver.path(),
        &["update-ref", "refs/heads/main", &fixture.tip],
        &[],
    )?;
    git(receiver.path(), &["fsck", "--strict", "--no-progress"], &[])?;
    Ok(())
}
struct Sample {
    nanos: u128,
    raw: usize,
    framed: usize,
    calls: u64,
    transfer: u64,
    intervals: Vec<(u128, u128)>,
}
impl Sample {
    fn git(nanos: u128, bytes: usize) -> Self {
        Self {
            nanos,
            raw: bytes,
            framed: 0,
            calls: 0,
            transfer: 0,
            intervals: vec![],
        }
    }
    fn record(
        &self,
        file: &mut File,
        label: &str,
        format: ObjectFormat,
        direction: &str,
        engine: &str,
        sample: usize,
    ) -> Result {
        let first = (engine == "git") == (sample.is_multiple_of(2));
        writeln!(
            file,
            "{{\"kind\":\"sample\",\"fixture\":\"{label}\",\"hash\":\"{}\",\"direction\":\"{direction}\",\"engine\":\"{engine}\",\"sample\":{sample},\"first\":{first},\"elapsed_ns\":{},\"raw_bytes\":{},\"framed_bytes\":{},\"logical_calls\":{},\"transferred_bytes\":{},\"serial_depth\":{},\"intervals_ns\":{:?}}}",
            hash_name(format),
            self.nanos,
            self.raw,
            self.framed,
            self.calls,
            self.transfer,
            serial_depth(&self.intervals),
            self.intervals
                .iter()
                .map(|&(a, b)| [a, b])
                .collect::<Vec<_>>()
        )?;
        file.flush()?;
        Ok(())
    }
}
async fn candidate(fixture: &Fixture, push: bool, thin: bool) -> Result<Sample> {
    let (log, store) = new_log().await?;
    if !push || thin {
        let target = if thin {
            fixture.have.as_deref().ok_or("thin base")?
        } else {
            &fixture.tip
        };
        let pack = if thin {
            fixture.seed_pack.clone()
        } else {
            git(
                &fixture.source,
                &["pack-objects", "--stdout", "--revs"],
                format!("{target}\n").as_bytes(),
            )?
        };
        drop(publish(&log, fixture, target, None, &pack).await?);
    }
    let incoming = if push {
        fixture.oracle(thin)?
    } else {
        Vec::new()
    };
    store.reset();
    let start = Instant::now();
    let response = if push {
        publish(
            &log,
            fixture,
            &fixture.tip,
            if thin { fixture.have.as_deref() } else { None },
            &incoming,
        )
        .await?
    } else {
        Repository::open(&log, fixture.format)
            .await?
            .upload_pack(request(fixture)?)
            .await?
    };
    let nanos = start.elapsed().as_nanos();
    let metrics = store.faults.metrics();
    let intervals = store.intervals();
    assert_eq!(
        intervals.len() as u64,
        metrics.total_requests(),
        "untimed store operation"
    );
    assert!(metrics.total_requests() <= 512);
    assert!(metrics.downloaded_bytes() + metrics.uploaded_bytes() <= 96 * MIB as u64);
    let framed = response.len();
    let raw = if push {
        drop(response);
        let response = Repository::open(&log, fixture.format)
            .await?
            .upload_pack(request(fixture)?)
            .await?;
        let raw = unpack(&response)?;
        verify_pack(fixture, &raw, thin)?;
        incoming.len()
    } else {
        let raw = unpack(&response)?;
        assert!(raw.len() <= 9_437_184);
        assert!(framed <= 9_437_926);
        verify_pack(fixture, &raw, fixture.have.is_some())?;
        raw.len()
    };
    Ok(Sample {
        nanos,
        raw,
        framed,
        calls: metrics.total_requests(),
        transfer: metrics.downloaded_bytes() + metrics.uploaded_bytes(),
        intervals,
    })
}
fn oracle(fixture: &Fixture, push: bool, thin: bool) -> Result<Sample> {
    if !push {
        let start = Instant::now();
        let pack = fixture.oracle(false)?;
        return Ok(Sample::git(start.elapsed().as_nanos(), pack.len()));
    }
    let receiver = tempfile::tempdir()?;
    init_receiver(receiver.path(), fixture)?;
    if thin {
        git(
            receiver.path(),
            &["index-pack", "--stdin", "--strict"],
            &fixture.seed_pack,
        )?;
    }
    let pack = fixture.oracle(thin)?;
    let start = Instant::now();
    let mut args = vec!["index-pack", "--stdin", "--strict"];
    if thin {
        args.push("--fix-thin");
    }
    git(receiver.path(), &args, &pack)?;
    git(
        receiver.path(),
        &["update-ref", "refs/heads/main", &fixture.tip],
        &[],
    )?;
    Ok(Sample::git(start.elapsed().as_nanos(), pack.len()))
}
fn percentile(samples: &mut [u128], numerator: usize) -> u128 {
    samples.sort_unstable();
    samples[(samples.len() * numerator).div_ceil(100) - 1]
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Keep paired order and escalation visible together.
#[ignore = "paired Git/shared performance acceptance; writes raw JSONL"]
async fn shared_git_performance_acceptance() -> Result {
    let output = std::env::var("OBJECT_LOG_GIT_PERFORMANCE_OUTPUT").unwrap_or_else(|_| {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../target/git-shared-performance.jsonl"
        )
        .into()
    });
    if let Some(parent) = Path::new(&output).parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(output)?;
    let version = String::from_utf8(git(Path::new("."), &["--version"], &[])?)?;
    assert!(
        version.starts_with("git version 2.54."),
        "requires pinned Git 2.54: {version}"
    );
    let revision = String::from_utf8(git(Path::new("."), &["rev-parse", "HEAD"], &[])?)?;
    writeln!(
        file,
        "{{\"kind\":\"conditions\",\"git\":\"{}\",\"revision\":\"{}\",\"profile\":\"{}\",\"store\":\"memory\",\"warmups\":1,\"initial_pairs\":10,\"config\":\"no system/global config; pack.threads=1; fixed identity/date\",\"serial_depth_definition\":\"longest nonoverlapping request interval chain; GET includes body; epoch process-monotonic\",\"latency_scope\":\"shared includes log open, validation, memory storage; Git subprocess pack-objects or index-pack+update-ref; transport excluded\"}}",
        version.trim(),
        revision.trim(),
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        }
    )?;
    for format in [ObjectFormat::Sha1, ObjectFormat::Sha256] {
        for label in ["4kib", "8mib", "history", "incremental", "thin"] {
            let fixture = Fixture::new(format, label)?;
            let count = git(&fixture.source, &["rev-list", "--count", &fixture.tip], &[])?;
            let count = std::str::from_utf8(&count)?.trim().parse::<usize>()?;
            assert_eq!(
                count,
                match label {
                    "history" => 384,
                    "incremental" | "thin" => 385,
                    _ => 1,
                }
            );
            if label == "thin" {
                let empty = tempfile::tempdir()?;
                init_receiver(empty.path(), &fixture)?;
                let error = git(
                    empty.path(),
                    &["index-pack", "--stdin"],
                    &fixture.oracle(true)?,
                )
                .err()
                .ok_or("thin fixture has no external base")?;
                assert!(error.to_string().contains("unresolved delta"), "{error}");
            }
            writeln!(
                file,
                "{{\"kind\":\"fixture\",\"fixture\":\"{label}\",\"hash\":\"{}\",\"tip\":\"{}\",\"have\":\"{}\",\"commits\":{count},\"seed_pack_bytes\":{}}}",
                hash_name(format),
                fixture.tip,
                fixture.have.as_deref().unwrap_or(""),
                fixture.seed_pack.len()
            )?;
            let directions: &[bool] = if matches!(label, "4kib" | "8mib") {
                &[true, false]
            } else if label == "thin" {
                &[true]
            } else {
                &[false]
            };
            for &push in directions {
                let thin = label == "thin";
                let direction = if push { "push" } else { "fetch" };
                let mut shared_times = vec![];
                let mut git_times = vec![];
                let mut pairs = 10;
                let mut sample = 0_usize;
                loop {
                    let (shared, baseline) = if sample.is_multiple_of(2) {
                        let baseline = oracle(&fixture, push, thin)?;
                        (candidate(&fixture, push, thin).await?, baseline)
                    } else {
                        let shared = candidate(&fixture, push, thin).await?;
                        (shared, oracle(&fixture, push, thin)?)
                    };
                    if !push && matches!(label, "8mib" | "incremental") {
                        assert!(
                            shared.raw * 100 <= baseline.raw * 110,
                            "{label} {} pack ratio: {} / {}",
                            hash_name(format),
                            shared.raw,
                            baseline.raw
                        );
                    }
                    shared.record(&mut file, label, format, direction, "shared", sample)?;
                    baseline.record(&mut file, label, format, direction, "git", sample)?;
                    if sample > 0 {
                        shared_times.push(shared.nanos);
                        git_times.push(baseline.nanos);
                    }
                    if sample == pairs {
                        let sp50 = percentile(&mut shared_times, 50);
                        let sp95 = percentile(&mut shared_times, 95);
                        let gp50 = percentile(&mut git_times, 50);
                        let gp95 = percentile(&mut git_times, 95);
                        let review = sp50 * 100 > gp50 * 125 || sp95 * 100 > gp95 * 125;
                        writeln!(
                            file,
                            "{{\"kind\":\"summary\",\"fixture\":\"{label}\",\"hash\":\"{}\",\"direction\":\"{direction}\",\"pairs\":{pairs},\"shared_p50_ns\":{sp50},\"shared_p95_ns\":{sp95},\"git_p50_ns\":{gp50},\"git_p95_ns\":{gp95},\"owner_review_before_oracle_removal\":{review}}}",
                            hash_name(format)
                        )?;
                        if review && pairs == 10 {
                            pairs = 30;
                        } else {
                            break;
                        }
                    }
                    sample += 1;
                }
            }
        }
    }
    Ok(())
}
