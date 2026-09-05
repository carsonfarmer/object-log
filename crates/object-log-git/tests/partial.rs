//! Both-hash installed Git upload protocol oracle.
#[path = "support/upload.rs"]
mod upload;
use object_log::{Log, LogId, Options, Resolution, TransactionId, ValidatedBackend};
use object_log_git::{ObjectFormat, Repository};
use object_store::{memory::InMemory, path::Path as StorePath};
use std::sync::Arc;
use upload::*;

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "both-hash filter oracle includes threshold, tag, shallow and lazy-want cases"
)]
async fn filtered_pack_selection_and_explicit_lazy_wants_match_git() -> TestResult {
    for (format, name) in [
        (ObjectFormat::Sha1, "sha1"),
        (ObjectFormat::Sha256, "sha256"),
    ] {
        let source = tempfile::tempdir()?;
        let path = source.path();
        git(
            path,
            &[
                "init",
                "--quiet",
                "-b",
                "main",
                &format!("--object-format={name}"),
            ],
            &[],
        )?;
        git(path, &["config", "uploadpack.allowFilter", "true"], &[])?;
        git(
            path,
            &["config", "uploadpack.allowReachableSHA1InWant", "true"],
            &[],
        )?;
        for (filename, contents) in [
            ("empty", vec![]),
            ("small", b"tiny".to_vec()),
            ("boundary", vec![b'b'; 1024]),
            ("large", vec![b'a'; 65536]),
        ] {
            std::fs::write(path.join(filename), contents)?;
        }
        git(path, &["add", "."], &[])?;
        git(path, &["commit", "--quiet", "-m", "base"], &[])?;
        let old = text(path, &["rev-parse", "HEAD"])?;
        std::fs::write(
            path.join("large"),
            [vec![b'a'; 65440], vec![b'b'; 96]].concat(),
        )?;
        git(path, &["commit", "--quiet", "-am", "update"], &[])?;
        let tip = text(path, &["rev-parse", "HEAD"])?;
        let blob = text(path, &["rev-parse", "HEAD:large"])?;
        let tree = text(path, &["rev-parse", "HEAD^{tree}"])?;
        git(path, &["tag", "-a", "commit-tag", "-m", "commit"], &[])?;
        git(path, &["tag", "-a", "blob-tag", "-m", "blob", &blob], &[])?;
        git(
            path,
            &["tag", "-a", "outer-tag", "-m", "outer", "blob-tag"],
            &[],
        )?;
        git(path, &["tag", "-a", "tree-tag", "-m", "tree", &tree], &[])?;
        let tree_tag = text(path, &["rev-parse", "tree-tag"])?;
        let blob_tag = text(path, &["rev-parse", "blob-tag"])?;
        let outer_tag = text(path, &["rev-parse", "outer-tag"])?;
        let unreachable = String::from_utf8(git(
            path,
            &["hash-object", "-w", "--stdin"],
            b"unreachable",
        )?)?
        .trim()
        .to_owned();
        let mut objects = git(path, &["rev-list", "--objects", "--all"], &[])?;
        objects.extend_from_slice(format!("{unreachable}\n").as_bytes());
        let pack = git(path, &["pack-objects", "--stdout"], &objects)?;
        let store =
            ValidatedBackend::new(Arc::new(InMemory::new()), StorePath::from("partial")).await?;
        let log = Log::open(&store, &LogId::new("repository")?, Options::default()).await?;
        let listing = text(path, &["for-each-ref", "--format=%(refname) %(objectname)"])?;
        let mut receive = Vec::new();
        for (index, line) in listing.lines().enumerate() {
            let (reference, id) = line.split_once(' ').ok_or("ref")?;
            let caps = if index == 0 {
                format!("\0report-status object-format={name}")
            } else {
                String::new()
            };
            packet(
                &mut receive,
                &format!("{} {id} {reference}{caps}", "0".repeat(tip.len())),
            )?;
        }
        receive.extend_from_slice(b"0000");
        receive.extend(pack);
        let prepared = Repository::open(&log, format)
            .await?
            .prepare_receive(TransactionId::new(), receive.into())
            .await?;
        assert!(matches!(
            prepared.publish_receive().await?.0,
            Resolution::Committed(_)
        ));
        let mut cases = Vec::new();
        for filter in [
            "blob:none",
            "blob:limit=0",
            "blob:limit=4",
            "blob:limit=1024",
            "blob:limit=1k",
            "blob:limit=4k",
            "blob:limit=1m",
        ] {
            cases.push(vec![format!("want {tip}"), format!("filter {filter}")]);
            cases.push(vec![
                format!("want {tip}"),
                format!("filter {filter}"),
                "include-tag".into(),
            ]);
            cases.push(vec![
                format!("want {blob}"),
                format!("filter {filter}"),
                format!("have {tip}"),
            ]);
        }
        for want in [&blob, &blob_tag, &outer_tag, &tree, &tree_tag] {
            for have in [&tip, &blob, &blob_tag, &outer_tag, &tree, &tree_tag] {
                for filter in [None, Some("blob:none"), Some("blob:limit=1024")] {
                    let mut args = vec![format!("want {want}"), format!("have {have}")];
                    if let Some(filter) = filter {
                        args.push(format!("filter {filter}"));
                    }
                    cases.push(args);
                }
            }
        }
        cases.extend([
            vec![
                format!("want {tip}"),
                "filter blob:none".into(),
                "deepen 1".into(),
                "include-tag".into(),
            ],
            vec![
                format!("want {tip}"),
                "filter blob:limit=1024".into(),
                format!("shallow {old}"),
                format!("have {old}"),
            ],
            vec![
                format!("want {tip}"),
                "filter blob:none".into(),
                format!("shallow {tip}"),
                format!("have {tip}"),
                "deepen 1".into(),
                "deepen-relative".into(),
            ],
            vec![format!("want {blob_tag}"), "filter blob:none".into()],
            vec![
                format!("want {outer_tag}"),
                "filter blob:none".into(),
                "include-tag".into(),
            ],
            vec![
                format!("want {blob}"),
                "filter blob:none".into(),
                "include-tag".into(),
            ],
            vec![format!("want {blob}"), format!("have {tip}")],
            vec![
                format!("want {blob}"),
                format!("have {blob}"),
                "filter blob:none".into(),
            ],
        ]);
        let base = object_log_git::PackfileUris::new("https://example.invalid/repo")?;
        let mut uri_count = 0;
        for mut args in cases {
            args.push("done".into());
            let input = request(name, &args)?;
            let expected = reply(
                path,
                &git(path, &["upload-pack", "--stateless-rpc", ".git"], &input)?,
            )?;
            let actual = reply(
                path,
                &Repository::open(&log, format)
                    .await?
                    .upload_pack(input.into())
                    .await?,
            )?;
            assert_eq!(actual, expected, "{name}: {args:?}");
            args.insert(0, "packfile-uris https".into());
            let response = Repository::open(&log, format)
                .await?
                .upload_pack_with_uris(request(name, &args)?.into(), &base)
                .await?;
            let mut combined = reply(path, &response)?;
            let locations = uri_locations(&response)?;
            assert!(locations.len() <= 8);
            drop(response);
            for (checksum, uri) in locations {
                let fields = uri.split('/').collect::<Vec<_>>();
                let id = object_log_git::ObjectId::parse(format, fields[fields.len() - 2])?;
                let checksum = object_log_git::ObjectId::parse(format, &checksum)?;
                let pack = Repository::open(&log, format)
                    .await?
                    .fetch_uri_pack(id, checksum)
                    .await?;
                let ids = reply(path, &frame_pack(&pack))?.ids;
                assert_eq!(ids.len(), 1);
                assert!(ids.contains(&id.to_string()));
                for id in ids {
                    assert!(combined.ids.insert(id));
                }
                uri_count += 1;
            }
            assert_eq!(combined, expected, "URI {name}: {args:?}");
        }
        assert!(uri_count > 0);
        // URI access must reject stored-but-unreachable objects and reachable non-blobs.
        let checksum = object_log_git::ObjectId::parse(format, &blob)?;
        for rejected in [&unreachable, &tip, &tree] {
            assert!(matches!(
                Repository::open(&log, format)
                    .await?
                    .fetch_uri_pack(object_log_git::ObjectId::parse(format, rejected)?, checksum)
                    .await,
                Err(object_log_git::Error::InvalidReference)
            ));
        }
        let args = vec![
            format!("want {tip}"),
            "packfile-uris http".into(),
            "done".into(),
        ];
        let unsupported = request(name, &args)?;
        assert!(
            Repository::open(&log, format)
                .await?
                .upload_pack(unsupported.clone().into())
                .await
                .is_err()
        );
        let fallback = Repository::open(&log, format)
            .await?
            .upload_pack_with_uris(unsupported.into(), &base)
            .await?;
        assert!(uri_locations(&fallback)?.is_empty());
        let fallback_ids = reply(path, &fallback)?;
        drop(fallback);
        let ordinary = Repository::open(&log, format)
            .await?
            .upload_pack(request(name, &[format!("want {tip}"), "done".into()])?.into())
            .await?;
        assert_eq!(fallback_ids, reply(path, &ordinary)?);
        drop(ordinary);

        for filter in ["blob:none", "blob:limit=1024"] {
            let input = request(
                name,
                &[
                    format!("want {unreachable}"),
                    format!("filter {filter}"),
                    "done".into(),
                ],
            )?;
            assert!(matches!(
                Repository::open(&log, format)
                    .await?
                    .upload_pack(input.into())
                    .await,
                Err(object_log_git::Error::InvalidReference)
            ));
        }
    }
    Ok(())
}
