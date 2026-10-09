# Final Artifact Gate Re-verification

**Date:** 2026-10-05 UTC  
**Workstream:** Reproducible Local Runtime Packaging & Launch Verification — unnumbered  
**Scope:** evidence-based final artifact prerequisite re-verification only

## 1. Governing artifact

The controlled artifact source is the explicitly authorized, previously verified sealed archive:

| Property | Required and currently measured value |
|---|---|
| Archive | `minio_frozen_linux_amd64_sealed_20261005.tar` |
| Location | Project root: `<repository-root>/` |
| Archive SHA256 | `58800abbac143c831a53c00fadbe9fe0af30914c83835fd52bc7a7c487fe355a` |
| Archive size | **80105984 bytes** |
| Supported payload platform | **linux/amd64** |

| Frozen image | Governing OCI index digest |
|---|---|
| `minio/minio:RELEASE.2024-11-07T00-52-20Z` | `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` |
| `minio/mc:RELEASE.2024-11-05T11-29-45Z` | `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` |

Both images are required independently. Archive SHA256, OCI index digest, Docker image ID, config digest, compressed layer digest, uncompressed DiffID and executable SHA256 remain distinct identity domains. The loaded Docker image IDs equal their respective index digests under the tested containerd-backed engine; the config digests are different.

## 2. Path B authorization

The owner explicitly selected **PATH B — EXPLICIT SEALED-ARCHIVE AUTHORIZATION**, authorizing creation and independent verification of an archive from these two exact cached images. That bounded phase completed with PASS.

The current instruction additionally authorizes this final artifact-gate re-verification against the fixed archive checksum, size and completed evidence. It does not authorize packaging implementation or Phase C.

The earlier owner-decision report's BLOCKED status described the then-unprovided authorization. The explicit owner decision and verified archive now satisfy that missing condition; the historical report is preserved rather than rewritten.

## 3. Evidence reviewed

Primary authority: **`minio_sealed_archive_verification_report.md`**, read in full, especially:

- §§2–4: owner authorization, exact source references, original OCI index descriptors, platform and pre-export identity.
- §§5–7: successful exact-image export, fixed archive checksum/size, original index bytes and complete Linux/amd64 manifest/config/layer chain.
- §8: a separate Docker29.6.2 daemon initially containing **zero images and zero containers**, with its own new volume, network disabled, and no source socket, image-store or retained-data mount; successful checksum-verified archive load.
- §9: exact post-load image IDs, OCI indexes and RepoDigests, config/RootFS equality, and independent reads/hashes of **all26 archived content blobs**.
- §10: exact release/commit probes and executable hashes matching the previously established official binaries.
- §§11–13: no rebuild/substitution, source images and retained OIP state unchanged, disposable-resource cleanup, and explicit scope limitations.

Current SHA256 of that unchanged evidence report: `0f3ef61f7d9c3a335fd747c47628236004fbe35ba9eb3058a22130bb04cb881f`; size **30338 bytes**.

Prior prerequisite authorities reviewed:

- `artifact_availability_gate_report.md` §§15–17: exact MinIO acquisition was the required-artifact blocker; explicit source/archive adoption, independent acquisition verification and separate implementation authorization were required.
- `artifact_authorization_decision_gate_report.md` §7: Path B requires both exact images, documented provenance, archive checksum, independent clean load, post-load identities, exact version probes and no rebuild/substitution.
- `minio_artifact_unblock_report.md`: preceding exact-release/binary provenance and image-acquisition findings, as carried forward by the sealed-archive report.
- `post_m9_roadmap_gate_report.md` §§11–12: the bounded local packaging proposal and frozen scope, as cited by the governing artifact reports.

This gate freshly streamed the existing archive through SHA256 and checked its byte size. Both matched the authorized values. It also fingerprinted existing project files and performed read-only Docker image/container-state inspection. The previous load/probes are reviewed evidence, **not repeated executions in this gate**. No registry inquiry, image pull/save/load, new Docker resource, application invocation or acceptance test was performed.

## 4. Acceptance criteria

All evidence references in the table below refer to `minio_sealed_archive_verification_report.md`.

| # | Required prerequisite | Evidence supporting the condition | Result |
|---:|---|---|---|
| 1 | Exact frozen server available through sealed archive | §§3–5,9: exact server reference/index, successful export/load and post-load equality | PASS |
| 2 | Exact frozen mc available through sealed archive | §§3–5,9: independent exact client reference/index, successful export/load and equality | PASS |
| 3 | Archive checksum fixed and recorded | §§5–6: fixed filename,80105984 bytes, required SHA256 before/after transfer and cleanup; current checksum/size also matched | PASS |
| 4 | Independent load into initially empty Docker daemon | §8: Images=0/Containers=0 before load, separate daemon/root/volume and successful image-load output | PASS |
| 5 | Original source cache unnecessary during load | §8: only copied tar supplied; no source Docker socket, image-store or retained-data mounts; network none | PASS |
| 6 | Governing OCI index digests preserved | §§7,9: original index bytes hashed, loaded index descriptors equal governing digests, loaded content bytes independently hashed | PASS |
| 7 | Exact RepoDigests preserved | §9: repository-qualified server/client RepoDigests preserved after independent load | PASS |
| 8 | Linux/amd64 manifests/configs/layers verified | §§4,7,9: selected manifests/configs,9 server/6 client ordered layers, compressed hashes/DiffIDs and loaded content equality | PASS |
| 9 | All archived content blobs independently verified | §§7,9: all26 blobs hashed in tar and independently read from loaded content store with matching hashes/sizes | PASS |
| 10 | Exact release/version probes passed | §10: both frozen release timestamps and expected commits, Go1.23.3/linux-amd64 | PASS |
| 11 | Executable hashes match official-release evidence | §10: server`c27873d7…23fd80`, client`a2b886f9…427bd1`; full values in §5 below | PASS |
| 12 | No rebuild/retag/squash/upgrade/substitution | §§4–5,9,11: direct image save/load, unchanged source inventory; no replacement or identity synthesis | PASS |
| 13 | Retained OIP resources untouched | §11: exact source-image/container-state comparison, stopped services, no retained-volume/network/data operations; current stopped state confirmed | PASS |
| 14 | Archive serves as controlled local Linux/amd64 artifact source | Explicit Path B adoption plus all preceding verified conditions; transfer/checksum/load procedure in §8 | PASS |

No criterion is satisfied solely by matching executable hashes, cached-image presence or a tag name. The controlling evidence is authorized source provenance plus archive-only independent load and complete required-platform image-content verification.

## 5. Verification result

**All14 artifact prerequisite criteria are satisfied within the declared Linux/amd64 scope.**

| Identity | Server | Client |
|---|---|---|
| Verified OCI index / loaded image ID | `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` | `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` |
| Verified loaded RepoDigest | `minio/minio@sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` | `minio/mc@sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` |
| Linux/amd64 manifest digest | `sha256:d051d800a3025588f37f69f132bb5ef718547a9a4ee95ddee44e04ad952a0a96` | `sha256:4ccb0c7437bda19ed232603eab10cc246c8f9ac7ccd282943ebb7657a2df4820` |
| Config digest | `sha256:afadd2395ca65dde8ec87e5c8460c97ead4561c21dc5c373bb83b099dfcbd657` | `sha256:88cf702a7041c364ae320e0585ac4de617719a52536ec171224a4e02089281da` |
| Executable SHA256 | `c27873d791b56d33518de11f12c5bfae4be88ebac00e12c6dd0027b08123fd80` | `a2b886f974c4b32ee57c5bf6f3c8070bc4ac8a62ce3556cc241514aeb5427bd1` |

The previous unresolved MinIO artifact-source/authorization condition is now satisfied by the **specific owner-approved sealed archive**, not by a claim of newly restored registry access or generic cache usability. The archive can serve as the controlled source for the proposed local Linux/amd64 packaging work, provided its fixed checksum is verified and the documented identity-preserving load behavior is honored.

This gate updates the prerequisite conclusion through a new report. Earlier reports remain unchanged historical evidence. Other artifact-availability observations are inherited from the original gate, not freshly recertified here; no new unavailable required artifact is asserted by this bounded review.

## 6. Limitations

- **Linux/amd64 only:** original multi-platform index bytes are preserved, but non-amd64 payloads are absent. The archive is not a complete all-architecture registry backup. Both required amd64 configs and complete layer sets are verified.
- **Tested Docker store:** independent load used Docker29.6.2 with the containerd image store. Legacy stores may report different image IDs or discard index/RepoDigest metadata. Such behavior is not assumed equivalent; a later runtime must verify its actual loaded identities.
- **Isolation scope:** the successful verification used a separate initially empty daemon/data store on the same physical host. No second-host operating-system qualification is claimed.
- **Provenance scope:** the owner expressly adopted these cached exact-image contents as the sealed source. This does not establish renewed Docker Hub/Quay availability, remote publication, publisher-signature certification or archive escrow.
- **Application scope:** artifact version/hash execution is not MinIO bucket initialization, application build/install, full pipeline launch, restart/regression evidence or M1–M9 acceptance certification. Python transitive compatibility and other future packaging obligations remain as previously defined.
- **Integrity qualification carried forward:** the preceding archive phase recorded a harness-managed `.remember` metadata replacement. It did not change frozen application/configuration/evidence; this gate does not retroactively claim a raw-all-files PASS for that earlier phase.

**Full packaging reproducibility is not claimed. Phase C is not complete and has not started. Fresh-runtime application certification is not claimed.**

## 7. Frozen-state integrity

Pre-gate current inventory, including the existing archive and all prior reports:

| Inventory | Files | Bytes | Before aggregate SHA256 |
|---|---:|---:|---|
| All existing regular project files | 1068 | 93289679 | `1b0b50da8af2963461b4a966a7d0cbabf5b6582cbe93cd350b3da5e3cb644e2f` |
| Same inventory excluding harness `.remember/` | 1026 | 93255634 | `ddfadb3698c8403735af2dd3e86979525d3486b7b4d3c1d410946fa4758ee284` |

Method: SHA256 each regular non-symlink file; sort `(relative path, file SHA256)` pairs; compact JSON serialization `separators=(',', ':')`; SHA256 the serialized list. Directory exclusions: `.venv`, `.git`, `__pycache__`, `.pytest_cache`, `.ruff_cache`. Only the new `artifact_reverification_gate_report.md` is excluded from the final existing-file comparison. Active `.env` was hashed without printing its contents; Python ran with `-B`.

Read-only Docker baseline:38 images,21 containers; all nine retained OIP containers exited/not running. The image inventory fingerprint includes IDs/tags/RepoDigests/platform/RootFS/descriptors, using sorted IDs and compact key-sorted JSON. Its baseline SHA256 is `c0c4d4fed5b4abd180a4c981fa817d0a3a6772b86b7f665d16d7a440588c5bfe`. The retained-state fingerprint includes container name/ID/image/status/running/start/finish/restart count, name-sorted and compact key-sorted JSON; baseline SHA256 is `f1f2f7305080a496f1128a8b6fff1d486f9162fc17e031fec6ed6886debc51fa`.

**Post-report integrity: PASS.** The completed existing-file comparison returned:

```json
{"existing_files":1068,"existing_bytes":93289679,"existing_sha256":"1b0b50da8af2963461b4a966a7d0cbabf5b6582cbe93cd350b3da5e3cb644e2f","all_existing_match":true,"non_harness_match":true,"archive_bytes":80105984,"archive_sha256":"58800abbac143c831a53c00fadbe9fe0af30914c83835fd52bc7a7c487fe355a","only_added_file":"artifact_reverification_gate_report.md"}
```

Both Docker fingerprints also matched their baseline exactly: **image inventory unchanged; retained OIP exact state unchanged; all nine OIP containers stopped**. All eight required report sections were confirmed. The existing sealed archive and authoritative verification report are unchanged; no current-gate harness metadata difference was observed.

No image or runtime mutation command was used. No new archive, Docker resource, deployment file or helper script was created by this gate.

**Files created by this gate:** `artifact_reverification_gate_report.md` only.  
**Pre-existing files modified by the agent:** NONE.

## 8. Final gate verdict

**PASS — PATH B ARTIFACT PREREQUISITE SATISFIED; READY FOR SEPARATE PACKAGING IMPLEMENTATION AUTHORIZATION**

Both exact frozen MinIO artifacts now have a specifically authorized, checksummed and independently verified archive acquisition path for Linux/amd64. The previously missing source decision and artifact-verification requirements are satisfied by the sealed-archive evidence.

**Packaging implementation has NOT started. Phase C has NOT started.** This gate grants no implementation permission; separate explicit packaging authorization is still required.

STOP after this gate report.
