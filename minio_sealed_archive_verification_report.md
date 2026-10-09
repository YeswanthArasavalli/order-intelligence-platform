# MinIO Sealed Archive Verification

**Audit date:** 2026-10-05 UTC  
**Authorization:** explicitly selected PATH B — sealed archive of the two cached exact frozen images  
**Scope:** archive creation and independent Linux/amd64 artifact verification only  
**Artifact verification verdict:** **PASS**, with the platform and provenance limitations in §12

## 1. Objective

Create a fixed, checksummed, provenance-preserving archive of the two exact cached frozen MinIO images, then demonstrate that it loads and executes in an isolated Docker daemon with no access to the source image cache.

The complete Linux/amd64 image identity chain was checked: original OCI index bytes → selected platform manifest → config → all compressed layer blobs and their uncompressed DiffIDs. Archive, image/index, config, layer and executable hashes are reported separately.

**Result:** both images were archived and independently loaded with their exact governing OCI index digests, Docker image IDs and RepoDigests preserved. All26 archived content blobs matched their bytes in the isolated content store. Exact release and executable probes passed. This is an artifact result, not application packaging or M1–M9 runtime certification.

## 2. Authorization

The owner explicitly instructed:

> I explicitly authorize PATH B — EXPLICIT SEALED-ARCHIVE AUTHORIZATION.
> Proceed with ONLY the bounded sealed-archive creation and independent artifact verification phase for the two exact cached frozen MinIO images.

This authorizes this specific cached-image archive as the artifact source, subject to independent identity/load verification. It does not authorize a general cache backup, version substitution, source rebuild, packaging implementation, Phase C, retained-runtime startup, deployment files or acceptance-suite execution.

Inputs include the unchanged `post_m9_roadmap_gate_report.md`, `artifact_availability_gate_report.md`, `minio_artifact_unblock_report.md` and `artifact_authorization_decision_gate_report.md`. The earlier decision report is historical: its lack of authorization was superseded by this explicit user instruction, not by editing that report.

No previous MinIO registry investigation was repeated. The only new remote image acquisition was a temporary official Docker daemon tool image, used solely to establish the independent image state.

## 3. Governing Artifacts

| Artifact | Exact source repository/tag | Governing digest | Required platform |
|---|---|---|---|
| Server | `minio/minio:RELEASE.2024-11-07T00-52-20Z` | `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` | `linux/amd64` |
| Client | `minio/mc:RELEASE.2024-11-05T11-29-45Z` | `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` | `linux/amd64` |

The governing values are now directly verified as **OCI image-index digests** from available local descriptor/blob content. They are not config hashes or raw binary hashes.

## 4. Pre-export Identity Verification

The source engine was Docker Desktop4.84.0, Docker Engine29.6.2/API1.55, Linux/amd64, context`desktop-linux`, using the containerd image store and `overlayfs` snapshotter. Before work, the source daemon held38 images and21 containers; all nine retained OIP containers were stopped.

At **2026-10-05T11:52:10.761506+00:00**, the pre-export audit asserted for each source image:

- Exact requested tag present.
- Docker image ID equal to the governing digest.
- Local `.Descriptor.digest` equal to the governing digest.
- Descriptor media type `application/vnd.oci.image.index.v1+json`.
- Expected repository-qualified RepoDigest present.
- OS/architecture `linux`/`amd64`.
- Config fields and complete ordered RootFS DiffIDs captured for later comparison.

```bash
docker.exe image inspect --format "{{json .}}" minio/minio:RELEASE.2024-11-07T00-52-20Z minio/mc:RELEASE.2024-11-05T11-29-45Z
```

| Identity domain | Server | Client |
|---|---|---|
| Source Docker image ID | `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` | `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` |
| Source OCI index digest | `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` | `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` |
| Source RepoDigest | `minio/minio@sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` | `minio/mc@sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` |
| Original index byte size | 2364 | 2364 |
| Selected Linux/amd64 manifest digest | `sha256:d051d800a3025588f37f69f132bb5ef718547a9a4ee95ddee44e04ad952a0a96` | `sha256:4ccb0c7437bda19ed232603eab10cc246c8f9ac7ccd282943ebb7657a2df4820` |
| Selected manifest byte size | 2011 | 1438 |
| Config digest | `sha256:afadd2395ca65dde8ec87e5c8460c97ead4561c21dc5c373bb83b099dfcbd657` | `sha256:88cf702a7041c364ae320e0585ac4de617719a52536ec171224a4e02089281da` |
| Config byte size | 8451 | 5852 |
| RootFS layers | 9 | 6 |
| Platform | `linux/amd64` | `linux/amd64` |
| Image build timestamp | `2024-11-08T00:03:50.81147061Z` | `2024-11-08T00:17:33.309263921Z` |

Docker image IDs here identify root indexes under this containerd-backed engine. The actual config digests are different and are explicitly derived from the selected manifests and verified blob bytes. Previous reports' observations of image ID/RepoDigest equality must not be interpreted as config/index equivalence.

The manifest/config/layer identities in the table were established by hashing the immutable content emitted directly by `docker image save` and comparing its config/RootFS with the captured pre-export inspection. No source image was retagged, rebuilt, flattened, squashed or edited.

## 5. Archive Creation

**Created artifact:** `minio_frozen_linux_amd64_sealed_20261005.tar`, in the project root.

Windows source path:

```text
<repository-root>\minio_frozen_linux_amd64_sealed_20261005.tar
```

Linux workspace path:

```text
<repository-root>/minio_frozen_linux_amd64_sealed_20261005.tar
```

Exact export command:

```bash
docker.exe image save --output "<repository-root>\minio_frozen_linux_amd64_sealed_20261005.tar" minio/minio:RELEASE.2024-11-07T00-52-20Z minio/mc:RELEASE.2024-11-05T11-29-45Z
```

The command completed successfully. No platform-pruning rewrite was requested: the original index bytes were preserved, with the locally available Linux/amd64 content. No other source image or application state was selected for export.

Export completion/file modification timestamp: **2026-10-05T11:52:16.933840+00:00**. This is the measured completed-file timestamp, not a claimed historical upstream publication time.

The tar contains **29 regular members**:26 content-addressed blobs plus `index.json`, `manifest.json`, and `oci-layout`. OCI layout version is1.0.0. Docker's compatibility manifest names exactly the two frozen repository/tags. The transport `index.json` references both unchanged original image indexes.

The archive was sealed by fixing its checksum and removing write permissions with `chmod a-w`; the measured WSL permission mode is **0555**. No bytes were edited after export. A read-only filesystem flag is local protection; the recorded SHA256 is the integrity check for transfer/load.

## 6. Archive SHA-256

| Field | Value |
|---|---|
| Filename | `minio_frozen_linux_amd64_sealed_20261005.tar` |
| Size | **80105984 bytes** |
| Archive SHA256 | **`58800abbac143c831a53c00fadbe9fe0af30914c83835fd52bc7a7c487fe355a`** |
| SHA256 after independent transfer into isolated daemon container | Same exact value |
| SHA256 after execution and disposable-resource cleanup | Same exact value |
| Archive transport `index.json` SHA256 | `ddd16e63c67980e9fcbe71e65fcdea5eb43f33f8fffb44543d60a4345f19cdca` |

The transport-index hash is not either image-index hash, and none of these values is a config, executable or layer hash.

The full tar was streamed through SHA256 before load and again after cleanup. The independent copy was hashed inside the verifier before import:

```bash
docker.exe exec minio-archive-isolated-20261005 sha256sum /tmp/minio-sealed.tar
```

Output:

```text
58800abbac143c831a53c00fadbe9fe0af30914c83835fd52bc7a7c487fe355a  /tmp/minio-sealed.tar
```

## 7. Archive Provenance

The adopted source is **the owner's explicitly authorized exact cached images on the existing Docker Desktop daemon**, not a newly claimed remote registry source. Repository tags, local image IDs, original RepoDigests, descriptors, build timestamps, platform, config and all Linux/amd64 layers were recorded. The preceding official-binary provenance evidence was retained and used only for executable verification.

For every included `blobs/sha256/<digest>` member, its actual byte SHA256 matched its filename. Original index, selected manifest and config hashes/sizes were verified against their descriptors. Each compressed layer was verified against its manifest digest; its decompressed tar SHA256 matched the config's ordered `rootfs.diff_ids` and pre-export Docker inspection.

There are15 ordered layer references across the images, comprising14 unique blobs because the base layer is shared. All are present and were also independently read and hashed in the loaded content store.

### Server Linux/amd64 layer identities

| Layer | Compressed blob SHA256 | Compressed bytes | Uncompressed DiffID SHA256 |
|---:|---|---:|---|
| 1 | `55360c0b72d681ef9173ea809fdba2f8b7cfcc396d99531d43d8d927cbf7387f` | 7285285 | `9917ecb8e223ecc410a27cecdf3769bbb34e35e84ba045839c375d16a7fc1476` |
| 2 | `f2f8f30a646af9b050a5a82bb5f3a6c1ce08a422ab0a31194b1bb221f14f0783` | 1638810 | `13e8315a6723ed2322469a7208ea4adb60e9c46b0c9a20e59a2ab2849802da4f` |
| 3 | `3440aa9567dd1d2e8583a8a099be34dc50552a383ca7bb8f4e2731f1e6e9529e` | 126708 | `52ab8667633be3e1a100c6d4318c961379ecc94ea00ca8b4e7daf8ad25776cd8` |
| 4 | `4414594dd5104b0fcd98ab36e756169536237eb9d6971b26778e492d109089fd` | 37424955 | `b1e4358c0b0c4e56e6fe3176848b37016e0caeb1507b4e7380924e44abb9aa7a` |
| 5 | `c1cc85e2de65f9f49e4c7f8895a8d35724fbac6c00f25b6bee99d271879bbb68` | 10145676 | `cfbd5b11e28650c5e24eb58b8a398813d88c24d4eec265f6e4c64796c19c2525` |
| 6 | `d57a4fe62ee8f498f49171ec002eaf1211ceb1e1d4f7826615e2cd2ed92b5eff` | 2394829 | `4594bc07642dca471984211688ea232260f0cd79763d9e07c4a9e40f1f9c289c` |
| 7 | `48e0cffc0f68dfc7fc5395f6002b57fd8c04d79852cc85aa2b3002e0ff65bbbf` | 172952 | `54d506c95f0885f7459cf6c981c16ee096903b8567ca7e490696bb50a8b9225f` |
| 8 | `2b027acd57fe5f8f07662b6ad02e8163b970ebab34417354deabdc4a9ea33c12` | 11876 | `8d54ab2eedd67bde4ea1ea5c20d480fe1e40e1521c61eee61899bd3ac2b4d620` |
| 9 | `c1d0e26236f586317ed8c2bb5336d08a406720d1e48c84b167913ed11aee1ec3` | 500 | `495788686561f1a3a712e0a4d22d523e538bcb588901d3c17b56bcc117dc0f9c` |

### Client Linux/amd64 layer identities

| Layer | Compressed blob SHA256 | Compressed bytes | Uncompressed DiffID SHA256 |
|---:|---|---:|---|
| 1 | `55360c0b72d681ef9173ea809fdba2f8b7cfcc396d99531d43d8d927cbf7387f` | 7285285 | `9917ecb8e223ecc410a27cecdf3769bbb34e35e84ba045839c375d16a7fc1476` |
| 2 | `7fcd74abb7ca4b3eae848f7c540791dbc479f1627a0dde2015c8a77d89fd7761` | 129919 | `8289a506ccd1d4427d6d63c90a48e1785557ab1026a852a64ed675a22addb4d9` |
| 3 | `b1782d1656d78915ef3b656b6d20bc3a288fb50c5d4e66b3940c6c5ed0d4d5e4` | 68836 | `2a1ff8e764b38981b0d2ca1a0dbabcf7264f1e1cecfcb771b727b1154a1ac0dd` |
| 4 | `ff21e8f36d5638982691afc4918c1054e4bac28df90db4c9d4d730ec85e386cf` | 11877 | `da25f5a68929e375ed2e0795f1bdaa884e294251b34dd36bd2d79589f53dc466` |
| 5 | `8709d551a1b2ca9ebd60c043d17bc998cb77fffb2fc93c9dc36b6508d0c8f571` | 10319400 | `cf637a8dabad2a8e6407fae02b69497ea4f8bf452153c78725c252e50ec3873d` |
| 6 | `6fdc21f85c2a7c280a32ed397fcef75619ff1d0441e6ddf98e1e9246e0fec1dc` | 10319401 | `c9a7ca4a8c5df324e31769e28be6cd8e80a2ea99261e11d78062ea10713a9ac4` |

All layer media types are `application/vnd.oci.image.layer.v1.tar+gzip`. Compressed blob identities and uncompressed DiffIDs are distinct values.

The remaining included blobs are the two indexes, two platform manifests, two configs and six Linux/amd64-attestation-associated blobs. Their presence and bytes were verified too; no cryptographic publisher-attestation certification is claimed.

## 8. Isolated Load Procedure

### Independent Docker image state

The official tool image `docker:29.6.2-dind` was newly acquired at measured digest:

```text
sha256:bfec1f5159c63a81ca6fdedbd81404d2c0e16378ed0feec3bb3fbf3998847659
```

This supplied a temporary Docker29.6.2 daemon, matching the source engine version. It was not a project dependency upgrade or application image. The helper and its tool image were removed after verification.

Exact daemon creation command:

```bash
docker.exe run --detach --name minio-archive-isolated-20261005 --privileged --network none --env DOCKER_TLS_CERTDIR= --entrypoint dockerd docker@sha256:bfec1f5159c63a81ca6fdedbd81404d2c0e16378ed0feec3bb3fbf3998847659 --host unix:///var/run/docker.sock --data-root /var/lib/docker --exec-root /var/run/docker-exec --iptables=false --ip6tables=false --bridge=none --feature containerd-snapshotter=true
```

Isolation evidence:

- Outer helper container ID: `c58d2d389506b716717738d8ac043402378ad880ee3304aa1d1f246a4aedfec2`.
- Outer network mode: **none**, no published ports.
- Inner Docker daemon ID: `e2227936-69fd-4363-8c52-9764fd4e333f`.
- Inner Docker29.6.2, Linux/x86_64, containerd image store/overlayfs.
- Inner Docker root: `/var/lib/docker`, backed by a **new anonymous volume**, not the source daemon data root.
- New volume: `0cddf5203310053d993cfd65215e2f647d362898a1c2d49b38ec6626c211088a`.
- No bind mount of the source Docker socket, source image store, project or retained OIP data.
- Before copying/loading the archive, inner daemon reported **Images=0, Containers=0**. It did not inherit the helper image or source images.

All inner Docker commands explicitly targeted `unix:///var/run/docker.sock` **inside the helper**, reached via outer `docker.exe exec`. They did not resolve to the Windows Docker Desktop socket. The helper's privileged mode enabled the nested daemon; it had no attached OIP network or retained data.

### Transfer, checksum and load

```bash
docker.exe cp "<repository-root>\minio_frozen_linux_amd64_sealed_20261005.tar" minio-archive-isolated-20261005:/tmp/minio-sealed.tar
docker.exe exec minio-archive-isolated-20261005 sha256sum /tmp/minio-sealed.tar
docker.exe exec minio-archive-isolated-20261005 docker --host unix:///var/run/docker.sock image load --input /tmp/minio-sealed.tar
```

The copied archive checksum matched §6. Load exited successfully and printed:

```text
Loaded image: minio/minio:RELEASE.2024-11-07T00-52-20Z
Loaded image: minio/mc:RELEASE.2024-11-05T11-29-45Z
```

Both tag-registration timestamps in the isolated daemon were approximately **2026-10-05T11:54:14 UTC**. Later inner daemon state was **Images=2, Containers=0**, including after the four disposable execution probes.

For reproduction, transfer this exact tar together with this report, verify its SHA256/size before loading, and use an empty Linux/amd64 Docker image state with the tested containerd-backed Docker29.6.2 behavior. Loading the sealed bytes is the artifact procedure; a subsequent source-cache lookup is unnecessary. This was tested in independent daemon state on the same host, not a physically separate machine.

## 9. Post-load Identity Verification

Fresh post-load inspection used:

```bash
docker.exe exec minio-archive-isolated-20261005 docker --host unix:///var/run/docker.sock image inspect --format "{{json .}}" minio/minio:RELEASE.2024-11-07T00-52-20Z minio/mc:RELEASE.2024-11-05T11-29-45Z
```

| Verified loaded identity | Server | Client |
|---|---|---|
| Docker image ID | `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` | `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` |
| OCI index descriptor digest | `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` | `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` |
| RepoDigest | `minio/minio@sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` | `minio/mc@sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` |
| Linux/amd64 manifest digest | `sha256:d051d800a3025588f37f69f132bb5ef718547a9a4ee95ddee44e04ad952a0a96` | `sha256:4ccb0c7437bda19ed232603eab10cc246c8f9ac7ccd282943ebb7657a2df4820` |
| Config digest | `sha256:afadd2395ca65dde8ec87e5c8460c97ead4561c21dc5c373bb83b099dfcbd657` | `sha256:88cf702a7041c364ae320e0585ac4de617719a52536ec171224a4e02089281da` |
| Config fields versus source | Exact equality | Exact equality |
| Ordered RootFS DiffIDs versus source | Exact equality, 9 layers | Exact equality, 6 layers |
| OS/architecture | `linux/amd64` | `linux/amd64` |
| Exact governing digest equality | **Verified** | **Verified** |

The actual loaded content store was independently read through:

```text
docker.exe exec minio-archive-isolated-20261005 ctr --address /var/run/docker-exec/containerd/containerd.sock --namespace moby content get <each exact archived blob digest>
```

For **all26 blobs**, the actual returned bytes matched the archive's SHA256 and byte size. This included both governing index documents, selected manifests, configs and compressed layers, so the result is not merely equality of Docker metadata strings. Verification completed at **2026-10-05T11:56:32.978682+00:00**.

An initial content-list diagnostic used the conventional `/var/run/docker/containerd/containerd.sock` path and returned “no such file”; the daemon's explicit `--exec-root` placed the socket at `/var/run/docker-exec/containerd/containerd.sock`. The corrected content read succeeded for every blob. This path correction did not modify either image or the archive.

Unlike a legacy Docker store/import that may retain only configs and lose registry RepoDigests, this tested containerd-backed export/load preserved both original OCI index descriptors and both RepoDigests. No digest was fabricated, manually assigned or recreated by retagging.

## 10. Version/Executable Verification

All four probes ran **inside the isolated loaded daemon**, using exact digest references, `--pull never`, `--network none` and `--rm`. No retained service was used.

```bash
docker.exe exec minio-archive-isolated-20261005 docker --host unix:///var/run/docker.sock run --rm --network none --pull never --platform linux/amd64 --entrypoint minio minio/minio@sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332 --version
docker.exe exec minio-archive-isolated-20261005 docker --host unix:///var/run/docker.sock run --rm --network none --pull never --platform linux/amd64 --entrypoint mc minio/mc@sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682 --version
docker.exe exec minio-archive-isolated-20261005 docker --host unix:///var/run/docker.sock run --rm --network none --pull never --platform linux/amd64 --entrypoint sha256sum minio/minio@sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332 /usr/bin/minio
docker.exe exec minio-archive-isolated-20261005 docker --host unix:///var/run/docker.sock run --rm --network none --pull never --platform linux/amd64 --entrypoint sha256sum minio/mc@sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682 /usr/bin/mc
```

Successful exact version outputs:

```text
minio version RELEASE.2024-11-07T00-52-20Z (commit-id=cefc43e4daa4cbb490ef6726ea374e26a93eb85e)
Runtime: go1.23.3 linux/amd64

mc version RELEASE.2024-11-05T11-29-45Z (commit-id=6ac18619cf881074fe6edcc79ab62c9c85da60b9)
Runtime: go1.23.3 linux/amd64
```

Successful executable-hash outputs:

```text
c27873d791b56d33518de11f12c5bfae4be88ebac00e12c6dd0027b08123fd80  /usr/bin/minio
a2b886f974c4b32ee57c5bf6f3c8070bc4ac8a62ce3556cc241514aeb5427bd1  /usr/bin/mc
```

Both equal the independently acquired official release-binary hashes established by the previous unblock audit. Executable identity supplements the full image-content checks in §§7–9; it is not their substitute. These were version/hash invocations, not a MinIO server serving retained data or a bucket initializer making requests.

## 11. Integrity and Non-modification Verification

### Project-file integrity

Before export, the inventory contained **1066 regular existing files**, **13153357 bytes**, aggregate SHA256:

```text
72175bcc4f627b24c9d4bc2790f3edecc7db7e4763f877d9a76f229c79da6784
```

Exclusions were `.venv`, `.git`, `__pycache__`, `.pytest_cache`, `.ruff_cache`; `.env` was hashed without revealing its content. Each path/file SHA256 was retained for comparison. The new archive and this report are excluded from the final existing-file comparison.

The stable application/configuration/contracts/tests/reports/evidence inventory, excluding harness-managed `.remember/`, contains **1024 files**, **13119312 bytes**, aggregate:

```text
57e3cbcc65a8aec45f89bd92966f4d3cedde512431c062a0a809f9b47d0fbb45
```

After archive verification and helper cleanup, every existing file outside `.remember/` remained byte-identical. **The complete raw path inventory is not claimed unchanged:** the harness replaced `.remember/tmp/remember-config-83.json` with `.remember/tmp/remember-config-1215.json` during the audit. Both are3-byte files with identical SHA256 `ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356`. No agent command edited, removed or restored these harness files. That generated metadata change is recorded rather than hidden behind a full-inventory PASS.

The raw post-cleanup and post-report existing-file aggregate was `a1a35261c565fc23a9ce69c1dc9ed00a280f48250b4712d9d34a2326193ee0d7`, still1066 files/13153357 bytes. The post-report per-file comparison confirmed that the harness filename replacement above was the only existing-inventory difference. All1024 non-harness application/configuration/dependency/contract/test/report/evidence files retained their before content/path hashes and aggregate. The new archive and report are the only agent-created project artifacts.

### Exact files created and modified

**Project artifacts created by this task:**

1. `minio_frozen_linux_amd64_sealed_20261005.tar` — the authorized sealed artifact, fixed bytes/checksum in §6.
2. `minio_sealed_archive_verification_report.md` — this verification/provenance report.

**Existing project files modified by the agent: NONE.** Existing application, Compose, Dockerfiles, dependency pins, `.env`, tests, contracts and prior reports were not edited. No `deployment/local/` files or directories were created.

Temporary audit tooling/evidence created outside the project under `/tmp/omnirush/`:

- `minio_archive_audit.py` — bounded local checksum/identity audit utility, not a project packaging script.
- `minio-source-baseline.json` — file/image/container before inventory.
- `minio-archive-audit.json` — archive hash/content graph evidence.
- `minio-isolated-empty.json` — independent zero-image/zero-container daemon evidence.
- `minio-postload-identity.json` — loaded metadata and26 independently read blob checksums.
- `minio-final-integrity.json` — final per-file/source-image/retained-container comparison.

These temporary evidence files are not runtime or packaging inputs. The durable artifact procedure depends on the fixed tar and this report, not on undeclared helper-installed application packages.

### Docker resources created and removed

| Resource | Created / operation | Final disposition |
|---|---|---|
| Official `docker:29.6.2-dind` tool image at digest`bfec1f...847659` | Pulled solely for independent daemon tooling | Removed; helper image did not pre-exist |
| Container `minio-archive-isolated-20261005`, ID`c58d2d...edfec2` | Separate network-none nested daemon | Stopped and removed |
| New anonymous volume`0cddf5203310053d993cfd65215e2f647d362898a1c2d49b38ec6626c211088a` | Empty inner image/data store | Removed with helper using`rm --volumes` |
| Two exact images in the inner daemon | Loaded only from copied sealed tar | Removed when the isolated volume was removed |
| Four disposable inner probe containers | Two version and two executable-hash probes, `--rm` | Automatically removed; inner final Containers=0 |
| Inner copied `/tmp/minio-sealed.tar` | Checksum-verified independent copy | Removed with helper container filesystem |

Cleanup commands:

```bash
docker.exe stop --time 10 minio-archive-isolated-20261005
docker.exe rm --volumes minio-archive-isolated-20261005
docker.exe image rm docker:29.6.2-dind
```

Docker noted the `--time` stop flag is deprecated in favor of `--timeout`; stop still succeeded. Explicit inspections afterward returned “no such container”, “no such volume”, and “no such image” for these disposable resources.

### Retained OIP resources

**Retained OIP resource mutations: NONE.** Source image inspection/export and stopped-container inspection were read-only. No retained container was started, restarted, deleted, rebuilt, retagged, mounted into the verifier, or used for execution. No retained volume/network/topic/database/cache/lake/receipt state was operated on.

All original38 source images retained the same IDs, tags, RepoDigests, descriptors, platform and RootFS inventory. Each of the nine OIP containers retained its exact ID, image binding, stopped state, start/finish timestamps and restart count. Source daemon final counts returned to **Images=38, Containers=21**. Unrelated Supabase services were not operated on.

## 12. Limitations

1. **Platform scope is Linux/amd64.** The original multi-platform OCI indexes are preserved byte-for-byte, including references to arm64, ppc64le and attestation manifests. Only the cached Linux/amd64 image payloads and their locally available attestation-associated blobs are included. Other platform payloads are absent; this is not a complete all-architecture registry backup. Every required Linux/amd64 config and layer is included and independently verified.
2. **Docker store behavior is explicit.** Digest-preserving load was tested with Docker29.6.2 and the containerd image store. A legacy image store may report config-based image IDs, omit RepoDigests or discard index metadata. This report does not invent equivalence for such an untested load. Future use must verify actual post-load identities.
3. **Source authorization is cache-specific.** The owner expressly adopted these exact cache contents as the archive source. This does not assert a new successful remote MinIO pull, publisher-signature certification, or continuing availability at Docker Hub/Quay. The governing index bytes are independently hashed here, and binary provenance is consistent with the official releases.
4. **Physical-host independence was not tested.** The verifier used a separate initially empty daemon/data store with no source-cache socket/mount and network disabled on the same host. It demonstrates archive-only acquisition/load, not a second-host operating-system qualification.
5. **Seal semantics:** fixed filename, fixed checksum and read-only local permissions establish the artifact to transfer/verify. No signing key, external artifact registry publication, escrow or immutable remote-storage policy was created.
6. **Artifact execution only:** version/hash probes passed; no MinIO service initialization, OIP stage, M1–M9 acceptance suite, packaging build, pipeline execution or restart/business regression was run.
7. **Integrity scope is honest:** frozen application/configuration/evidence and earlier reports are unchanged; the independently generated harness metadata rename in §11 prevents a blanket claim that every raw workspace path is unchanged.

## 13. Final Verdict

**PASS — authorized sealed-archive creation and independent verification of both exact frozen Linux/amd64 MinIO artifacts.**

| Acceptance requirement | Result |
|---|---|
| Both exact cached images exported | PASS |
| Archive checksum, size, timestamp and source provenance recorded | PASS |
| Initially empty isolated Docker state, no original-cache dependency | PASS |
| Successful archive-only load | PASS |
| Both images Linux/amd64 | PASS |
| Governing OCI index digests, image IDs and RepoDigests independently preserved | PASS |
| Selected platform manifest/config/all layer bytes independently verified | PASS |
| Exact versions and official-release executable hashes | PASS |
| No rebuild, retag, squash, version/object-store substitution | PASS |
| Retained OIP runtime untouched and stopped | PASS |

**Final post-report audit: existing application/configuration/contracts/tests/reports/evidence unchanged; original source images and retained OIP container states unchanged.** The detailed audit returned:

```json
{"existing_files":1066,"existing_bytes":13153357,"existing_sha256":"a1a35261c565fc23a9ce69c1dc9ed00a280f48250b4712d9d34a2326193ee0d7","all_preexisting_files_unchanged":false,"application_and_evidence_unchanged":true,"original_source_images_unchanged":true,"oip_exact_state_unchanged":true,"oip_stopped":true}
```

The false raw-all-files field is the explicitly recorded harness metadata replacement in §11, not an application, image or evidence modification. The report-structure check verified all13 required sections; all15 reported layer digest/size/DiffID rows matched the measured archive inventory. Archive checksum remained the fixed value in §6. The final audit was executed using the temporary external audit utility, `python3 -B /tmp/omnirush/minio_archive_audit.py final`; no audit script was added to the project.

The **Path B MinIO artifact-source prerequisite is satisfied for this exact archive and Linux/amd64 scope**. A separate bounded Phase B re-verification/authorization step may consider this evidence. This is not a full Phase B packaging gate PASS and does not authorize or certify Phase C.

**Packaging implementation has NOT started. Phase C has NOT started. Packaging reproducibility is NOT claimed.**

Stop after this archive-verification phase.
