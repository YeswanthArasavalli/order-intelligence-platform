# Artifact Authorization Decision Gate

## 1. Objective

This is an owner authorization gate for the exact frozen MinIO artifacts required by the Reproducible Local Runtime Packaging & Launch Verification work.

It does not implement packaging, start Phase C, alter the application architecture, change MinIO versions, create an archive, or modify the frozen runtime. Its purpose is to identify whether the owner has authorized one of the two legitimate paths needed for a later bounded artifact re-verification.

No explicit owner authorization for Path A or Path B is present in the current inputs. The correct outcome is therefore **BLOCKED — OWNER DECISION REQUIRED**.

## 2. Governing Frozen MinIO Artifacts

| Artifact | Exact frozen reference | Governing digest | Platform |
|---|---|---|---|
| Server | `minio/minio:RELEASE.2024-11-07T00-52-20Z` | `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` | `linux/amd64` |
| Client | `minio/mc:RELEASE.2024-11-05T11-29-45Z` | `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` | `linux/amd64` |

Both artifacts are required independently. The server is the frozen object-storage service; the client is the frozen bucket-initialization image. A successful decision for only one artifact is insufficient.

## 3. Previously Established Evidence

The following distinctions are authoritative and must remain separate.

### Complete image identity evidence

- The cached Docker images have the governing local image IDs and RepoDigests shown above, and `docker image inspect` reports Linux/amd64.
- The cached images execute the exact frozen release/version probes.
- A complete independently retrievable OCI/Docker manifest, image config and layer set has **not** been established for either artifact.
- The cached image identity is therefore local evidence, not a reproducible clean-source result.

### Executable/binary identity evidence

- Official GitHub release binaries for both exact releases were downloaded independently.
- Their published SHA256 values matched the full downloaded bytes.
- Their Linux/amd64 ELF identity and release commits matched the cached executables:
  - Server binary SHA256: `c27873d791b56d33518de11f12c5bfae4be88ebac00e12c6dd0027b08123fd80`.
  - Client binary SHA256: `a2b886f974c4b32ee57c5bf6f3c8070bc4ac8a62ce3556cc241514aeb5427bd1`.
- Matching executable hashes do **not** prove complete OCI image identity. They do not establish image configuration, entrypoint, base filesystem, metadata or all layers.

### Cached-image evidence

- Server cached image: governing digest `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332`, Linux/amd64, release `RELEASE.2024-11-07T00-52-20Z`.
- Client cached image: governing digest `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682`, Linux/amd64, release `RELEASE.2024-11-05T11-29-45Z`.
- Cached existence and cached execution are useful identity evidence but are insufficient for fresh-runtime reproducibility without an authorized source or sealed archive.

### Registry/source availability evidence

- Exact Docker Hub tag and digest pulls failed for both images.
- Exact official Quay tag and digest pulls failed with unauthorized responses; anonymous manifest access did not resolve the issue.
- Historical binary-download URLs returned HTTP410.
- Official GitHub binary assets are available and verified, but they are binary artifacts rather than complete frozen OCI images.
- No authorized organization registry, complete OCI archive or new credentials/source has been supplied in the current inputs.

## 4. Current Blocking Condition

The blocker is an authorization and distribution decision, not Docker Engine availability.

The project does not currently have an owner-authorized path that can independently supply and verify **both complete exact frozen images**. Cached images cannot be declared reproducible merely because their release probes and embedded executable hashes match. Official binaries cannot be substituted for the required images, and no image archive has been created or authorized.

Accordingly, the MinIO artifact gate remains blocked and Phase C is prohibited.

## 5. Owner Decision Required

The owner must explicitly select one of the following:

1. **PATH A — Authorized Exact Image Source:** authorize a source, registry or credentials path capable of supplying both complete exact images; or
2. **PATH B — Explicit Sealed-Archive Authorization:** authorize creation and use of a sealed archive from the already cached exact images, subject to the provenance and clean-load requirements below; or
3. **PATH C — No Authorization / Stop:** provide no artifact authorization and stop the packaging initiative at this gate.

No decision is inferred from the existence of cached images, official binaries, prior reports, or the general packaging authorization. Credentials, if later needed for Path A, must be supplied through an appropriate secret channel and must never be printed in a report.

## 6. Decision Options

### PATH A — Authorized Exact Image Source

The owner authorizes access to a legitimate source capable of supplying both:

- `minio/minio:RELEASE.2024-11-07T00-52-20Z` at digest `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332`.
- `minio/mc:RELEASE.2024-11-05T11-29-45Z` at digest `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682`.

The source may require credentials, but the credentials are not part of this report. Selecting Path A would authorize only a **bounded artifact re-verification phase**. It would not authorize packaging implementation or Phase C.

### PATH B — Explicit Sealed Archive Authorization

If no authorized registry/source is available, the owner may explicitly authorize a sealed archive created from the existing cached exact images.

Selecting Path B would authorize only a **bounded sealed-archive creation and independent load/identity verification phase**. The archive must preserve provenance and must not be treated as a generic warm-cache workaround. The archive must contain both exact images and must be independently usable by a clean runtime.

No archive has been created, exported, loaded, transferred, or adopted in this gate.

### PATH C — No Authorization / Stop

If the owner does not authorize Path A or Path B, the project remains blocked at the artifact gate.

Under Path C:

- Packaging remains unimplemented.
- Phase C does not start.
- The frozen architecture and dependency tuple remain unchanged.
- No MinIO version or object-store substitution is considered.
- The cached images remain local evidence only.
- The packaging initiative stops pending a later owner decision.

## 7. Exact Acceptance Requirements for the Selected Path

### Path A acceptance requirements

For **both** images, a later bounded re-verification must record and verify:

- Authorized source identity and exact repository/reference.
- Exact manifest or index digest equal to the governing frozen digest.
- Image config identity.
- Complete layer set and layer content acquisition.
- Linux/amd64 platform.
- Successful fresh pull, or an independently documented equivalent fresh acquisition.
- Successful local runtime execution of the exact server and `mc` version probes.
- Release/version and commit identity.
- No use of a retained application volume or retained OIP runtime as acquisition evidence.

The result must establish digest equality with both governing frozen digests. A tag-only result, binary-only result, cached-only result or matching filename is insufficient.

### Path B acceptance requirements

Before the archive is created, the owner must have explicitly selected Path B. The later bounded phase must record and verify:

- Archive source and provenance.
- Both exact frozen image references.
- Source image IDs and config identities.
- Governing RepoDigests.
- Manifest/index identity where available.
- Complete layer set.
- Archive SHA256.
- Linux/amd64 platform.
- Transfer/access procedure usable by a clean runtime.
- Successful load into an isolated Docker/image state without relying on the original warm cache.
- Post-load image IDs, config identities and digest equality with both governing digests.
- Exact server and `mc` version probes.
- No substitution, source rebuild, MinIO upgrade or object-store change.

The archive must not be declared reproducible until independent load and identity verification passes for both images.

### Common requirements

Even after Path A or Path B is selected, this gate does not authorize Phase C. A separate artifact re-verification must first produce a resolved outcome. Packaging implementation requires its own explicit authorization after that outcome.

## 8. Explicit Non-Goals

This gate does not:

- Create or load a sealed archive.
- Export or repackage cached Docker images.
- Build or rebuild MinIO.
- Download or evaluate another MinIO/AIStor release.
- Substitute another object-storage server.
- Create Dockerfiles, Compose overlays, launch scripts, packaging manifests or packaging tests.
- Modify `docker-compose.yml`, requirements, dependency pins, `.env`, application code, tests, contracts or reports other than this report.
- Start or restart Kafka, Schema Registry, MinIO, PostgreSQL, Redis, Spark, the dashboard, the exporter or any retained OIP service.
- Run the M1–M9 acceptance suite or runtime certification.
- Create M10, CI/CD or a deployment directory.
- Claim that packaging is reproducible.

## 9. Current Project State

- **Packaging:** NOT implemented.
- **Phase C:** NOT started.
- **Frozen architecture:** unchanged.
- **Frozen files:** unchanged by this gate; only this report is being added.
- **OIP containers:** all nine retained OIP containers remain stopped.
- **Docker images:** the two cached MinIO image identities remain unchanged; no image pull, rebuild, export, load or tag operation was performed by this gate.
- **Project artifact addition:** `artifact_authorization_decision_gate_report.md` only.

The current Docker engine remains Linux/amd64 on the `desktop-linux` context. Its reachability does not resolve the missing authorized complete-image source.

The current pre-gate inventory recorded 1,065 regular files, 13,142,062 bytes and aggregate SHA256 `26d54c93823fe45a0efe59cc3d0edb051ed3d72461ca520e90047f31e3fdd6be`, excluding `.venv`, `.git`, `__pycache__`, `.pytest_cache` and `.ruff_cache`. This includes the previously authorized reports and five harness-managed `.remember` files present before this gate. The historical frozen application inventory and prior report hashes remain preserved as documented in the artifact-unblock report. The post-gate comparison uses this current pre-gate inventory and excludes only this new decision report.

## 10. Final Gate Status

**BLOCKED**

**BLOCKED — OWNER DECISION REQUIRED.**

No explicit Path A or Path B authorization is present in the current inputs. Path C therefore applies by default: stop at the artifact gate.

The project is not authorized to proceed to bounded artifact re-verification, sealed-archive creation, or Phase C until the owner makes and records an explicit decision. No claim of MinIO unblocking or packaging reproducibility is made.
