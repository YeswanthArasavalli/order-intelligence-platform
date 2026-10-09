# Phase B — Artifact Availability Gate

**Audit date:** 2026-10-05 UTC  
**Workstream:** Reproducible Local Runtime Packaging & Launch Verification — unnumbered  
**Authority:** approved Phase B scope, completed Phase A inspection, frozen M1–M9 contracts and reports, and `post_m9_roadmap_gate_report.md` §§11–12  
**Verdict:** **BLOCKED — exact frozen MinIO server and initializer-client acquisition is not established.**

## 1. Executive Summary

Docker Desktop's Linux engine is now reachable; the engine-pipe failure observed during Phase A is resolved. The selected artifact target is **Linux/amd64**, controlled from the existing WSL workspace through `docker.exe` and Docker Desktop's `desktop-linux` context.

Actual digest pulls succeeded for Apache Spark, Kafka, Schema Registry, PostgreSQL, Redis, Prometheus, Grafana and optional Kafka UI. These eight pulls returned **“Image is up to date”**: remote digest access was tested, but their locally cached layers were reused. Exact Python 3.11.9 and 3.11.17 base-image pulls returned **“Downloaded newer image”** and their measured digests are recorded below.

The frozen Spark tuple was confirmed inside a disposable container. A separate empty Ivy cache resolved and downloaded **18 unique JARs, with zero evictions**, including all five frozen direct coordinates and their runtime transitive closure. PySpark 3.5.9's **317,858,593-byte** source distribution was actually downloaded and matched its published SHA256.

No-cache, download-only Python dependency checks succeeded for all 26 non-PySpark root pins on Python 3.11.9 (**82 wheels**) and Spark's Python 3.10.12 (**85 wheels**), and for all six exporter pins on Python 3.11.17 (**14 wheels**). These are acquisition/resolution results, not application installation, build or regression results.

The two required MinIO images exist locally and report the exact frozen releases. However, **both release-tag pulls and both digest pulls failed** from their existing Docker Hub sources. An approved independently obtainable exact-artifact source or sealed archive has not been supplied. A warm cached copy therefore cannot satisfy the clean-runtime gate.

**Phase C is not allowed while that blocker remains.** This report completes an availability gate, not packaging certification. There is no M10 designation.

## 2. Phase B Scope

This phase inventories existing artifacts, tests registry/package/Maven acquisition, inspects disposable runtime versions, assesses frozen-interface feasibility, and checks project-file integrity. The sole new project artifact is this report.

All existing production modules, schemas, contracts, tests, verifiers, reports/evidence, dependency files, Compose/Docker configuration, README, PHASE_STATUS and active `.env` are frozen inputs. No project dependency installation or update was performed. No Dockerfile, overlay, launcher, wrapper, helper script, lock, packaging manifest or packaging test was added.

Probes used inline commands and disposable `docker run --rm` containers without project or retained-data mounts. Downloads and resolver caches were confined to disposable container filesystems; the large PySpark download was streamed and hashed. Image pulls intentionally added the two exact Python bases to Docker's image store. No retained OIP service was started, rebuilt, reset or attached to the probes. Credentials and full container environments were not printed.

The historical pycairo/PyGObject warning in the retained verifier was not repaired or used as a prerequisite. Existing business tests, runtime pipelines, acceptance fixtures and restart scenarios were not executed during this acquisition-only phase.

## 3. Docker Engine Status

**Status: PASS — reachable.**

| Item | Observed value |
|---|---|
| Docker CLI | 29.6.2, API 1.55, Windows/amd64 |
| Docker Desktop | 4.84.0 (234817) |
| Docker Engine | 29.6.2, API 1.55, Linux/amd64 |
| containerd / runc / docker-init | 2.2.5 / 1.3.6 / 0.19.0 |
| Compose | v5.3.1 |
| Active context | `desktop-linux` |
| Engine endpoint | `npipe:////./pipe/dockerDesktopLinuxEngine` |
| Engine architecture reported by info | `x86_64` |
| Images, initial → final | 36 → 38 |
| Containers, initial → final | 21 → 21 |

The final container listing still shows all nine retained OIP containers exited: exporter and verifier exit137; Registry and Kafka exit143; MinIO, PostgreSQL, Redis, Prometheus and Grafana exit0. Those historical exit codes were not produced by this gate. Disposable probes left no additional containers. Unrelated Supabase services were not operated on.

This is a Linux/amd64 artifact result. Other architectures and an independent second machine were not tested.

## 4. Artifact Inventory

### Existing runtime roles and minimum dependency boundaries

| Role | Frozen interface / inputs | Required artifact families |
|---|---|---|
| Infrastructure | Existing Compose service contracts | Kafka7.7.12, Registry7.7.12, exact MinIO server and `mc`, PostgreSQL16.14, Redis7.4.9 |
| Producer / Avro | Existing producer CLI, generator and `producer/schemas/order_event.avsc` | Python3.11.9; Kafka/Registry clients, Fastavro, Authlib, Faker, configuration/logging dependencies |
| Bronze / Silver / DLQ / Gold | Existing Spark streaming APIs and bounded verifier drains | Exact Apache Spark base, matching driver/worker Python3.10.12, Java17, Scala2.12, frozen Kafka/Avro/S3A JAR tuple; Kafka/Registry/MinIO |
| M4 SQL serving | `storage.gold_postgres_serving.serve_current_gold()` | Spark role plus PostgreSQL JDBC42.7.3, Python SQL/S3 dependencies, MinIO and PostgreSQL; API-only interface requires an additive invocation wrapper later |
| M5 Redis publication | `python -m storage.postgres_redis_serving` | Non-Spark Python3.11.9, SQLAlchemy/Psycopg/Redis/configuration/logging; one-shot invocation |
| M6 dashboard | `python -m streamlit run dashboard/app.py` | Python3.11.9, Streamlit1.40.1, Pandas2.2.3, Plotly5.24.1, SQL/Redis read path |
| M7 evaluation | `python -m alerts.evaluator --rules alerts/rules.example.json` | Python3.11.9, PostgreSQL/configuration/logging and explicit rule file; one-shot invocation |
| M8 controlled notification | `python -m notifications.service`, existing controlled receiver | Python3.11.9, PostgreSQL/HTTP/configuration/logging, stdlib HTTP and SQLite; shared permitted loopback namespace, durable receiver ledger |
| M9 observation | Frozen exporter source/Dockerfile, scrape and provisioning files | Python3.11.17, six exporter pins, Prometheus2.54.1, Grafana11.3.0; opt-in telemetry |
| Verification / development | Existing tests/verifiers only | Frozen Pytest/Cov/Asyncio/Mock, Ruff and Black pins; no new test framework |
| Optional topic inspection | Existing Kafka UI service | `provectuslabs/kafka-ui:v0.7.2`; not required by the business pipeline |

The entire root pin set was checked rather than inventing new per-role requirement files. It has **27 direct pins**, including PySpark. The download-only wheel probes use the other26; the exact PySpark sdist and its Py4J dependency artifacts were checked separately. The exporter has six direct pins. No new runtime role or scheduling behavior was defined.

Additional required local inputs are the frozen Avro schema, DQ/Gold/serving/alert/notification contracts, SQL initialization, explicit rules, Prometheus configuration and Grafana provisioning/dashboard JSON. Their provenance is the existing workspace and frozen reports; the all-file integrity audit covers them. Runtime credentials are execution inputs, not image/dependency artifacts.

## 5. Image Provenance

Full measured digests appear in §12. All inspected images are Linux/amd64. For existing infra images, targeted `image inspect` supplied local repository digests; actual digest pulls then tested remote acquisition independently of the Compose tags.

| Repository / frozen tag | Provenance and acquisition evidence |
|---|---|
| `apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu` | Existing Apache distribution digest; Docker Hub digest pull succeeded; runtime tuple inspected without retained verifier |
| `confluentinc/cp-kafka:7.7.12` | Existing Confluent image digest; Docker Hub digest pull succeeded |
| `confluentinc/cp-schema-registry:7.7.12` | Existing Confluent image digest; Docker Hub digest pull succeeded |
| `postgres:16-alpine` | Mutable original tag; measured retained digest pulled successfully; `postgres --version` confirmed16.14 |
| `redis:7-alpine` | Mutable original tag; measured retained digest pulled successfully; `redis-server --version` confirmed7.4.9 |
| `minio/minio:RELEASE.2024-11-07T00-52-20Z` | Cached digest and binary release known; exact tag and digest pulls failed |
| `minio/mc:RELEASE.2024-11-05T11-29-45Z` | Cached digest and binary release known; exact tag and digest pulls failed |
| `python:3.11.9-slim-bookworm` | Exact patch tag actually pulled; digest measured; Python3.11.9/SQLite3.40.1 inspected |
| `python:3.11.17-slim-bookworm` | Exact patch tag actually pulled; digest measured; Python3.11.17/SQLite3.40.1 inspected |
| `prom/prometheus:v2.54.1` | Frozen M9 version; measured digest pull succeeded |
| `grafana/grafana:11.3.0` | Frozen M9 version; measured digest pull succeeded |
| `provectuslabs/kafka-ui:v0.7.2` | Existing optional UI image; measured digest pull succeeded |
| `oip-metrics-exporter:m9` | Existing local build, not an established published remote image. Its repository digest/image identity alone does not prove remote availability. Frozen source, Dockerfile and dependency inputs are present; rebuilding them is untested here. |

The eight “up to date” pulls prove remote digest resolution/access at the audit time, not a network transfer of every cached layer. The Python pulls show actual acquisition and layer-download output; this report does not assert that every underlying layer was previously absent.

Future additive packaging must retain the measured PostgreSQL/Redis digests and explicit Python patch versions. The existing exporter Dockerfile's `python:3.11-slim-bookworm` tag is mutable; it is not a permanent Python3.11.17 lock. Existing files were not repinned during this phase.

## 6. MinIO Investigation

**Status: BLOCKED for both required image artifacts.**

| Artifact | Exact locally confirmed release | Local measured digest | Cached version-probe evidence |
|---|---|---|---|
| Server | `RELEASE.2024-11-07T00-52-20Z` | `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` | commit `cefc43e4daa4cbb490ef6726ea374e26a93eb85e`, Go1.23.3, Linux/amd64 |
| Initializer client | `RELEASE.2024-11-05T11-29-45Z` | `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` | commit `6ac18619cf881074fe6edcc79ab62c9c85da60b9`, Go1.23.3, Linux/amd64 |

Actual Docker pulls were attempted for each image's release tag and measured digest. The server attempts returned:

```text
Error response from daemon: pull access denied for minio/minio, repository does not exist or may require 'docker login'
```

The client attempts returned:

```text
Error response from daemon: pull access denied for minio/mc, repository does not exist or may require 'docker login'
```

Current Docker Hub tag-endpoint fetches also returned404:

- `https://hub.docker.com/v2/repositories/minio/minio/tags/RELEASE.2024-11-07T00-52-20Z`
- `https://hub.docker.com/v2/repositories/minio/mc/tags/RELEASE.2024-11-05T11-29-45Z`

These failures establish lack of demonstrated acquisition from the current sources under the current access conditions. They do not establish a universal registry outage, identify deletion versus access-policy changes, or rule out every authorized alternative source. No credentials/login, alternate version, substitute object store or unapproved archive was used.

Cached `--version` probes succeed but do not fix the acquisition failure. `mc` is required by the frozen bucket initializer for `bronze`, `silver`, `gold` and `checkpoints`; it cannot simply be omitted to declare the existing initialization procedure reproducible.

**Required decision:** authorize a legitimate exact-artifact distribution source or explicitly adopt a sealed, checksummed archive with independently verified provenance and clean-machine access for **both** images, or resolve access to their exact existing registry artifacts. No such archive/source was supplied or approved in this phase. Creating an archive from the undocumented warm cache automatically would not satisfy this decision gate.

## 7. Spark/JAR Dependency Verification

**Availability status: PASS for the frozen tuple and tested Maven runtime closure. Application execution remains untested.**

Inside the exact Apache Spark digest, read-only inspection confirmed:

- PySpark/Spark **3.5.9** and Python **3.10.12**.
- Java **17.0.19**, Temurin **17.0.19+10**.
- `scala-library-2.12.18.jar`, establishing the frozen Scala2.12 family.
- `hadoop-client-api-3.3.4.jar` and `hadoop-client-runtime-3.3.4.jar`.
- Spark's existing `ivy-2.5.3.jar` resolver.

The exact five coordinates in `streaming/spark_session.py` were supplied to Spark's package resolver in a disposable container. `/tmp/artifact-gate-ivy` was an empty container-only cache; no project, retained Ivy directory or verification-container cache was mounted. Maven Central was the source: actual download logs used `https://repo1.maven.org/maven2/`; `https://repo.maven.apache.org/maven2` was also explicitly supplied as a repository and its five direct POMs returnedHTTP200.

Ivy reported **18 modules searched/downloaded, 18 artifacts downloaded, zero evictions; 18 copied, zero already retrieved**. Hash enumeration saw36 paths because each of the18 unique JARs appeared in both the cache and retrieval directory. The table records each unique artifact once.

| Coordinate | Downloaded bytes | SHA256 of actual JAR bytes | Status |
|---|---:|---|---|
| `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9` | 432716 | `fed393dcc3ef70611fe3d4f35ae78a8ea7c55301fd4cdd66c5dc97fcfaa5a00f` | PASS — direct |
| `org.apache.spark:spark-avro_2.12:3.5.9` | 207272 | `9dda7512c1052bcd3774cbe247572cd481d4481eb8a90b7c4865b23a12bbecf8` | PASS — direct |
| `org.apache.hadoop:hadoop-aws:3.3.4` | 962685 | `53f9ae03c681a30a50aa17524bd9790ab596b28481858e54efd989a826ed3a4a` | PASS — direct |
| `com.amazonaws:aws-java-sdk-bundle:1.12.262` | 280645251 | `873fe7cf495126619997bec21c44de5d992544aea7e632fdc77adb1a0915bae5` | PASS — direct |
| `org.postgresql:postgresql:42.7.3` | 1089312 | `a2644cbfba1baa145ff7e8c8ef582a6eed7a7ec4ca792f7f054122bdec756268` | PASS — direct |
| `org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.9` | 56812 | `e82612e6070438c5ea48b08c74f367a6d214a390bdf6dcf3ff316dca592ddda5` | PASS — transitive |
| `org.apache.kafka:kafka-clients:3.4.1` | 5050443 | `9d0a9058c8ede79db6e54ae378ed16fe8d1bead2894635a4aa6aa0b7981668c6` | PASS — transitive |
| `org.lz4:lz4-java:1.8.0` | 682804 | `d74a3334fb35195009b338a951f918203d6bbca3d1d359033dc33edd1cadc9ef` | PASS — transitive |
| `org.xerial.snappy:snappy-java:1.1.10.5` | 2299797 | `0f3f1857ed33116583f480b4df5c0218836c47bfbc9c6221c0d73f356decf37b` | PASS — transitive |
| `org.slf4j:slf4j-api:2.0.7` | 63635 | `5d6298b93a1905c32cda6478808ac14c2d4a47e91535e53c41f7feeb85d946f4` | PASS — transitive |
| `org.apache.hadoop:hadoop-client-api:3.3.4` | 19458635 | `e513d71b78086b5caaa439f4402b43e20df01446d56b66084ad419452878701c` | PASS — transitive/bundled family |
| `org.apache.hadoop:hadoop-client-runtime:3.3.4` | 30085504 | `9377a68071137ae5f5c8cdc2ed20d6f904a1df4d06df26b47ed8872a1b0d8d47` | PASS — transitive/bundled family |
| `commons-logging:commons-logging:1.1.3` | 62050 | `70903f6fc82e9908c8da9f20443f61d90f0870a312642991fe8462a0b9391784` | PASS — transitive |
| `com.google.code.findbugs:jsr305:3.0.0` | 33031 | `bec0b24dcb23f9670172724826584802b80ae6cbdaba03bdebdef9327b962f6a` | PASS — transitive |
| `org.apache.commons:commons-pool2:2.11.1` | 145516 | `ea0505ee7515e58b1ac0e686e4d1a5d9f7d808e251a61bc371aa0595b9963f83` | PASS — transitive |
| `org.tukaani:xz:1.9` | 116013 | `211b306cfc44f8f96df3a0a3ddaf75ba8c5289eed77d60d72f889bb855f535e5` | PASS — transitive |
| `org.wildfly.openssl:wildfly-openssl:1.0.7.Final` | 424076 | `9f7058778978ec2ad90c7224401a4b0013d93e0dd3fa0f72b25244ffa3507ca8` | PASS — transitive |
| `org.checkerframework:checker-qual:3.42.0` | 230830 | `ccaedd33af0b7894d9f2f3b644f4d19e43928e32902e61ac4d10777830f5aac7` | PASS — transitive |

The direct POM SHA256 observations, in the same five-coordinate order, were:

```text
63b5534b10b7932dfe88e0abebca57110a7fdc2cc32e606a05e4826c14edf68c
333b2031af2386a9681c9237be1fc4208ac5910d206d5b9fbbb9a3da9bb2cff7
89d8f44c5749cf8c67f2938bb6799556cae62adad8b7e040f2c5d33780e112d6
dbef1a3dd1bda9533d3a6dbc3aec745a5b8d32ad853225a3e0fe9f51cbfbdd83
c2acb68457d996a8e49ed0777e59255b74650976523951f101adb94075e15885
```

Spark gateway/dependency initialization exited successfully. Its native-Hadoop loader warning fell back to built-in Java classes and was not a dependency-resolution failure. No project SparkSession, Kafka consumption, S3A write, streaming job or JDBC serving operation was run. Classpath/application compatibility and fresh pipeline execution require later verification; artifact acquisition does not certify them.

## 8. Python Runtime Dependency Verification

### Runtime and resolver evidence

| Probe | Frozen direct inputs checked | Actual downloaded wheel files | pip | Result / inventory SHA256 |
|---|---:|---:|---|---|
| Python3.11.9 core base | 26 root non-PySpark pins | 82 | 24.0 | exit0; `6611329d1a6accea3f41b853db0ae0ae3096b98fb8c93b4dc135e64e53d1997f` |
| Spark Python3.10.12 base | Same26 pins | 85 | 22.0.2 | exit0; `acbad188d42059c64a2393d76cae0186c3fccf5e17b45c70910eb7f75cd43504` |
| Python3.11.17 exporter base | Six exporter pins | 14 | 24.0 | exit0; `a5eb3bffd0d55aeb63aa6a2014bfaa6426c939d02628695976410fe9ee32c6ba` |

Each download command used `--no-cache-dir --only-binary=:all: --index-url https://pypi.org/simple` and a new container-only temporary destination. All direct pins retained their exact versions. No package installation or pip upgrade occurred. The existing pip versions supplied by the bases were used.

Inventory digests are SHA256 of the filename-sorted list of dictionaries containing `filename`, `bytes` and each file's measured `sha256`, serialized with `json.dumps(..., separators=(',', ':'), sort_keys=True)`. They fingerprint the downloaded candidate closure; they are **not an individual-hash transitive lock**. Initial verbose download output was truncated; the final concise reruns above explicitly captured exit0, file counts, direct hashes and transitive filenames. A final metadata cross-check verified that every reported direct-wheel hash in all26 rows also belongs to a published wheel for that exact PyPI package/version.

### Frozen direct wheel checksums

For universal wheels, “same” means the identical measured Python3.11 SHA256. Native-wheel hashes below refer to the actually selected Linux/x86_64 `cp311` or `cp310` wheel. This table also covers all six exporter direct hashes on its Python3.11.17 base.

| Exact direct pin | Python3.11 wheel SHA256 | Python3.10 wheel SHA256 | Availability |
|---|---|---|---|
| `confluent-kafka[schemaregistry]==2.15.1` | `44b849e0b375ff4c7a2e5e7af27a48d808933b53f3a8bf8e84b5c81eef9463ec` | `c410e97bc41bc54f88a258324673bcc4f32531d9bce0fccac8c91ecd4e2327ae` | PASS |
| `fastavro==1.12.2` | `2b73d50978d5e57416fa68461f9f3c8f39ea39e761cb1e12f919745adefe26a7` | `731aefe6c4bf2bafa0798ef83927676d06e44d1d18202cfb56d63b40422ab900` | PASS |
| `authlib==1.8.0` | `88aebbd9af6757e14e912d5dc007ae1dc1f3e27e3b2152ce7c552ee2c3b3c121` | same | PASS |
| `boto3==1.35.62` | `e6574047701ab009c2b2bb17b530a3a2fb34de8698b77f8bbb34dd0c9286c117` | same | PASS |
| `botocore==1.35.62` | `4c3960a33289371d96eba5116364c41e6b848b5afbed3a43f5d8c7ba36f55e1d` | same | PASS |
| `psycopg2-binary==2.9.10` | `512d29bb12608891e349af6a0cccedce51677725a921c07dba6342beaf576f9a` | `7f4152f8f76d2023aac16285576a9ecd2b11a9895373a1f10fd9db54b3ff06b4` | PASS |
| `sqlalchemy==2.0.36` | `2519f3a5d0517fc159afab1015e54bb81b4406c278749779be57a569d8d1bb0d` | `c245b1fbade9c35e5bd3b64270ab49ce990369018289ecfde3f9c318411aaa07` | PASS |
| `redis==5.2.0` | `ae174f2bb3b1bf2b09d54bf3e51fbc1469cf6c10aa03e21141f51969801a7897` | same | PASS |
| `streamlit==1.40.1` | `b9d7a317a0cc88edd7857c7e07dde9cf95647d3ae51cbfa8a3db82fbb8a2990d` | same | PASS |
| `plotly==5.24.1` | `f67073a1e637eb0dc3e46324d9d51e2fe76e9727c892dde64ddf1e1b51f29089` | same | PASS |
| `pandas==2.2.3` | `c124333816c3a9b03fbeef3a9f230ba9a737e9e5bb4060aa2107a86cc0a497fc` | `86976a1c5b25ae3f8ccae3a5306e443569ee3c3faf444dfd0f41cda24667ad57` | PASS |
| `faker==31.0.0` | `691000b3f589b0f1145e124d3f10262f5c08ef20a736d85e03ab3745f8e9e458` | same | PASS |
| `prometheus-client==0.21.0` | `4fa6b4dd0ac16d58bb587c04b1caae65b8c5043e85f778f42f5f632f6af2e166` | same | PASS |
| `requests==2.34.2` | `2a0d60c172f83ac6ab31e4554906c0f3b3588d37b5cb939b1c061f4907e278e0` | same | PASS |
| `httpx==0.27.2` | `7bb2708e112d8fdd7829cd4243970f0c223274051cb35ee80c03301ee29a3df0` | same | PASS |
| `tenacity==9.0.0` | `93de0c98785b27fcf659856aa9f54bfbd399e29969b0621bc7f762bd441b4539` | same | PASS |
| `python-dotenv==1.0.1` | `f7b63ef50f1b690dddf550d03497b66d609393b40b564ed0d674909a68ebf16a` | same | PASS |
| `pydantic==2.9.2` | `f048cec7b26778210e28a0459867920654d48e5e62db0958433636cde4254f12` | same | PASS |
| `pydantic-settings==2.6.1` | `7fb0637c786a558d3103436278a7c4f1cfd29ba8973238a50c5bb9a55387da87` | same | PASS |
| `structlog==24.4.0` | `597f61e80a91cc0749a9fd2a098ed76715a1c8a01f73e336b746504d1aad7610` | same | PASS |
| `pytest==8.3.3` | `a6853c7375b2663155079443d2e45de913a911a11d669df02a50814944db57b2` | same | PASS |
| `pytest-cov==6.0.0` | `eee6f1b9e61008bd34975a4d5bab25801eb31898b032dd55addc93e96fcaaa35` | same | PASS |
| `pytest-asyncio==0.24.0` | `a811296ed596b69bf0b6f3dc40f83bcaf341b155a269052d82efa2b25ac7037b` | same | PASS |
| `pytest-mock==3.14.0` | `0b72c38033392a5f4621342fe11e9219ac11ec9d375f8e2a0c164539e0d70f6f` | same | PASS |
| `ruff==0.7.4` | `0e92dfb5f00eaedb1501b2f906ccabfd67b2355bdf117fea9719fc99ac2145bc` | same | PASS |
| `black==24.10.0` | `4007b1393d902b48b36958a216c20c4482f601569d19ed1df294a496eb366392` | `649fff99a20bd06c6f727d2a27f401331dc0cc861fb69cde910fe95b01b5928f` | PASS |

### Observed transitive candidates — not frozen installed versions

The following54 package/version pairs were common to the two root-wheel resolutions; their wheel ABIs can differ between Python versions:

```text
altair==5.5.0; annotated-types==0.8.0; anyio==4.15.1; attrs==26.1.0;
blinker==1.9.0; cachetools==5.5.2; certifi==2026.7.22; cffi==2.1.1;
charset-normalizer==3.5.2; click==8.5.0; coverage==7.16.2; cryptography==50.0.2;
gitdb==4.0.12; gitpython==3.2.0; greenlet==3.5.6; h11==0.16.0;
httpcore==1.0.9; idna==3.20; iniconfig==2.3.0; jinja2==3.1.6;
jmespath==1.1.0; joserfc==1.7.5; jsonschema==4.26.0;
jsonschema-specifications==2025.9.1; markdown-it-py==4.2.0; markupsafe==3.0.4;
mdurl==0.1.2; mypy-extensions==1.1.0; narwhals==2.26.0; packaging==24.2;
pathspec==1.1.1; pillow==11.3.0; platformdirs==4.12.3; pluggy==1.6.0;
protobuf==5.29.6; pyarrow==25.0.1; pycparser==3.0; pydantic-core==2.23.4;
pydeck==0.9.3; pygments==2.21.0; python-dateutil==2.9.0.post0; pytz==2026.5;
referencing==0.37.0; rich==13.9.4; s3transfer==0.10.4; six==1.17.0;
smmap==5.0.3; sniffio==1.3.1; toml==0.10.2; tornado==6.5.10;
typing-extensions==4.16.0; tzdata==2026.5; urllib3==2.8.0; watchdog==6.0.0
```

| Difference | Python3.11.9 root probe | Python3.10.12 root probe |
|---|---|---|
| NumPy | 2.4.6 | 2.2.6 |
| rpds-py | 2026.9.1 | 0.30.0 |
| Additional conditional dependencies | none of these three | async-timeout5.0.1, exceptiongroup1.3.1, tomli2.4.1 |
| Total transitive wheels | 56 | 59 |

The exporter resolved eight transitive wheels in addition to its six direct ones. Their individual measured hashes were:

| Exact transitive wheel | SHA256 |
|---|---|
| `anyio-4.15.1-py3-none-any.whl` | `6152fdbbf9a77fdec97731721bebf7c4c44f7c29b424b0065826173efc7ed101` |
| `certifi-2026.7.22-py3-none-any.whl` | `62f22742b58a1a33014a2b6b706588a8d7e2a88ae7bd1a6ebe8c992928483775` |
| `greenlet-3.5.6-cp311-cp311-manylinux_2_24_x86_64.manylinux_2_28_x86_64.whl` | `1c20ea32a73d17b9b60e3371240e17b0068120c98a5ec01a224a7dd8c89733ba` |
| `h11-0.16.0-py3-none-any.whl` | `63cf8bbe7522de3bf65932fda1d9c2772064ffb3dae62d55932da54b31cb6c86` |
| `httpcore-1.0.9-py3-none-any.whl` | `2d400746a40668fc9dec9810239072b40b4484b640a8c38fd654a024c7a1bf55` |
| `idna-3.20-py3-none-any.whl` | `ab7ae7122974553370f0bdb919e1a960b2cd1bc1ef0276416d896db81c14582c` |
| `sniffio-1.3.1-py3-none-any.whl` | `2f6da418d1f1e0fddd844478f41680e794e6051915791a034ff65e5f100525a2` |
| `typing_extensions-4.16.0-py3-none-any.whl` | `481caa481374e813c1b176ada14e97f1f67a4539ce9cfeb3f350d78d6370c2e8` |

These newly resolved transitives were downloaded, not installed as upgrades. Root requirements explicitly permit pip to resolve transitives. Resolver success does not prove these candidates match the old verifier's installed closure or pass its business/runtime oracles. Individual checksummed packaging locks and compatibility verification belong to a later authorized phase.

### PySpark distribution and Py4J

- PySpark metadata: `https://pypi.org/pypi/pyspark/3.5.9/json`, HTTP200, Python requirement`>=3.8`.
- Actual source URL: `https://files.pythonhosted.org/packages/95/ce/81e53e729790556e3983e95de1a7d5df91a34adfcd34b5a5ab0e0c6e9b33/pyspark-3.5.9.tar.gz`.
- Actual download: **317858593 bytes**, SHA256 **`ea27adc39ddac9413b8951e45aa748cbed6c785971b81386efc41938f6243d93`**; byte count and digest matched PyPI metadata.
- Base requirement: `py4j>=0.10.9.7,<0.10.9.10`. Optional ML/SQL/Connect extras were not requested by the frozen root pin.
- Py4J0.10.9.7 wheel: **200481 bytes**, SHA256 **`85defdfd2b2376eb3abf5ca6474b51ab7e0de341c75a02f46dc9b5976f5a5c1b`**; actual bytes matched PyPI metadata. The Spark-image gateway probe used its bundled0.10.9.7 bridge.
- Py4J0.10.9.9 wheel: **203008 bytes**, SHA256 **`c7c26e4158defb37b0bb124933163641a2ff6e3a3913f7811b0ddbe07ed61533`**; actual bytes matched PyPI metadata. Its availability is recorded because the PySpark metadata permits it; no replacement of Spark's bundled bridge was made or certified.

The sdist's acquisition is PASS. Constructing/installing a wheel from that sdist, an all27-pin installation, and application imports on that installation were not tested. The exact existing Spark image independently supplies the inspected Spark/PySpark runtime.

## 9. Fresh-Runtime Reproducibility Assessment

| Boundary | Assessment |
|---|---|
| Declared local Docker target | PASS — reachable Linux/amd64 engine and Compose |
| Required non-MinIO image acquisition | PASS for tested remote digest access; cached layers qualification retained |
| Exact Python base acquisition | PASS — actual pulls, patch-version inspection |
| Frozen Spark/Scala/Hadoop/SDK/JDBC tuple | PASS for artifact/version and empty-cache resolution |
| Frozen Python direct pins and candidate closures | PASS for download/resolution; installed/runtime compatibility unverified |
| MinIO server/client clean acquisition | BLOCKED — exact approved-source pulls fail; no approved archive/source |
| Core application image build | NOT RUN — existing source is present; new packaging recipe belongs to Phase C |
| Exporter source rebuild | NOT RUN — local built image is not remote acquisition proof |
| Fresh isolated fixtures, complete launch and restart | NOT RUN — beyond Phase B and currently blocked by MinIO acquisition |

The retained verification container and its caches were never used by these probes. Nevertheless, a clean-machine launch cannot currently be asserted because two required infra artifacts lack an established acquisition path. A successful cached binary probe is not equivalent to reproducibility from an approved source.

For later isolation, changing only the Compose project name is insufficient: existing Compose declares explicit `oip-*` network/container/volume names. A fresh deployment must independently name its resources and isolated topics/lake/checkpoint roots/database/Redis namespace, while retaining frozen schema restrictions such as `gold_serving` and `alerts_serving`. Registry state is Kafka-backed and must be preserved with the isolated Kafka state. An extra Registry data volume is not an inherent requirement.

M4 remains a Spark-dependent Python API; M5/M7/M8 remain manual one-shot operations. Existing capture wrappers with hardcoded `oip-modernization-verify` and historical topics are not clean launch primitives. This gate did not convert them into packaging tools.

## 10. M8 Feasibility

**Artifact/interface assessment: feasible conditionally; fresh delivery/restart evidence not run.**

The exact Python3.11.9 base includes stdlib HTTP, threading and **SQLite3.40.1**, and the frozen SQL/HTTP direct dependencies are obtainable. `scripts/milestone8_receiver.py` supplies the existing receiver API and durable SQLite receipt ledger; it has no standalone receiver CLI. An additive invocation mechanism can call the existing interface later without changing provider semantics.

The provider requires **`http://127.0.0.1:<port>/notify`**. Processor and receiver must therefore share the permitted network namespace. A separate receiver container addressed by a Docker service hostname would fail the frozen validation. Namespace sharing/co-location is a future packaging requirement, not an implementation made here.

The receiver ledger must be mounted durably and survive runtime recreation. Processor execution remains explicit one-shot invocation; retries/leases/identity/dedup/source binding remain frozen. Fresh M8 test fixtures require exactly two newly established M7 events in `alerts_serving`; old default events and `/tmp/omnirush/m8-20261004a.sqlite` must not be used to manufacture a clean pass. The previously qualified timing-sensitive test must remain unchanged and run sequentially when authorized.

No loopback request, notification attempt, receipt, database mutation or ledger-restart test was generated in Phase B. There is no additional missing M8-specific binary artifact identified, but full end-to-end verification remains behind the overall MinIO gate.

## 11. M9 Feasibility

**Artifact/interface assessment: feasible conditionally; fresh source build, live scrapes and dashboard queries not run.**

Python3.11.17 and all six exporter direct dependencies plus their14-wheel closure are obtainable. Prometheus2.54.1 and Grafana11.3.0 exact digests are remotely accessible. Frozen exporter/configuration/provisioning/dashboard source is present and integrity-covered. The local `oip-metrics-exporter:m9` image was inspected but not treated as an independently published dependency.

Later additive packaging must:

- Use the exact Python patch/base digest rather than assume the frozen Dockerfile's mutable tag still identifies it.
- Keep telemetry opt-in via `OIP_TELEMETRY_URL`, fail-open, with exporter absence unable to gate business success.
- Preserve the isolated-network service targets `metrics-exporter`, `prometheus`, `grafana`, three scrape jobs, and Grafana datasource UID **`prometheus-oip`** with `http://prometheus:9090` in that network.
- Supply **`OIP_DASHBOARD_HEALTH_URL`** for the actual packaged Streamlit health endpoint instead of the existing default `oip-modernization-verify:8501`.
- Retain the three frozen dashboards and their12/8/9 panels; do not rewrite PromQL, metric meanings or dashboard provisioning to accommodate a packaging defect.
- Preserve durable Prometheus/Grafana data where the declared local restart procedure requires it.

No source/configuration change, source image build, live Prometheus target check, Grafana provisioning request, browser check or failure/restart test was performed in this gate. Previously certified M9 results remain historical evidence, not fresh packaging evidence.

## 12. Artifact Availability Matrix

**Legend:** PASS means the stated acquisition/version probe passed, not that the application was packaged or regression-tested. PARTIAL means known source/input evidence with a stated missing build/check. BLOCKED means required clean acquisition is not established. UNKNOWN means no authorized supplied source/probe exists. “Cached” distinguishes pre-existing local copies from bases acquired during this gate.

| Artifact | Exact Version/Tag | Digest | Source | Cached | Pull/Resolution Tested | Reproducible | Status |
|---|---|---|---|---|---|---|---|
| Apache Spark runtime | `3.5.9-scala2.12-java17-python3-r-ubuntu` | `sha256:2442368ef4a5ca60e81604cf48d22d847344d009237cd21a1943b060278898bb` | `docker.io/apache/spark` | Yes | Digest pull, runtime inspection | Yes for tested digest access; cached layers | PASS |
| Kafka | `7.7.12` | `sha256:686262702020bc0513a5bfa6468bd4e3c10463d783f0569ad9ba0205ea11c4c5` | `docker.io/confluentinc/cp-kafka` | Yes | Digest pull | Yes for tested digest access; cached layers | PASS |
| Schema Registry | `7.7.12` | `sha256:44c9e2dd719aa197b26d3e6f51a8e6a48d98016782ba9e41f68d1b7b31f892d1` | `docker.io/confluentinc/cp-schema-registry` | Yes | Digest pull | Yes for tested digest access; cached layers | PASS |
| MinIO server | `RELEASE.2024-11-07T00-52-20Z` | `sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332` | `docker.io/minio/minio` | Yes | Tag/digest pulls failed; cached version probe passed | Not established from approved source | BLOCKED |
| MinIO initializer client | `RELEASE.2024-11-05T11-29-45Z` | `sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682` | `docker.io/minio/mc` | Yes | Tag/digest pulls failed; cached version probe passed | Not established from approved source | BLOCKED |
| PostgreSQL | Retained `16-alpine`, binary16.14 | `sha256:16bc17c64a573ef34162af9298258d1aec548232985b33ed7b1eac33ba35c229` | `docker.io/library/postgres` | Yes | Digest pull, binary version | Yes by measured digest; mutable tag alone insufficient | PASS |
| Redis | Retained `7-alpine`, binary7.4.9 | `sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99` | `docker.io/library/redis` | Yes | Digest pull, binary version | Yes by measured digest; mutable tag alone insufficient | PASS |
| Core Python base | `3.11.9-slim-bookworm` | `sha256:8fb099199b9f2d70342674bd9dbccd3ed03a258f26bbd1d556822c6dfc60c317` | `docker.io/library/python` | Acquired in Phase B | Tag pull, digest inspection, Python/SQLite probe | Yes, exact measured base acquired | PASS |
| Exporter Python base | `3.11.17-slim-bookworm` | `sha256:2333bd330d12de02514770b3585cad313644316047cdee24a7acfdece6de6efb` | `docker.io/library/python` | Acquired in Phase B | Tag pull, digest inspection, Python/SQLite probe | Yes, exact measured base acquired | PASS |
| Prometheus | `v2.54.1` | `sha256:f6639335d34a77d9d9db382b92eeb7fc00934be8eae81dbc03b31cfe90411a94` | `docker.io/prom/prometheus` | Yes | Digest pull | Yes for tested digest access; cached layers | PASS |
| Grafana | `11.3.0` | `sha256:a0f881232a6fb71a0554a47d0fe2203b6888fe77f4cefb7ea62bed7eb54e13c3` | `docker.io/grafana/grafana` | Yes | Digest pull | Yes for tested digest access; cached layers | PASS |
| Optional Kafka UI | `v0.7.2` | `sha256:8f2ff02d64b0a7a2b71b6b3b3148b85f66d00ec20ad40c30bdcd415d46d31818` | `docker.io/provectuslabs/kafka-ui` | Yes | Digest pull | Yes for tested digest access; cached layers | PASS |
| Local built exporter image | `oip-metrics-exporter:m9` | `sha256:36ffacf81bfcb48c3253aa9464ad37720421aeed7d29488b1574b8091783788d` | Frozen local source/build; no remote publication established | Yes | Local inspect only; rebuild not run | Source inputs known; fresh build unverified | PARTIAL |
| Spark Java / Scala / Hadoop bundled runtime | Java17.0.19+10; Scala2.12.18; Hadoop3.3.4; Python3.10.12 | Included in exact Spark image digest above | Apache Spark image | In base | Version/JAR inspection, gateway initialization | Yes as inspected base contents | PASS |
| Kafka/Avro/S3A/SDK/JDBC JAR closure | Five frozen direct pins;18 unique runtime artifacts in §7 | Individual JAR SHA256s in §7 | Maven Central | Empty disposable Ivy cache | Actual download; zero evictions | Exact tested runtime closure obtainable | PASS |
| PySpark source artifact | `3.5.9` | `sha256:ea27adc39ddac9413b8951e45aa748cbed6c785971b81386efc41938f6243d93` | PyPI / files.pythonhosted.org | No retained download cache | Actual full sdist download/hash match | Artifact obtainable; sdist build/install untested | PASS |
| Py4J bridge wheels | `0.10.9.7` and metadata-permitted`0.10.9.9` | Individual SHA256s in §8 | PyPI / files.pythonhosted.org | No retained download cache | Actual wheel downloads/hash matches | Artifacts obtainable; no bridge substitution | PASS |
| Root Python3.11 candidate wheels | Frozen26 non-PySpark direct pins;82-wheel closure | Inventory`6611329d1a6accea3f41b853db0ae0ae3096b98fb8c93b4dc135e64e53d1997f`; direct hashes in §8 | PyPI | No retained pip cache | `pip download`, exit0 | Current acquisition yes; lock/runtime compatibility pending | PASS — acquisition |
| Spark Python3.10 candidate wheels | Same26 pins;85-wheel closure | Inventory`acbad188d42059c64a2393d76cae0186c3fccf5e17b45c70910eb7f75cd43504`; direct hashes in §8 | PyPI | No retained pip cache | `pip download`, exit0 | Current acquisition yes; lock/runtime compatibility pending | PASS — acquisition |
| Exporter Python3.11 candidate wheels | Six frozen pins;14-wheel closure | Inventory`a5eb3bffd0d55aeb63aa6a2014bfaa6426c939d02628695976410fe9ee32c6ba`; individual hashes in §8 | PyPI | No retained pip cache | `pip download`, exit0 | Current acquisition yes; build/runtime compatibility pending | PASS — acquisition |
| Application/contracts/config/test source | Existing frozen M1–M9 workspace | All-existing-file aggregate in §14 | Local frozen project | Present | Before/after integrity audit | Source contents established; packaging not built | PASS — integrity input |
| Approved MinIO archive/alternate exact source | Must cover both exact frozen releases | UNKNOWN — none supplied | UNKNOWN — no authorized source supplied | Not supplied | Not tested | Not established | UNKNOWN — prerequisite to unblock |

The two BLOCKED rows prevent overall PASS. The archive UNKNOWN is an unprovided possible prerequisite, not an artifact silently accepted into the runtime. The local exporter PARTIAL is a known-source rebuild boundary, not evidence that its required base or dependency artifacts are unavailable. Future application images are outputs of packaging, not already existing remote artifacts.

## 13. Exact Commands and Evidence

Commands ran from the existing workspace; probes did not mount it into containers. Acquisition/version probes below completed successfully except the four documented MinIO pulls and the two Hub404 fetches. Download-only Python subprocess exit codes were explicitly captured as0. No success is inferred from truncated pip output.

### Docker reachability and inspection

```bash
docker.exe version
docker.exe info --format "OSType={{.OSType}} Architecture={{.Architecture}} ServerVersion={{.ServerVersion}} Images={{.Images}} Containers={{.Containers}}"
docker.exe compose version
docker.exe context show && docker.exe context ls
docker.exe image ls --digests --no-trunc
docker.exe container ls -a --format "{{.Names}} {{.Image}} {{.Status}}"
docker.exe image inspect --format "{{.Id}} {{json .RepoTags}} {{json .RepoDigests}} {{.Os}}/{{.Architecture}}" minio/minio:RELEASE.2024-11-07T00-52-20Z minio/mc:RELEASE.2024-11-05T11-29-45Z apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu confluentinc/cp-kafka:7.7.12 confluentinc/cp-schema-registry:7.7.12 postgres:16-alpine redis:7-alpine prom/prometheus:v2.54.1 grafana/grafana:11.3.0 oip-metrics-exporter:m9
docker.exe image inspect --format "{{.Id}} {{json .RepoTags}} {{json .RepoDigests}} {{.Os}}/{{.Architecture}}" provectuslabs/kafka-ui:v0.7.2
```

### Actual image acquisition attempts

```bash
docker.exe pull --platform linux/amd64 apache/spark@sha256:2442368ef4a5ca60e81604cf48d22d847344d009237cd21a1943b060278898bb
docker.exe pull --platform linux/amd64 confluentinc/cp-kafka@sha256:686262702020bc0513a5bfa6468bd4e3c10463d783f0569ad9ba0205ea11c4c5
docker.exe pull --platform linux/amd64 confluentinc/cp-schema-registry@sha256:44c9e2dd719aa197b26d3e6f51a8e6a48d98016782ba9e41f68d1b7b31f892d1
docker.exe pull --platform linux/amd64 postgres@sha256:16bc17c64a573ef34162af9298258d1aec548232985b33ed7b1eac33ba35c229
docker.exe pull --platform linux/amd64 redis@sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99
docker.exe pull --platform linux/amd64 prom/prometheus@sha256:f6639335d34a77d9d9db382b92eeb7fc00934be8eae81dbc03b31cfe90411a94
docker.exe pull --platform linux/amd64 grafana/grafana@sha256:a0f881232a6fb71a0554a47d0fe2203b6888fe77f4cefb7ea62bed7eb54e13c3
docker.exe pull --platform linux/amd64 provectuslabs/kafka-ui@sha256:8f2ff02d64b0a7a2b71b6b3b3148b85f66d00ec20ad40c30bdcd415d46d31818
docker.exe pull --platform linux/amd64 python:3.11.9-slim-bookworm
docker.exe pull --platform linux/amd64 python:3.11.17-slim-bookworm
docker.exe pull --platform linux/amd64 minio/minio@sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332
docker.exe pull --platform linux/amd64 minio/mc@sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682
docker.exe pull --platform linux/amd64 minio/minio:RELEASE.2024-11-07T00-52-20Z
docker.exe pull --platform linux/amd64 minio/mc:RELEASE.2024-11-05T11-29-45Z
```

Evidence: first eight →same measured digest and “Image is up to date”; Python bases →measured digests in §12 and “Downloaded newer image”; last four →the exact errors in §6. Docker Hub endpoints in §6 were additionally fetched through the read-only web-fetch tool and returned404.

### Disposable binary/runtime checks

```bash
docker.exe run --rm --network none --entrypoint minio minio/minio@sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332 --version
docker.exe run --rm --network none --entrypoint mc minio/mc@sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682 --version
docker.exe run --rm --network none --entrypoint postgres postgres@sha256:16bc17c64a573ef34162af9298258d1aec548232985b33ed7b1eac33ba35c229 --version
docker.exe run --rm --network none --entrypoint redis-server redis@sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99 --version
docker.exe run --rm --network none --entrypoint python3 python:3.11.9-slim-bookworm -B -c "import sys,sqlite3,json; print(json.dumps({'python':sys.version.split()[0],'sqlite':sqlite3.sqlite_version}))"
docker.exe run --rm --network none --entrypoint python3 python:3.11.17-slim-bookworm -B -c "import sys,sqlite3,json; print(json.dumps({'python':sys.version.split()[0],'sqlite':sqlite3.sqlite_version}))"
docker.exe run --rm --network none --entrypoint python3 apache/spark@sha256:2442368ef4a5ca60e81604cf48d22d847344d009237cd21a1943b060278898bb -B -c "import sys,glob,json; bridges=glob.glob('/opt/spark/python/lib/py4j-*-src.zip'); sys.path[:0]=['/opt/spark/python',*bridges]; import py4j,pyspark; print(json.dumps({'python':sys.version.split()[0],'pyspark':pyspark.__version__,'py4j':py4j.__version__,'bundled_bridges':bridges},sort_keys=True))"
docker.exe run --rm --user 0 --entrypoint java apache/spark@sha256:2442368ef4a5ca60e81604cf48d22d847344d009237cd21a1943b060278898bb -jar /opt/spark/jars/ivy-2.5.3.jar -?
```

Evidence: exact MinIO releases/commits in §6; PostgreSQL16.14; Redis7.4.9; Python3.11.9 and3.11.17 each with SQLite3.40.1; Spark Python3.10.12, PySpark3.5.9 and `/opt/spark/python/lib/py4j-0.10.9.7-src.zip` with bridge version0.10.9.7. Ivy2.5.3 displayed its CLI usage before the actual resolution probe.

### Exact empty-cache Spark/Maven acquisition probe

```bash
docker.exe run --rm --user 0 -e "PYSPARK_SUBMIT_ARGS=--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.apache.spark:spark-avro_2.12:3.5.9,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3 --repositories https://repo.maven.apache.org/maven2 --conf spark.jars.ivy=/tmp/artifact-gate-ivy pyspark-shell" --entrypoint python3 apache/spark@sha256:2442368ef4a5ca60e81604cf48d22d847344d009237cd21a1943b060278898bb -B -c "import sys,glob,pathlib,hashlib,json,os; sys.path[:0]=['/opt/spark/python',*glob.glob('/opt/spark/python/lib/py4j-*-src.zip')]; import pyspark; from pyspark.java_gateway import launch_gateway; gateway=launch_gateway(); files=sorted(pathlib.Path('/tmp/artifact-gate-ivy').rglob('*.jar')); print('MAVEN_GATE '+json.dumps({'python':sys.version.split()[0],'pyspark':pyspark.__version__,'jar_count':len(files),'files':[{'filename':f.name,'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()} for f in files]},sort_keys=True)); gateway.shutdown(); gateway.proc.terminate(); gateway.proc.wait(timeout=10)"
```

Recorded resolver summary:

```text
default: modules18, searched18, downloaded18, evicted0; artifacts18, downloaded18
18 artifacts copied, 0 already retrieved
MAVEN_GATE: python3.10.12, pyspark3.5.9, jar_count36
```

The36 paths contain18 cache/retrieval duplicates with matching hashes, as explained in §7.

### Exact final root-wheel probe (Python3.11.9)

```bash
docker.exe run --rm --entrypoint python3 python@sha256:8fb099199b9f2d70342674bd9dbccd3ed03a258f26bbd1d556822c6dfc60c317 -B -c "import sys,subprocess,tempfile,pathlib,hashlib,json; req=['confluent-kafka[schemaregistry]==2.15.1','fastavro==1.12.2','authlib==1.8.0','boto3==1.35.62','botocore==1.35.62','psycopg2-binary==2.9.10','sqlalchemy==2.0.36','redis==5.2.0','streamlit==1.40.1','plotly==5.24.1','pandas==2.2.3','faker==31.0.0','prometheus-client==0.21.0','requests==2.34.2','httpx==0.27.2','tenacity==9.0.0','python-dotenv==1.0.1','pydantic==2.9.2','pydantic-settings==2.6.1','structlog==24.4.0','pytest==8.3.3','pytest-cov==6.0.0','pytest-asyncio==0.24.0','pytest-mock==3.14.0','ruff==0.7.4','black==24.10.0']; dest=pathlib.Path(tempfile.mkdtemp(prefix='artifact-gate-')); result=subprocess.run([sys.executable,'-m','pip','download','--no-cache-dir','--only-binary=:all:','--index-url','https://pypi.org/simple','--dest',str(dest),*req],text=True,capture_output=True); print('PIP_EXIT '+str(result.returncode)); assert result.returncode==0,result.stdout[-4000:]+result.stderr[-4000:]; files=[{'filename':f.name,'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()} for f in sorted(dest.iterdir())]; names={r.split('==')[0].split('[')[0].replace('-','_').lower() for r in req}; direct=[f for f in files if f['filename'].split('-')[0].lower() in names]; assert len(direct)==len(req); print('ARTIFACT_GATE '+json.dumps({'python':sys.version.split()[0],'pip':subprocess.check_output([sys.executable,'-m','pip','--version'],text=True).strip(),'returncode':result.returncode,'requirements':req,'file_count':len(files),'inventory_sha256':hashlib.sha256(json.dumps(files,separators=(',',':'),sort_keys=True).encode()).hexdigest(),'direct_files':direct,'transitive_files':[f['filename'] for f in files if f not in direct]},sort_keys=True))"
```

The exact same inline `-c` payload was also executed on Spark's Python3.10.12 with the following Docker arguments in place of the initial image/entrypoint arguments:

```text
docker.exe run --rm --user 0 --entrypoint python3 apache/spark@sha256:2442368ef4a5ca60e81604cf48d22d847344d009237cd21a1943b060278898bb -B -c <the identical inline payload above>
```

That notation records the exact argument substitution without repeating the26-pin payload; it is not a separately executed helper/script. Both final outputs explicitly contained `PIP_EXIT 0`, `returncode: 0`, all26 direct files, and the82/85-wheel counts and inventory hashes in §8.

### Exact final exporter-wheel probe

```bash
docker.exe run --rm --entrypoint python3 python@sha256:2333bd330d12de02514770b3585cad313644316047cdee24a7acfdece6de6efb -B -c "import sys,subprocess,tempfile,pathlib,hashlib,json; req=['prometheus-client==0.21.0','sqlalchemy==2.0.36','psycopg2-binary==2.9.10','redis==5.2.0','httpx==0.27.2','confluent-kafka==2.15.1']; dest=pathlib.Path(tempfile.mkdtemp(prefix='artifact-gate-')); result=subprocess.run([sys.executable,'-m','pip','download','--no-cache-dir','--only-binary=:all:','--index-url','https://pypi.org/simple','--dest',str(dest),*req],text=True,capture_output=True); print('PIP_EXIT '+str(result.returncode)); assert result.returncode==0,result.stdout[-4000:]+result.stderr[-4000:]; files=[{'filename':f.name,'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()} for f in sorted(dest.iterdir())]; print('ARTIFACT_GATE '+json.dumps({'python':sys.version.split()[0],'pip':subprocess.check_output([sys.executable,'-m','pip','--version'],text=True).strip(),'returncode':result.returncode,'requirements':req,'file_count':len(files),'inventory_sha256':hashlib.sha256(json.dumps(files,separators=(',',':'),sort_keys=True).encode()).hexdigest(),'files':files},sort_keys=True))"
```

Evidence: `PIP_EXIT 0`, Python3.11.17, pip24.0,14 files, inventory hash and all individual hashes summarized in §8.

### Exact PySpark full-byte acquisition probe

```bash
docker.exe run --rm --entrypoint python3 python@sha256:8fb099199b9f2d70342674bd9dbccd3ed03a258f26bbd1d556822c6dfc60c317 -B -c "import urllib.request,json,hashlib,tempfile,tarfile,zipfile,io; metadata=json.load(urllib.request.urlopen('https://pypi.org/pypi/pyspark/3.5.9/json',timeout=30)); entries=[f for f in metadata['urls'] if f['packagetype']=='sdist']; print('PYSPARK_METADATA '+json.dumps({'requires_python':metadata['info']['requires_python'],'requires_dist':metadata['info']['requires_dist'],'files':[{'filename':f['filename'],'url':f['url'],'bytes':f['size'],'sha256':f['digests']['sha256']} for f in entries]})); f=entries[0]; h=hashlib.sha256(); total=0
with urllib.request.urlopen(f['url'],timeout=120) as r:
 for block in iter(lambda:r.read(1048576),b''): h.update(block); total+=len(block)
assert h.hexdigest()==f['digests']['sha256'] and total==f['size']; print('PYSPARK_DOWNLOAD '+json.dumps({'http_status':200,'filename':f['filename'],'bytes':total,'sha256':h.hexdigest(),'matches_pypi':True}))"
```

Evidence: `PYSPARK_DOWNLOAD` reportedHTTP200, the full317858593 bytes, the SHA256 in §8 and `matches_pypi: true`.

### Exact Py4J acquisition probe

```bash
python3 -B -c 'import urllib.request,json,hashlib
for version in ["0.10.9.7","0.10.9.9"]:
 metadata=json.load(urllib.request.urlopen("https://pypi.org/pypi/py4j/"+version+"/json",timeout=30)); f=next(f for f in metadata["urls"] if f["filename"].endswith(".whl")); data=urllib.request.urlopen(f["url"],timeout=30).read(); digest=hashlib.sha256(data).hexdigest(); assert digest==f["digests"]["sha256"] and len(data)==f["size"]; print(json.dumps({"version":version,"filename":f["filename"],"url":f["url"],"bytes":len(data),"sha256":digest,"matches_pypi":True}))'
```

Both results explicitly reported `matches_pypi: true`.

### Exact direct-POM metadata probe

```bash
python3 -B -c 'import urllib.request,json,hashlib,xml.etree.ElementTree as E; coordinates=[("org.apache.spark","spark-sql-kafka-0-10_2.12","3.5.9"),("org.apache.spark","spark-avro_2.12","3.5.9"),("org.apache.hadoop","hadoop-aws","3.3.4"),("com.amazonaws","aws-java-sdk-bundle","1.12.262"),("org.postgresql","postgresql","42.7.3")]
for group,name,version in coordinates:
 url="https://repo.maven.apache.org/maven2/"+group.replace(".","/")+"/"+name+"/"+version+"/"+name+"-"+version+".pom"
 with urllib.request.urlopen(url,timeout=30) as r: data=r.read(); status=r.status
 tree=E.fromstring(data); ns={"m":"http://maven.apache.org/POM/4.0.0"}; parent=tree.find("m:parent",ns); print(json.dumps({"coordinate":group+":"+name+":"+version,"pom_url":url,"http_status":status,"pom_sha256":hashlib.sha256(data).hexdigest(),"parent":{x.tag.rsplit("}",1)[-1]:x.text for x in parent} if parent is not None else None}))'
```

All five returnedHTTP200 and the POM hashes in §7. Actual JAR acquisition is separately proven by the Ivy probe, not inferred from POM requests.

### Report direct-checksum cross-check

```bash
python3 -B -c 'import pathlib,re,json,urllib.request,concurrent.futures; text=pathlib.Path("artifact_availability_gate_report.md").read_text(); entries=[]
for line in text.splitlines():
 m=re.fullmatch(r"\| `([^`]+)==([^`]+)` \| `([a-f0-9]+)` \| (same|`[a-f0-9]+`) \| PASS \|",line)
 if m: entries.append((m[1].split("[")[0],m[2],[m[3]]+([] if m[4]=="same" else [m[4].strip("`")])) )
def check(entry):
 name,version,hashes=entry; metadata=json.load(urllib.request.urlopen("https://pypi.org/pypi/"+name+"/"+version+"/json",timeout=30)); published={f["digests"]["sha256"] for f in metadata["urls"] if f["filename"].endswith(".whl")}; return {"pin":name+"=="+version,"reported_hashes":len(hashes),"all_match_published_wheels":all(len(h)==64 and h in published for h in hashes)}
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool: results=list(pool.map(check,entries))
print(json.dumps({"direct_pin_rows":len(entries),"results":results,"all_pass":len(entries)==26 and all(r["all_match_published_wheels"] for r in results)},sort_keys=True)); assert len(entries)==26 and all(r["all_match_published_wheels"] for r in results)'
```

Result:26 rows, all reported hashes matched exact-version published wheels, `all_pass: true`. This is a report-transcription cross-check; actual byte acquisition remains the earlier download evidence.

All command evidence is embedded here; no additional project evidence log or resolver manifest was created. Network access and Docker image-cache changes occurred, but project installation and retained-data access did not.

## 14. Frozen-State Integrity Result

**Frozen-file integrity: PASS — all1059 existing project files unchanged before/after report creation.**

Phase B-before matched Phase A. A second check after all acquisition/version probes and immediately before this report was created still returned:

```json
{"files":1059,"bytes":13034978,"sha256":"973865cacd157eaf4b97a8d56c530f6accb8fa65cc15051aa1e4f0dec486ee08","matches_Phase_B_before":true,"gate_report_exists":false}
```

The post-report comparison, excluding only this new report, returned:

```json
{"existing_files":1059,"existing_bytes":13034978,"existing_sha256":"973865cacd157eaf4b97a8d56c530f6accb8fa65cc15051aa1e4f0dec486ee08","matches_Phase_B_before":true}
```

The method hashes every regular, non-symlink project file, sorts `(relative path, file SHA256)` pairs, serializes with compact JSON separators, then hashes the serialized list. Directory exclusions are `.venv`, `.git`, `__pycache__`, `.pytest_cache`, `.ruff_cache`. Final comparison excludes only the newly added `artifact_availability_gate_report.md`. It preserves the1059-file count,13034978-byte total and aggregate above. Active `.env` is hashed without revealing its contents. There is no Git repository, so this is a content/path audit rather than `git diff`.

The report has all18 numbered sections and the required packaging-status statement. A preliminary report-check assertion over-matched a hexadecimal segment of the PyPI source URL as a SHA256 field; the frozen-file comparison already passed. The final checksum-format check excludes URL path segments. This was a report-checker false positive, not an artifact acquisition or integrity failure.

Exact final content/path integrity and report-structure command:

```bash
python3 -B -c 'import os,pathlib,hashlib,json,re; root=pathlib.Path.cwd(); excluded={".venv",".git","__pycache__",".pytest_cache",".ruff_cache"}; report="artifact_availability_gate_report.md"; rows=[]; total=0
for directory,dirs,files in os.walk(root):
 dirs[:]=sorted(d for d in dirs if d not in excluded)
 for name in sorted(files):
  p=pathlib.Path(directory)/name; relative=p.relative_to(root).as_posix()
  if relative!=report and p.is_file() and not p.is_symlink():
   data=p.read_bytes(); rows.append((relative,hashlib.sha256(data).hexdigest())); total+=len(data)
rows.sort(); digest=hashlib.sha256(json.dumps(rows,separators=(",",":")).encode()).hexdigest(); text=(root/report).read_text(); headings=[int(x) for x in re.findall(r"^## ([0-9]+)\.",text,re.M)]; hashes=re.findall(r"(?<![/0-9a-f])(?:sha256:)?([0-9a-f]{48,80})(?![/0-9a-f])",text); invalid=[h for h in hashes if len(h)!=64]; matches=len(rows)==1059 and total==13034978 and digest=="973865cacd157eaf4b97a8d56c530f6accb8fa65cc15051aa1e4f0dec486ee08"; statement="Packaging implementation has NOT started." in text; print(json.dumps({"existing_files":len(rows),"existing_bytes":total,"existing_sha256":digest,"matches_Phase_B_before":matches,"only_new_project_artifact":report if matches else "UNVERIFIED","numbered_sections":headings,"sections_valid":headings==list(range(1,19)),"sha256_length_errors":invalid,"packaging_statement_present":statement})); assert matches and headings==list(range(1,19)) and not invalid and statement'
```

The initial and final Docker state probes agree that the nine retained OIP containers remain stopped and container count is21. No probe mounted retained PostgreSQL/MinIO/Kafka/Redis/receiver/Prometheus/Grafana volumes or invoked a frozen application against them. No live retained-database digest is claimed: last certified M4/M7/M8 business-state hashes remain the frozen M9 evidence, not a fresh business audit.

Protected default source anchors remain inputs that must not be used as fresh fixtures: database`order_intelligence`, Gold schema`gold_serving`, topic`order-events-m3-20261003b`, Gold version3, snapshot digest`26ec1b51ba8c4735e57e5af5f0bdd28265dc125c520184d371986553f2b2a231`, manifest`s3a://gold/order-events-m3-20261003b/_commits/00000000000000000003.json`, and retained M7/M8 outcomes/receipt ledger. This gate did not read/mutate those live stores or reset their state.

## 15. PASS / PARTIAL / BLOCKED Verdict

**Overall Phase B verdict: BLOCKED.**

- Docker Engine requirement: **PASS**.
- Non-MinIO image digest access: **PASS**, with cached-layer qualifications.
- Exact Python bases: **PASS**.
- Frozen Spark tuple/JAR acquisition: **PASS**.
- Frozen Python direct dependency and candidate-closure acquisition: **PASS**; installed/runtime compatibility remains a later obligation.
- Exact required MinIO server and initializer-client acquisition: **BLOCKED**.
- Source exporter rebuild and full clean-runtime execution: **unperformed future checks**, not fabricated PASS results.

PASS requires established reproducible availability of every required artifact. PARTIAL would not remove a required-artifact acquisition failure. Docker's restored availability therefore does not justify proceeding to implementation while MinIO lacks an approved tested source.

## 16. Exact Blockers

1. **Server artifact:** `docker.io/minio/minio:RELEASE.2024-11-07T00-52-20Z`, measured digest`sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332`; both tag and digest pulls failed under current access conditions.
2. **Initializer artifact:** `docker.io/minio/mc:RELEASE.2024-11-05T11-29-45Z`, measured digest`sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682`; both tag and digest pulls failed under current access conditions.
3. **Source decision needed to resolve1–2:** no approved independently obtainable exact-artifact source or sealed checksummed archive for both releases has been supplied. Local cached images and successful cached version output alone do not establish this prerequisite.

No additional unavailable required Python or Maven artifact was found in the completed probes. This statement concerns acquisition only; it is not proof that later application builds, transitive compatibility, isolation, restart or regression checks will pass.

## 17. Exact Prerequisites for Phase C

**Phase C allowed now: NO.**

Before Phase C can be authorized:

1. Resolve the MinIO source/distribution decision explicitly for **both exact frozen images**, including provenance, version, Linux/amd64 content identity/checksums and an acquisition procedure independent of the existing warm cache. If using an archive, explicitly adopt its approved source and transfer/access procedure; do not silently generate and accept a warm-cache export.
2. Perform a bounded acquisition/verification of those approved artifacts and update the availability verdict with honest evidence. No version substitution or S3/initializer semantic change is implied by this report.
3. Obtain explicit permission to enter Phase C and its additive file scope after the required-artifact gate passes. This report and the user's request to continue Phase B do not authorize packaging implementation.

The subsequent authorized design/implementation must preserve the chosen Linux/amd64 target, measured base/infra digests and frozen direct pins; record complete per-artifact resolved versions/checksums before claiming a lock; keep Spark driver/worker Python matched; verify candidate transitive compatibility rather than assume it from downloads; and establish isolated resource names/fixtures, manual stage ordering, M4 invocation, M8 namespace/ledger durability and M9 health/datasource wiring. Builds, all27-pin installation where applicable, launch/restart/failure/regression checks remain later evidence obligations.

If satisfying the source decision or packaging design requires changing frozen business interfaces or upgrading dependencies, stop for a separately approved scope decision. The retained M1–M9 certification is the authority throughout.

## 18. Packaging Implementation Status

**Packaging implementation has NOT started.**

Only `artifact_availability_gate_report.md` was added in Phase B. No Phase C implementation was begun. Stop at this gate pending the exact MinIO artifact-source decision and a passing required-artifact availability result.
