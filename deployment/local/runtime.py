"""Declared manual local packaging workflow; host requirements: Python3 + Docker/Compose."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import time

LOCAL = Path(__file__).resolve().parent
ROOT = LOCAL.parents[1]
DOCKER = shutil.which('docker.exe') or shutil.which('docker')
if not DOCKER:
    raise RuntimeError('Declared prerequisite missing: Docker CLI with a reachable Linux engine')
DAEMON = 'oip-packaging-phase-c-engine'
VOLUME = 'oip-packaging-phase-c-data'
PLAN = json.loads((LOCAL / 'images.json').read_text())
RUN = 'packc20261005a'
PROJECT = os.getenv('OIP_LOCAL_PROJECT','oip-packaged')
if PROJECT != 'oip-packaged' and not (PROJECT.startswith('oip-packaged-') and all(c.islower() or c.isdigit() or c == '-' for c in PROJECT)):
    raise RuntimeError('Local project must use the isolated oip-packaged namespace')
TOPIC = 'order-events-m3-' + RUN
M8_RUN = RUN + 'b'
M8_DATABASE = 'order_intelligence_m8_' + M8_RUN
STATE = LOCAL / 'state.json'
EVIDENCE = LOCAL / 'evidence' / RUN
EVIDENCE.mkdir(parents=True, exist_ok=True)
OBSERVABILITY = True

def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1048576), b''):
            h.update(block)
    return h.hexdigest()

def hostpath(path):
    path = str(Path(path).resolve())
    if DOCKER.endswith('docker.exe') and path.startswith('/mnt/'):
        return path[5].upper() + ':\\' + path[7:].replace('/', '\\')
    return path

def run(args, label, expected=0, timeout=900):
    started = datetime.now(timezone.utc).isoformat()
    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        def text(value):
            return value.decode(errors='replace') if isinstance(value, bytes) else (value or '')
        result = subprocess.CompletedProcess(args, 124, text(exc.stdout), text(exc.stderr) + '\nCommand timed out; retain partial diagnostics and resume explicitly.')
    stamp = time.time_ns()
    # Only declared local example credentials exist in the packaged environment.
    output = result.stdout + '\n' + result.stderr
    for value in ['local-example-only']:
        output = output.replace(value, '[LOCAL_EXAMPLE_REDACTED]')
    name = f'{label}-{stamp}'
    (EVIDENCE / (name + '.log')).write_text(output)
    (EVIDENCE / (name + '.json')).write_text(json.dumps({'started_utc': started, 'command': args, 'exit_code': result.returncode, 'expected_exit': expected, 'output_file': name + '.log'}, indent=2))
    print(json.dumps({'step': label, 'exit': result.returncode, 'evidence': name + '.log'}), flush=True)
    if result.returncode != expected:
        raise RuntimeError(f'{label} failed; see {EVIDENCE / (name + ".log")}')
    return result.stdout

def outer(*args, label='docker', expected=0, timeout=900):
    return run([DOCKER, *args], label, expected, timeout)

def inner(*args, label='inner', expected=0, timeout=900):
    return outer('exec', DAEMON, 'docker', '--host', 'unix:///var/run/docker.sock', *args, label=label, expected=expected, timeout=timeout)

def compose(*args, label='compose', expected=0, telemetry=None, timeout=900):
    telemetry = OBSERVABILITY if telemetry is None else telemetry
    env = 'OIP_TELEMETRY_URL=http://metrics-exporter:8000/observe' if telemetry else 'OIP_TELEMETRY_URL='
    return outer('exec', '-e', env, DAEMON, 'docker', '--host', 'unix:///var/run/docker.sock', 'compose', '--project-name', PROJECT, '--project-directory', '/work/deployment/local', '-f', '/work/deployment/local/compose.yaml', '--profile', 'jobs', '--profile', 'observability', *args, label=label, expected=expected, timeout=timeout)

def core(module, *args, label=None, env=None, expected=0):
    opts = []
    for key, value in (env or {}).items():
        opts += ['-e', key + '=' + value]
    return compose('run', '--rm', '--no-deps', *opts, 'core', 'python3', '-m', module, *args, label=label or module.replace('.', '-'), expected=expected)

def wrapper(role, *args, label=None, env=None):
    opts = []
    for key, value in (env or {}).items():
        opts += ['-e', key + '=' + value]
    return compose('run', '--rm', '--no-deps', *opts, 'core', 'python3', '/opt/pack/entrypoints.py', role, *args, label=label or role)

def spark(script, *args, label=None, expected=0, env=None):
    opts = []
    for key, value in (env or {}).items():
        opts += ['-e', key + '=' + value]
    code = "import glob,subprocess,sys; sys.path.insert(0,'/opt/pack'); from preflight import check; check(True); jars=','.join(sorted(glob.glob('/opt/pack/jars/*.jar'))); sys.exit(subprocess.call(['/opt/spark/bin/spark-submit','--master','local[2]','--driver-memory','1g','--jars',jars,*sys.argv[1:]]))"
    return compose('run', '--rm', '--no-deps', *opts, 'spark', 'python3', '-c', code, script, *args, label=label or Path(script).stem, expected=expected, timeout=1800)

def require_archive(path=None):
    record = PLAN['minio_archive']
    path = Path(path or ROOT / record['filename'])
    if not path.is_file():
        raise RuntimeError('Missing controlled MinIO archive')
    if path.stat().st_size != record['bytes'] or digest(path) != record['sha256']:
        raise RuntimeError('Incorrect controlled MinIO archive SHA256/size')

def prepare():
    require_archive()
    if STATE.exists():
        raise RuntimeError('State already exists; refuse accidental fresh-runtime reuse')
    # These names are reserved to this declared runtime, not retained OIP names.
    existing = subprocess.run([DOCKER, 'container', 'inspect', DAEMON], capture_output=True)
    if existing.returncode == 0:
        raise RuntimeError('Isolated daemon already exists; use restart, not fresh start')
    existing_volume = subprocess.run([DOCKER,'volume','inspect',VOLUME],capture_output=True)
    if existing_volume.returncode == 0:
        raise RuntimeError('Declared engine volume already exists; refuse hidden persisted-state reuse')
    outer('pull', '--platform', 'linux/amd64', PLAN['images']['daemon'], label='daemon-acquisition')
    outer('run', '--detach', '--privileged', '--name', DAEMON, '--env', 'DOCKER_TLS_CERTDIR=', '--volume', VOLUME + ':/var/lib/docker', '-p', '127.0.0.1:18501:8501', '-p', '127.0.0.1:19090:9090', '-p', '127.0.0.1:13000:3000', '-p', '127.0.0.1:18000:8000', '--entrypoint', 'dockerd', PLAN['images']['daemon'], '--host', 'unix:///var/run/docker.sock', '--data-root', '/var/lib/docker', '--feature', 'containerd-snapshotter=true', label='fresh-daemon')
    for _ in range(60):
        r = subprocess.run([DOCKER, 'exec', DAEMON, 'docker', 'info', '--format', '{{.Images}} {{.Containers}}'], capture_output=True, text=True)
        if r.returncode == 0:
            assert r.stdout.strip() == '0 0', 'Fresh daemon is not empty'
            break
        time.sleep(1)
    else:
        raise RuntimeError('Isolated Docker Engine not ready')
    inner('info', '--format', '{{json .}}', label='initially-empty-engine')
    STATE.write_text(json.dumps({'daemon': DAEMON, 'volume': VOLUME, 'run': RUN, 'created_utc': datetime.now(timezone.utc).isoformat()}, indent=2))
    archive = ROOT / PLAN['minio_archive']['filename']
    outer('cp', hostpath(archive), DAEMON + ':/tmp/minio.tar', label='archive-transfer')
    copied = outer('exec', DAEMON, 'sha256sum', '/tmp/minio.tar', label='archive-checksum')
    assert copied.split()[0] == PLAN['minio_archive']['sha256']
    inner('image', 'load', '--input', '/tmp/minio.tar', label='archive-only-minio-load')
    for key in ['minio', 'mc']:
        info = json.loads(inner('image', 'inspect', PLAN['images'][key], label=key + '-identity'))[0]
        expected = PLAN['images'][key].split('@')[1]
        assert info['Descriptor']['digest'] == expected and info['Id'] == expected
        assert info['Os'] == 'linux' and info['Architecture'] == 'amd64'
    for key, reference in PLAN['images'].items():
        if key not in ['daemon', 'minio', 'mc']:
            inner('pull', '--platform', 'linux/amd64', reference, label=key + '-fresh-pull', timeout=1800)
    assets = LOCAL / 'assets'
    assets.mkdir(exist_ok=True)
    source_dirs = ['producer', 'kafka', 'streaming', 'data_quality', 'storage', 'dashboard', 'alerts', 'notifications', 'observability', 'utils', 'scripts', 'tests']
    with tarfile.open(assets / 'source.tar', 'w') as tf:
        for folder in source_dirs:
            for p in sorted((ROOT / folder).rglob('*')):
                if p.is_file() and '__pycache__' not in p.parts:
                    tf.add(p, arcname=p.relative_to(ROOT).as_posix(), recursive=False)
        tf.add(ROOT / 'main.py', arcname='main.py')
        tf.add(ROOT / 'milestone9_evidence/20261004a/baseline-1791128639822519664.json', arcname='milestone9_evidence/20261004a/baseline-1791128639822519664.json')
    (LOCAL / 'root-input.txt').write_bytes((ROOT / 'requirements.txt').read_bytes())
    (LOCAL / 'exporter-input.txt').write_bytes((ROOT / 'observability/requirements.txt').read_bytes())
    outer('exec', DAEMON, 'mkdir', '-p', '/work/deployment', label='build-context-directory')
    outer('cp', hostpath(LOCAL), DAEMON + ':/work/deployment/local', label='declared-build-context')
    for role, base in [('core', 'python'), ('spark', 'spark'), ('exporter', 'exporter_python'), ('jars', 'spark')]:
        inner('run', '--rm', '--user', '0', '-v', '/work/deployment/local:/pack', '--entrypoint', 'python3', PLAN['images'][base], '-B', '/pack/resolve.py', role, label='resolve-' + role, timeout=1800)
    outer('cp', DAEMON + ':/work/deployment/local/assets/.', hostpath(assets), label='retain-declared-artifacts', timeout=1800)
    # Config mounts need exact frozen observability files extracted in the isolated context.
    outer('exec', DAEMON, 'mkdir', '-p', '/work/deployment/local/assets/source', label='config-directory')
    outer('exec', DAEMON, 'tar', '-xf', '/work/deployment/local/assets/source.tar', '-C', '/work/deployment/local/assets/source', label='frozen-config-extract')
    inventory()

def inventory():
    artifacts = []
    for p in sorted((LOCAL / 'assets').rglob('*')):
        if p.is_file():
            artifacts.append({'name': p.relative_to(LOCAL).as_posix(), 'bytes': p.stat().st_size, 'sha256': digest(p), 'source': 'frozen workspace' if p.name == 'source.tar' else ('Maven Central' if p.suffix == '.jar' else 'PyPI/resolver metadata'), 'platform': 'linux/amd64', 'verification': 'actual-byte SHA256; installed/runtime oracles required'})
    result = {**PLAN, 'artifact_files': artifacts, 'python_roles': {'core': '3.11.9', 'spark_driver_and_workers': '3.10.12', 'exporter': '3.11.17'}, 'spark_tuple': {'spark': '3.5.9', 'scala': '2.12', 'hadoop': '3.3.4', 'aws_sdk_bundle': '1.12.262', 'jdbc': '42.7.3'}, 'business_source': 'frozen M1-M9 modules', 'runtime': 'isolated Docker daemon and Compose project oip-packaged'}
    (LOCAL / 'artifact_inventory.json').write_text(json.dumps(result, indent=2))

def finish_prepare():
    """Resume only interrupted artifact retention; never call this a new empty daemon."""
    outer('cp', hostpath(LOCAL / 'resolve.py'), DAEMON + ':/work/deployment/local/resolve.py', label='sync-resolver')
    for role, base in [('core', 'python'), ('spark', 'spark'), ('exporter', 'exporter_python')]:
        inner('run', '--rm', '--user', '0', '-v', '/work/deployment/local:/pack', '--entrypoint', 'python3', PLAN['images'][base], '-B', '/pack/resolve.py', role, label='isolated-installer-lock-' + role, timeout=1800)
    outer('cp', DAEMON + ':/work/deployment/local/assets/.', hostpath(LOCAL / 'assets'), label='retain-declared-artifacts-resumed', timeout=3600)
    outer('exec', DAEMON, 'mkdir', '-p', '/work/deployment/local/assets/source', label='config-directory')
    outer('exec', DAEMON, 'tar', '-xf', '/work/deployment/local/assets/source.tar', '-C', '/work/deployment/local/assets/source', label='frozen-config-extract')
    inventory()

def build(fresh=True):
    # Refresh additive wrappers/Dockerfile without rebuilding any frozen source.
    for name in ['Dockerfile', 'compose.yaml', 'entrypoints.py', 'oracle_adapter.py', 'preflight.py', 'mismatch_worker.sh']:
        outer('cp', hostpath(LOCAL / name), DAEMON + ':/work/deployment/local/' + name, label='sync-' + name)
    for role, base in [('core', 'python'), ('spark', 'spark'), ('exporter', 'exporter_python')]:
        inner('build', *(['--no-cache'] if fresh else []), '--network', 'none', '--build-arg', 'BASE=' + PLAN['images'][base], '--build-arg', 'ROLE=' + role, '-t', 'oip-local-' + role + ':phase-c', '/work/deployment/local', label=('build-' if fresh else 'declared-layer-refresh-') + role, timeout=1800)
        inner('run', '--rm', '--network', 'none', 'oip-local-' + role + ':phase-c', 'python3', '-m', 'pip', 'check', label='pip-check-' + role)
    built = json.loads(inner('image', 'inspect', 'oip-local-core:phase-c', 'oip-local-spark:phase-c', 'oip-local-exporter:phase-c', label='built-image-inventory'))
    inv = json.loads((LOCAL / 'artifact_inventory.json').read_text())
    inv['built_images'] = [{k: x.get(k) for k in ['Id', 'RepoTags', 'Descriptor', 'Os', 'Architecture']} for x in built]
    (LOCAL / 'artifact_inventory.json').write_text(json.dumps(inv, indent=2))

def refresh_build():
    build(fresh=False)

def resume_engine():
    if not STATE.exists():
        raise RuntimeError('No declared runtime state; run prepare first')
    running = json.loads(outer('inspect', '--format', '{{json .State.Running}}', DAEMON, label='engine-state'))
    if not running:
        outer('start', DAEMON, label='resume-isolated-engine')
        for _ in range(60):
            proc = subprocess.run([DOCKER, 'exec', DAEMON, 'docker', 'info'], capture_output=True)
            if proc.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError('Resumed isolated engine not ready')

def start():
    resume_engine()
    compose('up', '-d', '--force-recreate', '--wait', '--wait-timeout', '240', 'kafka', 'schema-registry', 'minio', 'postgres', 'redis', label='infrastructure-ready', telemetry=False)
    compose('run', '--rm', 'minio-init', label='bucket-initialization', telemetry=False)

def launch():
    smoke = 'order-events-packaged-' + RUN
    wrapper('fixture', '--topic', smoke, label='packaged-producer-fixture')
    spark('/opt/pack/entrypoints.py', 'drain', '--topic', smoke, label='packaged-bronze-silver-gold')
    for phase in ['initial', 'second', 'third']:
        spark('/opt/oip/scripts/verify_milestone3.py', '--topic', TOPIC, '--phase', phase, label='launch-m3-' + phase)
    launch_services()

def launch_services():
    """Resume the idempotent serving/one-shot roles after fixture creation."""
    spark('/opt/pack/entrypoints.py', 'm4', '--topic', TOPIC, label='packaged-m4')
    core('storage.postgres_redis_serving', label='packaged-m5')
    compose('up', '-d', '--wait', '--wait-timeout', '90', 'dashboard', label='packaged-dashboard-ready')
    core('alerts.evaluator', '--rules', 'alerts/rules.example.json', label='packaged-m7')
    compose('up', '-d', '--wait', '--wait-timeout', '60', 'receiver', label='packaged-receiver-ready')
    compose('run', '--rm', 'notifications', label='packaged-m8')
    wrapper('receipts', label='packaged-provider-receipts')
    if OBSERVABILITY:
        compose('up', '-d', 'metrics-exporter', 'prometheus', 'grafana', label='optional-m9-start')
        m9('stack')

def m9(phase, expected=0):
    return compose('run', '--rm', '--no-deps', 'core', 'python3', '/opt/pack/oracle_adapter.py', '--phase', phase, label='m9-' + phase, expected=expected)

def regression():
    m1 = 'order-events-m1-' + RUN
    spark('/opt/oip/scripts/verify_milestone3_m1.py', '--topic', m1, label='m1-wire-bronze')
    spark('/opt/oip/scripts/verify_milestone1_bronze.py', '--expected-count', '4', label='m1-checkpoint-restart', env={'KAFKA_TOPIC_ORDERS': m1, 'SPARK_CHECKPOINT_DIR': 's3a://checkpoints/' + m1})
    m2 = 'order-events-m2-' + RUN
    for phase in ['initial', 'restart', 'recovery']:
        spark('/opt/oip/scripts/verify_milestone2.py', '--topic', m2, '--phase', phase, label='m2-' + phase)
    for phase in ['replay', 'replay', 'recovery']:
        spark('/opt/oip/scripts/verify_milestone3.py', '--topic', TOPIC, '--phase', phase, label='m3-' + phase)
    for phase in ['initial', 'retry', 'failure', 'old', 'corrupt']:
        spark('/opt/oip/scripts/verify_milestone4.py', '--topic', TOPIC, '--schema', 'gold_m4_packaged', '--phase', phase, label='m4-' + phase)
    for phase in ['serve', 'verify', 'rebuild']:
        core('scripts.verify_milestone5', '--phase', phase, '--namespace', 'oip:m5:verify-' + RUN, label='m5-' + phase)
    for phase in ['production', 'healthy', 'missing', 'corrupt', 'rebuild']:
        args = [] if phase == 'production' else ['--namespace', 'oip:m5:verify-m6-' + RUN]
        core('scripts.verify_milestone6', '--phase', phase, *args, label='m6-' + phase)
    for phase in ['production', 'restart', 'controlled']:
        core('scripts.verify_milestone7', '--phase', phase, '--run', RUN, label='m7-' + phase)
    regression_tail()

def m8_verify():
    # A new explicit database avoids reusing a partially exercised, non-reentrant oracle fixture.
    code = "from sqlalchemy import text; from storage.postgres_redis_serving import get_postgres_engine; e=get_postgres_engine(); c=e.connect().execution_options(isolation_level='AUTOCOMMIT'); c.execute(text('CREATE DATABASE " + M8_DATABASE + "')); c.close(); e.dispose(); print('Fresh isolated M8 oracle database created')"
    compose('run', '--rm', '--no-deps', 'core', 'python3', '-c', code, label='m8-new-fixture-database')
    env = {'POSTGRES_DB': M8_DATABASE}
    spark('/opt/pack/entrypoints.py', 'm4', '--topic', TOPIC, label='m8-fresh-gold-binding', env=env)
    core('alerts.evaluator', '--rules', 'alerts/rules.example.json', label='m8-fresh-two-events', env=env)
    wrapper('m8-baseline', '--run', M8_RUN, label='m8-isolated-baseline', env=env)
    for phase in ['runtime', 'redis-down', 'integrity']:
        core('scripts.verify_milestone8', '--phase', phase, '--run', M8_RUN, label='m8-isolated-' + phase, env=env)

def regression_tail():
    m8_verify()
    full_suite()
    m9_activity()

def full_suite():
    # Frozen M8 tests use 100 ms transport deadlines. Remove unrelated telemetry/scrape load.
    if OBSERVABILITY:
        compose('stop', 'metrics-exporter', 'prometheus', 'grafana', label='suite-observability-quiesce')
    try:
        targets = ['test_notifications.py', 'test_alerts.py', 'test_dashboard.py', 'test_postgres_serving.py', 'test_redis_serving.py', 'test_observability.py', 'test_milestone2.py', 'test_gold.py']
        assert set(targets) == {p.name for p in (ROOT / 'tests/unit').glob('test_*.py')}, 'Frozen suite file coverage changed'
        # Execute every frozen test file in one session; tight HTTP deadlines precede Spark fixtures.
        spark('/opt/oip/scripts/run_spark_tests.py', *['tests/unit/' + name for name in targets], label='frozen-full-suite-http-before-spark', env={'OIP_TELEMETRY_URL': ''})
    finally:
        if OBSERVABILITY:
            compose('up', '-d', 'metrics-exporter', 'prometheus', 'grafana', label='suite-observability-restore')

def m9_activity():
    # Produce fresh Gold activity after exporter start without changing source bindings.
    spark('/opt/oip/scripts/verify_milestone3.py', '--topic', 'order-events-m3-activity-' + RUN, '--phase', 'initial', label='m9-fresh-gold-activity')
    core('storage.postgres_redis_serving', label='m9-redis-refresh')
    core('scripts.verify_milestone6', '--phase', 'production', label='m9-dashboard-activity')
    core('scripts.verify_milestone7', '--phase', 'controlled', '--run', RUN + 'm9', label='m9-fresh-suppression-activity')
    spark('/opt/pack/entrypoints.py', 'm4', '--topic', TOPIC, label='m9-fresh-postgres-commit', env={'POSTGRES_GOLD_SCHEMA': 'gold_m9_packaged'})
    for phase in ['serving', 'alerts', 'notifications', 'no-data']:
        core('scripts.milestone9_activity', '--phase', phase, '--run', RUN, label='m9-controlled-' + phase)
    m9('activity')
    m9('application-restart')

def failures():
    bad_archive = EVIDENCE / 'deliberately-invalid-minio.tar'
    shutil.copyfile(ROOT / PLAN['minio_archive']['filename'], bad_archive)
    with bad_archive.open('r+b') as f:
        first = f.read(1)
        f.seek(0)
        f.write(bytes([first[0] ^ 1]))
    assert bad_archive.stat().st_size == PLAN['minio_archive']['bytes']
    for label, p in [('missing-artifact', ROOT / 'deliberately-missing-archive.tar'), ('incorrect-archive-sha', bad_archive)]:
        try:
            require_archive(p)
        except RuntimeError as exc:
            (EVIDENCE / (label + '.json')).write_text(json.dumps({'expected_failure': str(exc), 'rejected': True, 'actual_sha256': digest(p) if p.is_file() else None, 'actual_bytes': p.stat().st_size if p.is_file() else None}))
        else:
            raise AssertionError(label)
    core('storage.postgres_redis_serving', label='unavailable-endpoint', env={'POSTGRES_HOST': 'nonexistent.invalid'}, expected=1)
    # Real different interpreter obtained from declared core base, not a version-string shim.
    inner('run', '--rm', '-v', '/work/worker-python:/out', '--entrypoint', '/bin/sh', PLAN['images']['python'], '-c', 'mkdir -p /out/bin /out/lib; cp /usr/local/bin/python3.11 /out/bin/; cp -a /usr/local/lib/python3.11 /out/lib/; cp -a /usr/local/lib/libpython3.11.so* /out/lib/; cp /lib/x86_64-linux-gnu/libc.so.6 /lib/x86_64-linux-gnu/libm.so.6 /out/lib/; cp /lib64/ld-linux-x86-64.so.2 /out/', label='mismatch-worker-binary')
    compose('run', '--rm', '--no-deps', '-v', '/work/worker-python:/worker:ro', 'spark', '/opt/pack/mismatch_worker.sh', '-c', 'import platform; print(platform.python_version())', label='mismatch-worker-initialized')
    compose('run', '--rm', '--no-deps', '-v', '/work/worker-python:/worker:ro', 'spark', 'python3', '/opt/pack/preflight.py', '--spark', '--worker', '/opt/pack/mismatch_worker.sh', label='driver-worker-mismatch', expected=1)
    missing_code = "import pathlib,sys; sys.path.insert(0,'/opt/pack'); from preflight import check; p=pathlib.Path('/tmp/missing-jars'); p.mkdir(); [ (p/x.name).symlink_to(x) for x in pathlib.Path('/opt/pack/jars').iterdir() if x.name!='org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.9.jar']; check(True,jar_root=str(p))"
    compose('run', '--rm', '--no-deps', 'spark', 'python3', '-c', missing_code, label='missing-connector', expected=1)
    compose('stop', 'redis', label='redis-stop')
    core('scripts.verify_milestone5', '--phase', 'redis-down', label='m5-redis-down')
    core('scripts.verify_milestone6', '--phase', 'redis-down', label='m6-redis-down')
    core('scripts.verify_milestone7', '--phase', 'redis-down', '--run', RUN, label='m7-redis-independent')
    core('scripts.verify_milestone8', '--phase', 'redis-down', '--run', M8_RUN, label='m8-actual-redis-outage', env={'POSTGRES_DB': M8_DATABASE})
    m9('redis-down')
    compose('up', '-d', '--wait', 'redis', label='redis-restore')
    core('scripts.verify_milestone5', '--phase', 'rebuild', '--namespace', 'oip:m5:verify-' + RUN, label='redis-rebuild')
    compose('stop', 'postgres', label='postgres-stop')
    core('scripts.verify_milestone5', '--phase', 'postgres-down', label='m5-postgres-down')
    core('scripts.verify_milestone7', '--phase', 'postgres-down', '--run', RUN, label='m7-postgres-down')
    core('scripts.verify_milestone8', '--phase', 'postgres-down', '--run', RUN, label='m8-postgres-down')
    m9('postgres-down')
    compose('up', '-d', '--wait', 'postgres', label='postgres-restore')
    core('scripts.verify_milestone8', '--phase', 'recovery', '--run', RUN, label='m8-restored')
    compose('stop', 'receiver', label='receiver-interruption')
    compose('run', '--rm', '--no-deps', 'notifications', label='receiver-namespace-unavailable', expected=1)
    compose('up', '-d', '--wait', '--force-recreate', 'receiver', label='receiver-ledger-recreation')
    compose('run', '--rm', 'notifications', label='packaged-m8-repeat')
    wrapper('receipts', label='receiver-durable-ledger-verified')
    for service in ['prometheus', 'grafana', 'metrics-exporter']:
        compose('stop', service, label=service + '-unavailable')
        core('storage.postgres_redis_serving', label='business-without-' + service)
        core('alerts.evaluator', '--rules', 'alerts/rules.example.json', label='alerts-without-' + service)
        if service in ['prometheus', 'grafana']:
            m9('stack', expected=1)
        compose('up', '-d', service, label=service + '-restored')
    m9('restart')
    m9('stack')

def extra_outages():
    compose('stop', 'redis', label='m8-explicit-redis-stop')
    try:
        core('scripts.verify_milestone8', '--phase', 'redis-down', '--run', M8_RUN, label='m8-actual-redis-outage', env={'POSTGRES_DB': M8_DATABASE})
    finally:
        compose('up', '-d', '--wait', 'redis', label='m8-explicit-redis-restore')
    code = "import json; from sqlalchemy import text; from storage.postgres_redis_serving import get_postgres_engine; e=get_postgres_engine(); c=e.connect(); print(json.dumps(c.execute(text('SELECT snapshot_digest FROM gold_serving.serving_state')).scalar_one())); c.close(); e.dispose()"
    snapshot = json.loads(compose('run','--rm','--no-deps','core','python3','-c',code,label='dashboard-cached-outage-digest'))
    compose('stop', 'postgres', label='dashboard-explicit-postgres-stop')
    try:
        core('scripts.verify_milestone6', '--phase', 'postgres-down', '--expected-digest', snapshot, label='m6-postgres-down-cache-still-valid')
        core('scripts.milestone9_activity', '--phase', 'postgres-down', '--run', RUN, label='m9-cached-dashboard-postgres-outage')
    finally:
        compose('up','-d','--wait','postgres',label='dashboard-explicit-postgres-restore')

def restart():
    resume_engine()
    compose('stop', label='clean-runtime-stop')
    compose('ps', '-a', '--format', 'json', label='stopped-state')
    # Deliberately restore only PostgreSQL first, then converge explicit readiness.
    compose('up', '-d', '--wait', 'postgres', label='partial-startup')
    start()
    compose('up', '-d', '--force-recreate', '--wait', 'dashboard', 'receiver', label='restart-applications')
    if OBSERVABILITY:
        compose('up', '-d', '--force-recreate', 'metrics-exporter', 'prometheus', 'grafana', label='restart-observability')
    spark('/opt/pack/entrypoints.py', 'm4', '--topic', TOPIC, label='restart-m4-noop')
    core('storage.postgres_redis_serving', label='restart-m5')
    core('alerts.evaluator', '--rules', 'alerts/rules.example.json', label='restart-m7-noop')
    compose('run', '--rm', 'notifications', label='restart-m8-noop')
    wrapper('receipts', label='restart-receipts')
    core('scripts.verify_milestone6', '--phase', 'production', label='restart-dashboard')
    if OBSERVABILITY:
        m9('stack')

def stop():
    if json.loads(outer('inspect','--format','{{json .State.Running}}',DAEMON,label='stop-engine-state')) is False:
        print(json.dumps({'already_stopped':DAEMON}),flush=True)
        return
    compose('stop', label='final-packaged-stop')
    compose('ps', '-a', '--format', 'json', label='final-packaged-state')
    inner('volume', 'ls', label='declared-persistent-volumes')
    from metadata import export_stopped
    export_stopped()
    outer('stop', '--timeout', '20', DAEMON, label='final-isolated-engine-stop')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['prepare', 'finish_prepare', 'build', 'refresh_build', 'start', 'launch', 'launch_services', 'regression', 'regression_tail', 'full_suite', 'm9_activity', 'failures', 'extra_outages', 'restart', 'stop', 'all'])
    parser.add_argument('--no-observability',action='store_true',help='Disable optional M9 services/telemetry for business launch/restart')
    args = parser.parse_args()
    OBSERVABILITY = not args.no_observability
    if args.no_observability and args.phase not in {'start','launch','launch_services','restart','stop','full_suite'}:
        parser.error('Full M9 certification phases require observability; use business launch/restart for opt-out')
    phases = [prepare, build, start, launch, regression, failures, restart, stop] if args.phase == 'all' else [globals()[args.phase]]
    for phase in phases:
        phase()
