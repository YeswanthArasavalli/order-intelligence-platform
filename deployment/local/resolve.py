"""Acquisition only: lock actual wheel/JAR bytes in a disposable exact base."""
import email
import glob
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import zipfile

ROOT = Path('/pack')
role = sys.argv[1]
if role == 'jars':
    sys.path[:0] = ['/opt/spark/python', *glob.glob('/opt/spark/python/lib/py4j-*-src.zip')]
    import os
    os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,org.apache.spark:spark-avro_2.12:3.5.9,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3 --repositories https://repo.maven.apache.org/maven2 --conf spark.jars.ivy=/tmp/empty-ivy pyspark-shell')
    from pyspark.java_gateway import launch_gateway
    gateway = launch_gateway()
    out = ROOT / 'assets/jars'
    out.mkdir(parents=True, exist_ok=True)
    original = json.loads((out / 'inventory.json').read_text()) if (out / 'inventory.json').exists() else None
    for jar in Path('/tmp/empty-ivy/jars').glob('*.jar'):
        shutil.copyfile(jar, out / jar.name)
    files = [{'filename': p.name, 'bytes': p.stat().st_size, 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(out.glob('*.jar'))]
    assert len(files) == 18
    if original is not None and files != original:
        raise RuntimeError('Resolved Maven closure differs from the frozen artifact lock')
    (out / 'inventory.json').write_text(json.dumps(files, indent=2))
    shutil.copytree('/tmp/empty-ivy', ROOT / 'assets/spark/ivy', dirs_exist_ok=True)
    shutil.copytree(out, ROOT / 'assets/spark/jars', dirs_exist_ok=True)
    gateway.shutdown()
    gateway.proc.terminate()
    print(json.dumps({'resolved_jars': len(files)}))
else:
    requirements = ROOT / ('exporter-input.txt' if role == 'exporter' else 'root-input.txt')
    req = [line.split('#')[0].strip() for line in requirements.read_text().splitlines()]
    req = [line for line in req if line and not line.startswith('pyspark==')]
    # Deterministically pinned packaging-only installer; application pins stay frozen.
    req.append('pip==22.0.2' if role == 'spark' else 'pip==24.0')
    out = ROOT / 'assets' / role
    out.mkdir(parents=True, exist_ok=True)
    locked = out / 'requirements.lock'
    selection = ['--no-index', '--find-links', str(out), '--require-hashes', '-r', str(locked)] if locked.exists() else ['--index-url', 'https://pypi.org/simple', *req]
    subprocess.run([sys.executable, '-m', 'pip', 'download', '--no-cache-dir', '--only-binary=:all:', '--dest', str(out), *selection], check=True)
    records = []
    for wheel in sorted(out.glob('*.whl')):
        with zipfile.ZipFile(wheel) as z:
            meta = email.message_from_bytes(z.read(next(n for n in z.namelist() if n.endswith('.dist-info/METADATA'))))
        records.append({'name': meta['Name'], 'version': meta['Version'], 'filename': wheel.name, 'bytes': wheel.stat().st_size, 'sha256': hashlib.sha256(wheel.read_bytes()).hexdigest()})
    lock = [f"{r['name']}=={r['version']} --hash=sha256:{r['sha256']}" for r in records]
    (out / 'requirements.lock').write_text('\n'.join(lock) + '\n')
    (out / 'inventory.json').write_text(json.dumps({'python': sys.version.split()[0], 'direct_requirements': req, 'wheels': records}, indent=2))
    print(json.dumps({'role': role, 'python': sys.version.split()[0], 'wheels': len(records)}))
