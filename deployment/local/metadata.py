"""Retain verified sources, installed closures and portable stopped-runtime evidence."""
import importlib.metadata
import json
import re
import time
from pathlib import Path
import runtime as r

TAGS = {'daemon': '29.6.2-dind', 'python': '3.11.9-slim', 'exporter_python': '3.11-slim (frozen resolution: 3.11.17)', 'spark': '3.5.9', 'kafka': '7.7.12', 'registry': '7.7.12', 'postgres': '16-alpine (frozen binary: 16.14)', 'redis': '7-alpine (frozen binary: 7.4.9)', 'prometheus': 'v2.54.1', 'grafana': '11.3.0', 'minio': 'RELEASE.2024-11-07T00-52-20Z', 'mc': 'RELEASE.2024-11-05T11-29-45Z'}
PURPOSE = {'daemon': 'Empty isolated Docker/Compose acquisition, build and verification engine', 'python': 'Core application Python base', 'exporter_python': 'Exporter-only Python base', 'spark': 'Bundled frozen Spark/PySpark, Scala, Hadoop, Java and Python driver/worker base', 'kafka': 'Avro event broker and Kafka-backed Registry state', 'registry': 'Frozen Avro schema registry', 'postgres': 'Authoritative Gold, M7 and M8 persistence', 'redis': 'Deterministic M5 snapshot cache', 'prometheus': 'Optional frozen scrape/query stack', 'grafana': 'Optional frozen datasource and three dashboards', 'minio': 'Controlled frozen S3A object store', 'mc': 'Controlled frozen bucket initialization client'}

def collect():
    inventory = json.loads((r.LOCAL / 'artifact_inventory.json').read_text())
    fmt = '{"id":{{json .Id}},"descriptor":{{json .Descriptor}},"repo_tags":{{json .RepoTags}},"repo_digests":{{json .RepoDigests}},"os":{{json .Os}},"architecture":{{json .Architecture}}}'
    images = []
    for role, ref in r.PLAN['images'].items():
        command = r.outer if role == 'daemon' else r.inner
        actual = json.loads(command('image','inspect',ref,'--format',fmt,label='verified-image-metadata-' + role))
        assert actual['architecture'] == 'amd64' and actual['os'] == 'linux'
        assert actual['descriptor']['digest'] == ref.split('@')[1]
        images.append({'name': role, 'source': 'sealed archive' if role in {'minio','mc'} else 'official Docker Hub repository', 'tag': TAGS[role], 'reference_used': ref, 'digest': ref.split('@')[1], 'platform': 'linux/amd64', 'purpose': PURPOSE[role], 'verification': 'actual Docker descriptor/RepoDigest/platform plus applicable binary/runtime oracle', 'actual': actual, 'image_id_semantics': 'containerd root descriptor identity, not an OCI config digest'})
    wheels = {}
    closures = {}
    for role, python in [('core','3.11.9'), ('spark','3.10.12'), ('exporter','3.11.17')]:
        expected = json.loads((r.LOCAL / 'assets' / role / 'inventory.json').read_text())
        for row in expected['wheels']:
            wheels[(role,row['filename'])] = row
        code = 'import importlib.metadata as m,json,sys; print(json.dumps({"python":sys.version.split()[0],"packages":{d.metadata["Name"]:d.version for d in m.distributions()}}))'
        actual = json.loads(r.inner('run','--rm','--network','none','oip-local-' + role + ':phase-c','python3','-c',code,label='installed-closure-' + role))
        normalize = lambda name: re.sub(r'[-_.]+','-',name).lower()
        installed = {normalize(k):v for k,v in actual['packages'].items()}
        assert actual['python'] == python
        assert installed == {normalize(x['name']):x['version'] for x in expected['wheels']}
        closures[role] = {**actual, 'wheel_count':len(expected['wheels']), 'hash_lock':'assets/' + role + '/requirements.lock', 'verification':'all installed distributions equal the full hash-locked wheel closure; pip check passed'}
    for artifact in inventory['artifact_files']:
        p = Path(artifact['name'])
        artifact['purpose'] = 'Declared build artifact or acquisition metadata'
        artifact['tag'] = None
        if p.suffix == '.whl':
            row = wheels[(p.parts[1],p.name)]
            artifact.update(source='https://pypi.org/simple', tag=row['version'], package=row['name'], purpose=p.parts[1] + ' virtual-environment dependency', verification='actual wheel SHA256, --require-hashes offline installation, installed closure equality and pip check')
        elif p.suffix == '.jar':
            if 'cache' in p.parts:
                group, remaining = p.parts[p.parts.index('cache') + 1], p.stem
            else:
                group, remaining = p.stem.split('_',1)
            name, version = remaining.rsplit('-',1)
            artifact.update(source='https://repo.maven.apache.org/maven2/' + group.replace('.','/') + '/' + name + '/' + version + '/' + name + '-' + version + '.jar', tag=version, coordinate=group + ':' + name + ':' + version, purpose='Frozen Spark connector/transitive dependency; duplicate vendored copies explicitly inventoried', verification='actual-byte SHA256 and hash-checked driver classpath; real Kafka/Avro/S3A/JDBC runtime oracles')
        elif 'ivy' in p.parts:
            artifact.update(source='Maven/Ivy resolution metadata', purpose='Declared vendored Ivy POM/cache metadata; generated resolver metadata is not a PyPI artifact')
        elif p.name == 'source.tar':
            artifact.update(source='frozen workspace source and required pre-M9 baseline', purpose='Unmodified M1-M9 modules, tests, contracts, configurations and dispatcher')
        else:
            artifact.update(source='generated packaging hash-lock/inventory metadata')
    volume_names = r.inner('volume','ls','--format','{{.Name}}',label='inventory-owned-volume-names').split()
    retired = [name for name in volume_names if re.fullmatch(r'[0-9a-f]{64}',name)]
    container_ids = r.inner('ps','-aq',label='inventory-owned-container-names').split()
    mounts = [json.loads(line) for line in r.inner('inspect',*container_ids,'--format','{{json .Mounts}}',label='inventory-explicit-volume-mounts').splitlines()]
    attached = {item['Name'] for rows in mounts for item in rows if item['Type'] == 'volume'}
    assert not attached.intersection(retired), 'Final runtime still depends on an undeclared anonymous volume'
    for name in retired:
        count = r.inner('run','--rm','--network','none','-v',name + ':/data:ro','oip-local-core:phase-c','python3','-c',"import pathlib; print(len(list(pathlib.Path('/data').iterdir())))",label='retired-directory-empty-' + name[:8])
        assert int(count) == 0
    inventory.update(image_records=images, installed_closures=closures, declared_network={'name':'oip-packaged_default','driver':'bridge','location':'new isolated engine'}, declared_volumes={'outer_engine_data':r.VOLUME,'persistent_business_and_evidence':['kafka-data','minio-data','postgres-data','redis-data','receiver-ledger','oracle-evidence','prometheus-data','grafana-data'],'explicit_empty_image_directories':{'kafka-secrets':'/etc/kafka/secrets','registry-secrets':'/etc/schema-registry/secrets'},'retired_empty_anonymous_volumes':['81a65c90d31d49cfafee441a1da9be3abbd54546f5f1692785855bbd85b370ae','c60928bb50c2d891d853ed14838a658f08e5acee5a8014643a50c36480047590'],'note':'Retired automatically-created directories remain as evidence, unattached and unused after explicit named-mount recreation'}, oracle_fixtures={'primary_topic':r.TOPIC,'primary_database':'order_intelligence','packaged_notification_schema':'m8_packaged','m8_independent_database':r.M8_DATABASE,'m8_independent_run':r.M8_RUN,'note':'M8 secondary database and ledger are freshly initialized from the same committed Gold through frozen M4/M7 APIs; no retained OIP state is copied'}, minio_identities={'server':{'index':'sha256:ac591851803a79aee64bc37f66d77c56b0a4b6e12d9e5356380f4105510f2332','linux_amd64_manifest':'sha256:d051d800a3025588f37f69f132bb5ef718547a9a4ee95ddee44e04ad952a0a96','config':'sha256:afadd2395ca65dde8ec87e5c8460c97ead4561c21dc5c373bb83b099dfcbd657','executable_sha256':'c27873d791b56d33518de11f12c5bfae4be88ebac00e12c6dd0027b08123fd80'},'client':{'index':'sha256:ba8af71554963bb6ad5c4301ff3d82176e96f0b0c7595047da989cacfae83682','linux_amd64_manifest':'sha256:4ccb0c7437bda19ed232603eab10cc246c8f9ac7ccd282943ebb7657a2df4820','config':'sha256:88cf702a7041c364ae320e0585ac4de617719a52536ec171224a4e02089281da','executable_sha256':'a2b886f974c4b32ee57c5bf6f3c8070bc4ac8a62ce3556cc241514aeb5427bd1'}})
    inventory['declared_volumes']['retired_empty_anonymous_volumes'] = retired
    inventory['declared_network']['name'] = r.PROJECT + '_default'
    inventory['runtime_projects_with_owned_volumes'] = sorted({name.split('_',1)[0] for name in volume_names if name.startswith('oip-packaged')})
    (r.LOCAL / 'artifact_inventory.json').write_text(json.dumps(inventory,indent=2))
    print(json.dumps({'verified_images':len(images),'wheel_closures':{k:v['wheel_count'] for k,v in closures.items()},'artifact_files':len(inventory['artifact_files'])}))

def export_stopped():
    stamp = str(time.time_ns())
    ids = r.inner('ps','-aq',label='all-new-container-identities').split()
    fmt = '{"id":{{json .Id}},"name":{{json .Name}},"image":{{json .Image}},"state":{{json .State}},"mounts":{{json .Mounts}}}'
    states = [json.loads(line) for line in r.inner('inspect',*ids,'--format',fmt,label='all-new-stopped-container-states').splitlines()]
    assert not any(x['state']['Running'] for x in states)
    assert all(x['state']['ExitCode'] == 0 for x in states if x['name'].endswith(('-receiver-1','-metrics-exporter-1'))), 'Python server stop bypassed its orderly close path'
    (r.EVIDENCE / ('stopped-runtime-' + stamp + '.json')).write_text(json.dumps(states,indent=2))
    r.compose('logs','--no-color',label='new-runtime-service-logs')
    destination = '/work/runtime-evidence/' + stamp
    r.outer('exec',r.DAEMON,'mkdir','-p',destination,label='ledger-export-directory')
    code = "import json,pathlib,sqlite3; records={}; out=pathlib.Path('/out');\nfor p in pathlib.Path('/source').glob('*.sqlite'):\n src=sqlite3.connect('file:'+str(p)+'?mode=ro',uri=True); dst=sqlite3.connect(out/p.name); src.backup(dst); records[p.name]={t:src.execute('SELECT count(*) FROM '+t).fetchone()[0] for t in ['receipts','calls']}; dst.close(); src.close()\n(out/'ledger-counts.json').write_text(json.dumps(records,indent=2)); print(json.dumps(records))"
    r.inner('run','--rm','--network','none','-v',r.PROJECT + '_receiver-ledger:/source:ro','-v',destination + ':/out','oip-local-core:phase-c','python3','-c',code,label='durable-ledger-backups')
    target = r.EVIDENCE / ('receiver-ledgers-' + stamp)
    target.mkdir(exist_ok=True)
    r.outer('cp',r.DAEMON + ':' + destination + '/.',r.hostpath(target),label='retain-durable-ledgers')

if __name__ == '__main__':
    collect()
