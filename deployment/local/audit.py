"""SHA256 change manifest and exact stopped retained-resource audit; no credential output."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

LOCAL = Path(__file__).resolve().parent
ROOT = LOCAL.parents[1]
OUT = LOCAL / 'evidence/packc20261005a'
BASELINE = OUT / 'frozen-input-baseline.json'
IMAGE_BASELINE = OUT / 'retained-image-baseline.json'
EXCLUDED = {'.git','.venv','__pycache__','.pytest_cache','.ruff_cache'}

def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(1048576),b''):
            h.update(block)
    return h.hexdigest()

def files_now():
    current = {}
    for folder, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in EXCLUDED]
        for name in files:
            p = Path(folder) / name
            if p.is_file() and not p.is_symlink():
                current[p.relative_to(ROOT).as_posix()] = {'sha256':sha(p),'bytes':p.stat().st_size}
    return current

def offline_finalize():
    """Rehash the frozen workspace after the daemon is intentionally stopped.

    Resource identity/state fields are carried only from the preceding successful
    live audit; this mode never labels a Docker API call as freshly performed.
    """
    baseline = json.loads(BASELINE.read_text())
    previous = json.loads((OUT / 'final-integrity.json').read_text())
    current = files_now()
    changed = {k:{'before':v,'after':current.get(k)} for k,v in baseline['files'].items() if current.get(k) != v}
    additions = {k:v for k,v in current.items() if k not in baseline['files']}
    frozen_changes = {k:v for k,v in changed.items() if not k.startswith('.remember/')}
    unexpected = [k for k in additions if not k.startswith(('deployment/local/','.remember/')) and k != 'packaging_phase_c_report.md']
    audit_rel = (OUT / 'final-integrity.json').relative_to(ROOT).as_posix()
    sums_rel = (OUT / 'SHA256SUMS').relative_to(ROOT).as_posix()
    result = {**previous, 'audited_utc':datetime.now(timezone.utc).isoformat(), 'audit_mode':'offline-rehash-after-successful-live-audit', 'docker_live_recheck_performed':False, 'docker_live_recheck_note':'Docker Desktop Linux endpoint was unavailable after the packaged daemon and containers had already been stopped; retained OIP/new-engine resource assertions below are carried from final successful live audit evidence.', 'frozen_existing_changes':frozen_changes, 'harness_metadata_changes':{k:v for k,v in changed.items() if k.startswith('.remember/')}, 'harness_metadata_additions':{k:v for k,v in additions.items() if k.startswith('.remember/')}, 'unexpected_additions':unexpected, 'new_files':{k:v for k,v in additions.items() if not k.startswith('.remember/') and k not in {audit_rel,sums_rel}}, 'archive':current['minio_frozen_linux_amd64_sealed_20261005.tar']}
    result['current_file_count'] = len(current)
    result['current_file_bytes'] = sum(v['bytes'] for v in current.values())
    (OUT / 'final-integrity.json').write_text(json.dumps(result,indent=2))
    (OUT / 'SHA256SUMS').write_text(''.join(v['sha256'] + '  ' + k + '\n' for k,v in sorted(result['new_files'].items())))
    assert not frozen_changes and not unexpected and result['retained_oip_exact_state_equal'] and result['retained_oip_all_stopped'] and result['new_engine_stopped']
    print(json.dumps({'audit_mode':result['audit_mode'],'current_file_count':len(current),'current_file_bytes':sum(v['bytes'] for v in current.values()),'frozen_changes':len(frozen_changes),'unexpected_additions':len(unexpected),'live_resource_assertions_carried':True}))

def audit():
    baseline = json.loads(BASELINE.read_text())
    current = files_now()
    changed = {k:{'before':v,'after':current.get(k)} for k,v in baseline['files'].items() if current.get(k) != v}
    additions = {k:v for k,v in current.items() if k not in baseline['files']}
    frozen_changes = {k:v for k,v in changed.items() if not k.startswith('.remember/')}
    unexpected = [k for k in additions if not k.startswith(('deployment/local/','.remember/')) and k != 'packaging_phase_c_report.md']
    docker = shutil.which('docker.exe') or shutil.which('docker')
    names = [x['name'].lstrip('/') for x in baseline['retained_containers']]
    data = json.loads(subprocess.check_output([docker,'inspect',*names],text=True))
    retained = [{'name':x['Name'],'id':x['Id'],'image':x['Image'],'state':x['State'],'restart_count':x['RestartCount'],'mounts':x['Mounts']} for x in data]
    def canonical_mounts(rows):
        # Docker inspect builds this unordered mount list from an internal map.
        return [{**row,'mounts':sorted(row['mounts'],key=lambda mount:(mount['Destination'],mount['Source']))} for row in rows]
    raw_equal = retained == baseline['retained_containers']
    field_differences = {old['name']:[key for key in old if old[key] != new[key]] for old,new in zip(baseline['retained_containers'],retained) if old != new}
    engine = json.loads(subprocess.check_output([docker,'inspect','--format','{{json .State}}','oip-packaging-phase-c-engine'],text=True))
    result = {'audited_utc':datetime.now(timezone.utc).isoformat(),'pre_existing_file_count':len(baseline['files']),'pre_existing_bytes':sum(v['bytes'] for v in baseline['files'].values()),'baseline_aggregate_sha256':hashlib.sha256(json.dumps(sorted((k,v['sha256']) for k,v in baseline['files'].items()),separators=(',',':')).encode()).hexdigest(),'frozen_existing_changes':frozen_changes,'harness_metadata_changes':{k:v for k,v in changed.items() if k.startswith('.remember/')},'harness_metadata_additions':{k:v for k,v in additions.items() if k.startswith('.remember/')},'unexpected_additions':unexpected,'retained_oip_exact_state_equal':retained == baseline['retained_containers'],'retained_oip_all_stopped':all(not x['state']['Running'] for x in retained),'retained_oip':retained,'new_engine_stopped':not engine['Running'],'new_engine_state':engine,'new_files':{k:v for k,v in additions.items() if not k.startswith('.remember/')},'archive':current['minio_frozen_linux_amd64_sealed_20261005.tar']}
    # The audit and checksum manifest exclude themselves to avoid circular hashes.
    audit_rel = (OUT / 'final-integrity.json').relative_to(ROOT).as_posix()
    sums_rel = (OUT / 'SHA256SUMS').relative_to(ROOT).as_posix()
    result['new_files'] = {k:v for k,v in result['new_files'].items() if k not in {audit_rel,sums_rel}}
    (OUT / 'final-integrity.json').write_text(json.dumps(result,indent=2))
    (OUT / 'SHA256SUMS').write_text(''.join(v['sha256'] + '  ' + k + '\n' for k,v in sorted(result['new_files'].items())))
    result.update(retained_inspect_raw_order_equal=raw_equal,retained_raw_field_differences=field_differences,retained_oip_exact_state_equal=canonical_mounts(retained) == canonical_mounts(baseline['retained_containers']),mount_comparison='All mount fields compared exactly; only unordered inspect mount-array order is canonicalized. Full state, health logs, IDs, images, counts and paths remain strict.')
    if IMAGE_BASELINE.exists():
        ids = sorted(set(subprocess.check_output([docker,'image','ls','-aq','--no-trunc'],text=True).split()))
        images = json.loads(subprocess.check_output([docker,'image','inspect',*ids],text=True))
        own_digest = json.loads((LOCAL / 'images.json').read_text())['images']['daemon'].split('@')[1]
        source = sorted([{'id':x['Id'],'tags':sorted(x.get('RepoTags') or []),'repo_digests':sorted(x.get('RepoDigests') or []),'descriptor':x.get('Descriptor'),'os':x['Os'],'architecture':x['Architecture'],'rootfs':x['RootFS']} for x in images if x['Id'] != own_digest],key=lambda row:row['id'])
        result.update(retained_source_images_equal=source == json.loads(IMAGE_BASELINE.read_text()),retained_source_image_count=len(source),outer_image_count=len(images),retained_source_image_fingerprint=hashlib.sha256(json.dumps(source,sort_keys=True,separators=(',',':')).encode()).hexdigest())
        assert result['retained_source_images_equal']
    (OUT / 'final-integrity.json').write_text(json.dumps(result,indent=2))
    assert not frozen_changes and not unexpected
    assert result['retained_oip_exact_state_equal'] and result['retained_oip_all_stopped'] and result['new_engine_stopped'], json.dumps(field_differences)
    print(json.dumps({k:result[k] for k in ['pre_existing_file_count','pre_existing_bytes','baseline_aggregate_sha256','retained_oip_exact_state_equal','retained_oip_all_stopped','new_engine_stopped']} | {'frozen_changes':len(frozen_changes),'harness_changes':len(result['harness_metadata_changes']),'harness_additions':len(result['harness_metadata_additions']),'new_file_count':len(result['new_files'])}))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--import-baseline',type=Path)
    parser.add_argument('--import-image-baseline',type=Path)
    parser.add_argument('--offline-finalize',action='store_true')
    args = parser.parse_args()
    if args.import_baseline:
        if BASELINE.exists():
            assert BASELINE.read_bytes() == args.import_baseline.read_bytes()
        else:
            BASELINE.write_bytes(args.import_baseline.read_bytes())
    if args.import_image_baseline:
        imported = json.loads(args.import_image_baseline.read_text())
        data = imported.get('all_source_images', imported) if isinstance(imported, dict) else imported
        if IMAGE_BASELINE.exists():
            assert json.loads(IMAGE_BASELINE.read_text()) == data
        else:
            IMAGE_BASELINE.write_text(json.dumps(data,indent=2))
    offline_finalize() if args.offline_finalize else audit()
