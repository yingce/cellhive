#!/usr/bin/env python3
"""Check stable routing without changing StatefulSet/PVC discovery identities."""
import pathlib
import subprocess
import yaml

root = pathlib.Path(__file__).resolve().parents[1]
for replicas in (1, 2, 3):
    raw = subprocess.check_output([
        'helm', 'template', 'routing-test', str(root / 'deploy/helm/cellhive'),
        '--namespace', 'routing-ns', '--set', 'existingSecret=test-secret',
        '--set', 'config.bucket=cos://test', '--set', f'cellAgent.replicas={replicas}',
    ], text=True)
    docs = {(d['kind'], d['metadata']['name']): d for d in yaml.safe_load_all(raw) if d}
    base = 'routing-test-cell-agent'
    assert docs['Service', base]['spec']['clusterIP'] == 'None'
    api = docs['Service', base + '-api']['spec']
    assert 'clusterIP' not in api and not api.get('publishNotReadyAddresses', False)
    assert 'statefulset.kubernetes.io/pod-name' not in api['selector']
    for i in range(replicas):
        spec = docs['Service', f'{base}-{i}-endpoint']['spec']
        assert 'clusterIP' not in spec
        assert spec['selector']['statefulset.kubernetes.io/pod-name'] == f'{base}-{i}'
        assert spec['selector']['app.kubernetes.io/instance'] == 'routing-test'
        assert spec['publishNotReadyAddresses'] is True
    sts = docs['StatefulSet', base]['spec']
    assert sts['serviceName'] == base
    assert sts['updateStrategy']['type'] == 'OnDelete'
    env = {e['name']: e.get('value') for e in sts['template']['spec']['containers'][0]['env']}
    assert env['CELLHIVE_ADVERTISE'] == '$(POD_NAME)-endpoint.routing-ns.svc.cluster.local:7001'
    assert env['CELLHIVE_PEER_URL'] == 'http://' + env['CELLHIVE_ADVERTISE']
    config = docs['ConfigMap', 'routing-test-config']['data']
    assert config['CELLHIVE_CELL_URL'] == f'http://{base}-api:7001'
    assert config['CELLHIVE_LOG_REGISTER_URLS'].split(',') == [
        f'http://{base}-{i}-endpoint.routing-ns.svc.cluster.local:7001' for i in range(replicas)]
print('stable agent service routing: 1/2/3 replicas passed')
