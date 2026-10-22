import json
import os
from pathlib import Path
from unittest.mock import Mock

import pytest

from trino_setup import runtime
from trino_setup.__main__ import main
from trino_setup.catalogs import apply, statements
from trino_setup.registry import build_snapshot
from trino_setup.runtime import SetupError, Vault, atomic_write, java_value, write_credentials


def rows():
    return json.loads(Path('examples/registry.json').read_text())['data']


def snapshot(data=None):
    return build_snapshot(data or rows(), mysqldbs=['application'], name='online',
                          source='test', env_types=['TEST'])[0]


def test_stable_names_across_filter_and_replica():
    first = snapshot()['mysql_shards'][0]
    alternate = rows()
    alternate[0]['mysqlhost'] = 'different.example.internal'
    assert snapshot(alternate)['mysql_shards'][0]['catalog'] == first['catalog']
    assert first['catalog'] == 'mysql_shard_test_' + __import__('hashlib').sha256(
        b'["v1","TEST","application","00000","ZZZZZ"]').hexdigest()[:16]


def test_prod_primary_never_selected():
    data = rows()
    data[0].update(env_type='PROD', used_for='GR-Node1')
    result, report = build_snapshot(data, mysqldbs=['application'], name='online', source='test')
    assert result['mysql_shards'] == []
    assert len(report['shards_without_eligible_replica']) == 1


def test_replica_priority_and_payroll_exclusion():
    data = rows()
    data += [{**data[0], 'used_for': 'GR-Node2', 'mysqlhost': 'other.example.internal'},
             {**data[0], 'mysqldb': 'payroll', 'name': 'bigdata_shard'}]
    result = snapshot(data)
    assert len(result['mysql_shards']) == 1
    assert result['mysql_shards'][0]['host'] == rows()[0]['mysqlhost']


@pytest.mark.parametrize('field,value', [('mysqlhost', "x?password=bad"),
                                        ('clientend', '000')])
def test_invalid_inventory_rejected(field, value):
    data = rows()
    data[0][field] = value
    with pytest.raises(ValueError):
        snapshot(data)


def test_ddl_uses_file_not_credentials():
    ddl = next(iter(statements(snapshot(), '/run/creds/mysql.properties').values()))
    assert 'credential-provider.type" = \'FILE\'' in ddl
    assert 'connection-password' not in ddl
    assert 'sslMode=VERIFY_IDENTITY' in ddl
    assert 'zeroDateTimeBehavior=convertToNull' in ddl


@pytest.mark.parametrize('path', ["/tmp/a'bad", '../a', '/tmp/../a'])
def test_path_injection_rejected(path):
    with pytest.raises(SetupError):
        statements(snapshot(), path)


def test_apply_rerun_skips_existing():
    conn = Mock()
    planned = statements(snapshot(), '/run/creds/mysql.properties')
    conn.cursor.return_value.fetchall.return_value = [[next(iter(planned))]]
    assert apply(conn, planned) == 0
    conn.cursor.return_value.execute.assert_called_once_with('SHOW CATALOGS')
    conn.cursor.return_value.close.assert_called_once()


def test_apply_creates_and_sanitizes_failure():
    conn = Mock()
    conn.cursor.return_value.fetchall.return_value = []
    planned = statements(snapshot(), '/run/creds/mysql.properties')
    assert apply(conn, planned) == 1
    conn.cursor.return_value.execute.side_effect = [None, RuntimeError('password=SECRET')]
    with pytest.raises(SetupError) as err:
        apply(conn, planned)
    assert 'SECRET' not in str(err.value)


def test_credentials_atomic_permissions_and_escaping(tmp_path):
    vault = Mock()
    vault.read.return_value = {'username': ' user', 'password': 'a\\b\nc=é😀'}
    output = tmp_path / 'creds'
    write_credentials(vault, {'SQL_VAULT_PATH': 'trino/mysql'}, output)
    assert output.stat().st_mode & 0o777 == 0o600
    assert output.read_text() == ('connection-user=\\ user\n'
        'connection-password=a\\\\b\\nc\\=\\u00e9\\ud83d\\ude00\n')
    assert len(list(tmp_path.iterdir())) == 1


def test_missing_secret_does_not_overwrite(tmp_path):
    output = tmp_path / 'creds'
    atomic_write(output, 'original')
    vault = Mock()
    vault.read.return_value = {'username': 'user'}
    with pytest.raises(SetupError):
        write_credentials(vault, {'SQL_VAULT_PATH': 'trino/mysql'}, output)
    assert output.read_text() == 'original'


def test_vault_kv2_and_namespace(monkeypatch):
    call = Mock(return_value={'data': {'data': {'username': 'user'}}})
    monkeypatch.setattr(runtime, 'request_json', call)
    vault = Vault({'VAULT_ADDR': 'https://vault.example', 'VAULT_TOKEN': 'secret',
                   'VAULT_NAMESPACE': 'team', 'VAULT_KV_MOUNT': 'kv'})
    assert vault.read('trino/admin') == {'username': 'user'}
    assert call.call_args.args == ('GET', 'https://vault.example/v1/kv/data/trino/admin')
    assert call.call_args.kwargs['headers']['X-Vault-Namespace'] == 'team'


def test_kubernetes_auth(monkeypatch, tmp_path):
    jwt = tmp_path / 'jwt'
    jwt.write_text('service-account-jwt')
    call = Mock(return_value={'auth': {'client_token': 'vault-token'}})
    monkeypatch.setattr(runtime, 'request_json', call)
    vault = Vault({'VAULT_ADDR': 'https://vault.example', 'VAULT_ROLE': 'setup',
                   'VAULT_K8S_TOKEN_FILE': str(jwt)})
    assert call.call_args.kwargs['json'] == {'role': 'setup', 'jwt': 'service-account-jwt'}
    assert vault.headers['X-Vault-Token'] == 'vault-token'


def test_http_failures_sanitized_and_redirects_disabled(monkeypatch):
    response = Mock(status_code=302, text='SECRET')
    call = Mock(return_value=response)
    monkeypatch.setattr(runtime.requests, 'request', call)
    with pytest.raises(SetupError) as err:
        runtime.request_json('GET', 'https://example.internal')
    assert 'SECRET' not in str(err.value)
    assert call.call_args.kwargs['allow_redirects'] is False


@pytest.mark.parametrize('url', ['http://vault.example', 'https://user:secret@host', 'https://host/?token=x'])
def test_secure_urls(url):
    with pytest.raises(SetupError):
        Vault({'VAULT_ADDR': url, 'VAULT_TOKEN': 'secret'})


def test_offline_plan(tmp_path, monkeypatch):
    monkeypatch.setattr(runtime, 'request_json', Mock(side_effect=AssertionError('network')))
    target = tmp_path / 'shards.json'
    assert main(['plan', '--catalogs-only', '--input', 'examples/registry.json', '--env-type', 'TEST',
                 '--output', str(target)]) == 0
    assert json.loads(target.read_text())['mysql_shards']


def test_empty_filter_does_not_publish(tmp_path):
    target = tmp_path / 'shards.json'
    assert main(['plan', '--catalogs-only', '--input', 'examples/registry.json', '--env-type', 'PROD',
                 '--output', str(target)]) == 1
    assert not target.exists()


def test_apply_failure_preserves_snapshot(monkeypatch, tmp_path):
    import trino_setup.__main__ as cli
    target = tmp_path / 'shards.json'
    target.write_text('previous')
    monkeypatch.setattr(cli, 'Vault', Mock())
    connection = Mock()
    connection.cursor.return_value.execute.side_effect = RuntimeError('SECRET')
    monkeypatch.setattr(cli, 'connect_trino', Mock(return_value=connection))
    assert main(['apply', '--catalogs-only', '--input', 'examples/registry.json', '--env-type', 'TEST',
                 '--output', str(target)]) == 1
    assert target.read_text() == 'previous'
    connection.close.assert_called_once()


def test_trino_connection_uses_vault_credentials(monkeypatch):
    import trino.dbapi
    connect = Mock()
    monkeypatch.setattr(trino.dbapi, 'connect', connect)
    vault = Mock()
    vault.read.return_value = {'username': 'admin', 'password': 'private'}
    runtime.connect_trino(vault, {'TRINO_URL': 'https://trino.example:8443',
                                  'TRINO_VAULT_PATH': 'trino/admin'})
    options = connect.call_args.kwargs
    assert options['host'] == 'trino.example'
    assert options['port'] == 8443
    assert options['user'] == 'admin'
    assert options['http_scheme'] == 'https'
    assert options['verify'] is True
    assert options['max_attempts'] == 1
    vault.read.assert_called_once_with('trino/admin')


def test_successful_apply_publishes_and_closes(monkeypatch, tmp_path):
    import trino_setup.__main__ as cli
    target = tmp_path / 'shards.json'
    monkeypatch.setattr(cli, 'Vault', Mock())
    connection = Mock()
    connection.cursor.return_value.fetchall.return_value = []
    monkeypatch.setattr(cli, 'connect_trino', Mock(return_value=connection))
    assert main(['apply', '--catalogs-only', '--input', 'examples/registry.json', '--env-type', 'TEST',
                 '--output', str(target)]) == 0
    assert len(json.loads(target.read_text())['mysql_shards']) == 1
    assert connection.cursor.return_value.execute.call_count == 2
    connection.close.assert_called_once()


@pytest.mark.parametrize('environment', ['TEST', 'PROD'])
@pytest.mark.parametrize('role', ['GR-Node1', 'DC1', 'unknown', '', None])
def test_only_gr_read_replicas_eligible_in_every_environment(environment, role):
    data = rows()
    data[0].update(env_type=environment, used_for=role)
    result, report = build_snapshot(data, mysqldbs=['application'], name='online', source='test')
    assert not result['mysql_shards']
    assert len(report['shards_without_eligible_replica']) == 1


def test_node2_fallback_and_non_online_rows_excluded():
    base = rows()[0]
    data = [{**base, 'used_for': 'GR-Node1'},
            {**base, 'used_for': 'GR-Node2', 'mysqlhost': 'reader2.example.internal'},
            {**base, 'name': 'other', 'clientstart': 'A', 'clientend': 'B'},
            {**base, 'mysqldb': 'analytics', 'clientstart': 'C', 'clientend': 'D'}]
    result = snapshot(data)
    assert len(result['mysql_shards']) == 1
    assert result['mysql_shards'][0]['host'] == 'reader2.example.internal'


@pytest.mark.parametrize('database,name', [('analytics', 'online'), ('application', 'other')])
def test_non_online_scope_cannot_be_requested(database, name):
    with pytest.raises(ValueError, match='Only online catalogs'):
        build_snapshot(rows(), mysqldbs=[database], name=name, source='test')


def test_ineligible_shard_blocks_apply_before_connection(monkeypatch, tmp_path):
    import trino_setup.__main__ as cli
    base = rows()[0]
    inventory = tmp_path / 'registry.json'
    inventory.write_text(json.dumps({'data': [base, {**base, 'used_for': 'GR-Node1',
                                                    'clientstart': 'A', 'clientend': 'B'}]}))
    connect = Mock(side_effect=AssertionError('Must not connect'))
    monkeypatch.setattr(cli, 'connect_trino', connect)
    target = tmp_path / 'shards.json'
    target.write_text('previous')
    assert main(['apply', '--catalogs-only', '--input', str(inventory), '--env-type', 'TEST',
                 '--output', str(target)]) == 1
    connect.assert_not_called()
    assert target.read_text() == 'previous'
