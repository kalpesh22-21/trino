import json
from pathlib import Path
from unittest.mock import Mock

import pytest
import sqlglot
from sqlglot import exp

import trino_setup.__main__ as cli
from trino_setup.registry import build_snapshot
from trino_setup.runtime import SetupError
from trino_setup.views import (ASSETS, EMPLOYEE_VIEWS, VIEW_TABLES, apply_views,
                               load_assets, render_views, split_statements)


def snapshot():
    rows = json.loads(Path('examples/registry.json').read_text())['data']
    return build_snapshot(rows, mysqldbs=['application'], name='online', source='test')[0]


def test_all_six_views_preserve_native_targets_and_security():
    result = render_views(snapshot(), proc_center='pc1')
    assert len(result) == 6
    for (shard, stem), text in result.items():
        statements = split_statements(text)
        assert len(statements) == 2
        assert '{{' not in text
        tree = sqlglot.parse_one(statements[1], read='trino')
        assert tree.this.sql() == f'reporting_warehouse.{shard}.{VIEW_TABLES[stem]}'
        assert tree.args['properties'].sql() == 'SQL SECURITY DEFINER'
        select = tree.args['expression']
        assert bool(list(select.find_all(exp.Exists))) == (stem in EMPLOYEE_VIEWS)
        assert "'pc1' AS proc_center" in select.sql()
        if stem in EMPLOYEE_VIEWS:
            assert any(node.alias_or_name == 'employee_code_num' for node in select.expressions)


def test_rendered_sql_preserves_original_text():
    original = load_assets(ASSETS / 'trino', '.txt')
    for (shard, stem), text in render_views(snapshot(), proc_center='pc2').items():
        assert text == original[stem].replace('{{SHARD_CATALOG}}', shard).replace('{{PROC_CENTER}}', 'pc2')


def test_mixed_centers_and_custom_view_catalog():
    payload = snapshot()
    first = payload['mysql_shards'][0]
    payload['mysql_shards'].append({**first, 'catalog': 'mysql_second'})
    plans = render_views(payload, proc_center='defaultpc', proc_centers={'mysql_second': 'pc2'},
                         view_catalog='reporting')
    assert len(plans) == 12
    assert "'pc2' AS proc_center" in plans['mysql_second', 'employee']
    assert "'defaultpc' AS proc_center" in plans[first['catalog'], 'employee']
    assert 'CREATE VIEW reporting.mysql_second.employee' in plans['mysql_second', 'employee']


@pytest.mark.parametrize('kwargs', [{}, {'proc_center': "pc1' OR TRUE"},
    {'proc_center': 'pc1', 'proc_centers': {'typo': 'pc2'}}, {'proc_center': 'pc1', 'view_catalog': 'bad.name'}])
def test_invalid_identity_and_names_rejected(kwargs):
    with pytest.raises((SetupError, ValueError)):
        render_views(snapshot(), **kwargs)


def test_wrong_database_rejected_before_network():
    payload = snapshot()
    payload['mysql_shards'][0]['database'] = 'analytics'
    with pytest.raises(SetupError, match='application'):
        render_views(payload, proc_center='pc1')


@pytest.mark.parametrize('old,new', [
    ("AND a.username = split_part(CURRENT_USER, '|', 3)", "-- AND a.username = split_part(CURRENT_USER, '|', 3)"),
    ("WHERE e.client_code = split_part(CURRENT_USER, '|', 2)", 'WHERE TRUE'),
    ('AND a.eecode = e.employee_code', 'AND a.eecode = a.eecode'),
    ('SECURITY DEFINER', 'SECURITY INVOKER'),
    ('{{PROC_CENTER}}', '{{UNKNOWN}}'),
])
def test_damaged_security_rejected_even_when_comment_mentions_pin(old, new):
    templates = load_assets(ASSETS / 'trino', '.txt')
    assert old in templates['employee']
    templates['employee'] = templates['employee'].replace(old, new)
    with pytest.raises(SetupError):
        render_views(snapshot(), proc_center='pc1', templates=templates)


def test_semicolon_in_literal_or_comment_does_not_split():
    text = "-- header;\nCREATE VIEW a AS SELECT ';\n' AS value /* ; */; -- tail;\nCREATE VIEW b AS SELECT 2;"
    parts = split_statements(text)
    assert len(parts) == 2
    assert "';\n'" in parts[0]


def test_apply_strict_and_explicit_replace_leave_templates_unchanged():
    plans = render_views(snapshot(), proc_center='pc1')
    before = dict(plans)
    connection = Mock()
    apply_views(connection, plans)
    sent = [call.args[0] for call in connection.cursor.return_value.execute.call_args_list]
    assert sum(sql.startswith('CREATE VIEW ') for sql in sent) == 6
    assert sum(sql.startswith('CREATE SCHEMA IF NOT EXISTS ') for sql in sent) == 6
    connection.reset_mock()
    apply_views(connection, plans, or_replace=True)
    sent = [call.args[0] for call in connection.cursor.return_value.execute.call_args_list]
    assert sum(sql.startswith('CREATE OR REPLACE VIEW ') for sql in sent) == 6
    assert plans == before


def test_view_failure_sanitized():
    connection = Mock()
    connection.cursor.return_value.execute.side_effect = RuntimeError('secret credential SQL body')
    with pytest.raises(SetupError) as err:
        apply_views(connection, render_views(snapshot(), proc_center='pc1'))
    assert 'secret' not in str(err.value)
    connection.cursor.return_value.close.assert_called_once()


def test_full_apply_orders_catalog_before_views(monkeypatch, tmp_path):
    connection = Mock()
    connection.cursor.return_value.fetchall.return_value = []
    monkeypatch.setattr(cli, 'Vault', Mock())
    monkeypatch.setattr(cli, 'connect_trino', Mock(return_value=connection))
    assert cli.main(['apply', '--input', 'examples/registry.json', '--env-type', 'TEST',
        '--proc-center', 'pc1', '--create-memory-view-catalog', '--output', str(tmp_path / 'shards.json')]) == 0
    sent = [call.args[0] for call in connection.cursor.return_value.execute.call_args_list]
    assert sent[0] == 'SHOW CATALOGS'
    assert sent[1].startswith('CREATE CATALOG mysql_shard_')
    assert sent[2] == 'CREATE CATALOG reporting_warehouse USING memory'
    assert sum(sql.startswith('CREATE VIEW ') for sql in sent) == 6
    assert len(list((tmp_path / 'views').rglob('*.sql'))) == 6
    connection.close.assert_called_once()


def test_missing_persistent_catalog_never_silently_creates_memory(monkeypatch, tmp_path):
    connection = Mock()
    connection.cursor.return_value.fetchall.return_value = []
    monkeypatch.setattr(cli, 'Vault', Mock())
    monkeypatch.setattr(cli, 'connect_trino', Mock(return_value=connection))
    assert cli.main(['apply', '--input', 'examples/registry.json', '--env-type', 'TEST',
        '--proc-center', 'pc1', '--output', str(tmp_path / 'shards.json')]) == 1
    connection.cursor.return_value.execute.assert_called_once_with('SHOW CATALOGS')
    assert not (tmp_path / 'shards.json').exists()


def test_missing_center_rejected_before_vault(monkeypatch, tmp_path):
    vault = Mock(side_effect=AssertionError('Must not connect'))
    monkeypatch.setattr(cli, 'Vault', vault)
    monkeypatch.delenv('TRINO_PROC_CENTER', raising=False)
    assert cli.main(['apply', '--input', 'examples/registry.json', '--env-type', 'TEST',
                     '--output', str(tmp_path / 'shards.json')]) == 1
    vault.assert_not_called()


def test_partial_view_failure_does_not_publish_snapshot(monkeypatch, tmp_path):
    target = tmp_path / 'shards.json'
    target.write_text('previous')
    connection = Mock()
    connection.cursor.return_value.fetchall.return_value = [['reporting_warehouse']]
    def execute(sql):
        if sql.startswith('CREATE VIEW'):
            raise RuntimeError('secret SQL')
    connection.cursor.return_value.execute.side_effect = execute
    monkeypatch.setattr(cli, 'Vault', Mock())
    monkeypatch.setattr(cli, 'connect_trino', Mock(return_value=connection))
    assert cli.main(['apply', '--input', 'examples/registry.json', '--env-type', 'TEST',
                     '--proc-center', 'pc1', '--output', str(target)]) == 1
    assert target.read_text() == 'previous'
    connection.close.assert_called_once()


def test_standalone_view_reapply_uses_snapshot_without_registry(monkeypatch, tmp_path):
    target = tmp_path / 'shards.json'
    target.write_text(json.dumps(snapshot()))
    monkeypatch.setattr(cli, 'fetch_registry', Mock(side_effect=AssertionError('registry fetch')))
    connection = Mock()
    connection.cursor.return_value.fetchall.return_value = [['reporting_warehouse']]
    monkeypatch.setattr(cli, 'Vault', Mock())
    monkeypatch.setattr(cli, 'connect_trino', Mock(return_value=connection))
    assert cli.main(['apply-views', '--snapshot', str(target), '--proc-center', 'pc1',
                     '--or-replace', '--rendered-dir', str(tmp_path / 'views')]) == 0
    sent = [call.args[0] for call in connection.cursor.return_value.execute.call_args_list]
    assert not any(sql.startswith('CREATE CATALOG') for sql in sent)
    assert sum(sql.startswith('CREATE OR REPLACE VIEW ') for sql in sent) == 6










def test_proc_center_must_be_emitted_by_view():
    templates = load_assets(ASSETS / 'trino', '.txt')
    templates['employee'] = templates['employee'].replace('AS proc_center', 'AS wrong_column')
    with pytest.raises(SetupError, match='proc-center literal'):
        render_views(snapshot(), proc_center='pc1', templates=templates)




def test_mysql_creation_command_is_unavailable():
    with pytest.raises(SystemExit) as err:
        cli.main(['mysql-views', '--host', 'writer.example', '--port', '3306'])
    assert err.value.code == 2


def test_precreated_views_checked_read_only_before_wrappers(monkeypatch, tmp_path):
    connection = Mock()
    connection.cursor.return_value.fetchall.return_value = [['reporting_warehouse']]
    monkeypatch.setattr(cli, 'Vault', Mock())
    monkeypatch.setattr(cli, 'connect_trino', Mock(return_value=connection))
    assert cli.main(['apply', '--input', 'examples/registry.json', '--env-type', 'TEST',
        '--proc-center', 'pc1', '--output', str(tmp_path / 'shards.json')]) == 0
    sent = [call.args[0] for call in connection.cursor.return_value.execute.call_args_list]
    checks = [sql for sql in sent if sql.startswith('SELECT ')]
    assert len(checks) == 7
    assert all(sql.endswith(' LIMIT 0') for sql in checks)
    assert max(sent.index(sql) for sql in checks) < next(i for i, sql in enumerate(sent) if sql.startswith('CREATE VIEW'))
    assert not any(sql.startswith('CREATE VIEW application.') for sql in sent)


def test_missing_precreated_view_prevents_wrappers_and_snapshot(monkeypatch, tmp_path, capsys):
    target = tmp_path / 'shards.json'
    target.write_text('previous')
    connection = Mock()
    connection.cursor.return_value.fetchall.return_value = [['reporting_warehouse']]
    def execute(sql):
        if sql.startswith('SELECT ') and 'reporting_employee' in sql:
            raise RuntimeError('sensitive remote details')
    connection.cursor.return_value.execute.side_effect = execute
    monkeypatch.setattr(cli, 'Vault', Mock())
    monkeypatch.setattr(cli, 'connect_trino', Mock(return_value=connection))
    assert cli.main(['apply', '--input', 'examples/registry.json', '--env-type', 'TEST',
        '--proc-center', 'pc1', '--output', str(target)]) == 1
    sent = [call.args[0] for call in connection.cursor.return_value.execute.call_args_list]
    assert not any(sql.startswith(('CREATE VIEW', 'CREATE SCHEMA')) for sql in sent)
    assert target.read_text() == 'previous'
    assert 'sensitive remote details' not in capsys.readouterr().err
    connection.close.assert_called_once()
