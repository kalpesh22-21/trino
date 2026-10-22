"""Reporting view assets, validation, rendering and Trino application."""
from pathlib import Path
import re

import sqlglot
from sqlglot import exp
from sqlglot.tokens import TokenType

from .catalogs import query
from .registry import validate_identifier, validate_shard_registry
from .runtime import SetupError, atomic_write

ASSETS = Path(__file__).parent / 'assets'
VIEW_TABLES = {
    'employee': 'employee',
    'expense': 'expense',
    'accrual_events': 'accrual_events',
    'accrual_balances': 'accrual_balances',
    'department': 'department',
    'labour_allocation': 'labor_allocation',
}
EMPLOYEE_VIEWS = {'employee', 'expense', 'accrual_events', 'accrual_balances'}


def split_statements(text, dialect='trino'):
    """Split using SQL token offsets; semicolons in strings/comments are safe.

    Original statement text is retained (no AST SQL rewrite of business logic).
    Leading/trailing comments outside statements are omitted.
    """
    tokens = sqlglot.Dialect.get_or_raise(dialect).tokenize(text)
    statements = []
    start = None
    last_end = None
    for token in tokens:
        if token.token_type == TokenType.SEMICOLON:
            if start is not None:
                statements.append(text[start:token.start].strip())
                start = None
                last_end = None
        else:
            if start is None:
                start = token.start
            last_end = token.end + 1
    if start is not None:
        statements.append(text[start:last_end].strip())
    return statements


def load_assets(directory, suffix):
    result = {path.stem: path.read_text(encoding='utf-8')
              for path in sorted(Path(directory).glob('*' + suffix))}
    if set(result) != set(VIEW_TABLES):
        raise SetupError('View assets must contain exactly the six reporting views')
    return result


def _create(statement, kind, target, dialect='trino'):
    tree = sqlglot.parse_one(statement, read=dialect)
    if (not isinstance(tree, exp.Create) or tree.args.get('kind') != kind
            or tree.this.sql() != target or tree.args.get('replace')):
        raise SetupError(f'Expected strict CREATE {kind} for {target}')
    return tree


def _conjuncts(expression):
    if isinstance(expression, exp.And):
        return _conjuncts(expression.this) + _conjuncts(expression.expression)
    return [expression]


def _has_predicates(expression, predicates):
    parts = _conjuncts(expression)
    return all(sqlglot.parse_one(predicate, read='trino') in parts for predicate in predicates)


def _validate_rls(tree, shard, table, center):
    select = tree.args.get('expression')
    if not isinstance(select, exp.Select) or not select.args.get('where'):
        raise SetupError('Reporting wrapper must have a SELECT with row-security predicates')
    identity = [item for item in select.expressions if item.alias == 'proc_center']
    if len(identity) != 1 or identity[0].this != exp.Literal.string(center):
        raise SetupError('Reporting wrapper must emit the configured proc-center literal')
    properties = tree.args.get('properties')
    if properties is None or properties.sql() != 'SQL SECURITY DEFINER':
        raise SetupError('Reporting wrapper must use SECURITY DEFINER')
    if select.args.get('joins') or select.args.get('distinct') or select.args.get('group'):
        raise SetupError('Business joins and aggregation belong in the native MySQL view')
    source = select.args.get('from_')
    if source is None or source.this.sql() != f'{shard}.application.reporting_{table} AS e':
        raise SetupError('Reporting wrapper must reference its native MySQL reporting view')
    where = select.args['where'].this
    if not _has_predicates(where, ["e.client_code = split_part(CURRENT_USER, '|', 2)"]):
        raise SetupError('Reporting wrapper lost its client-code predicate')
    if table in EMPLOYEE_VIEWS:
        allowed = False
        for predicate in _conjuncts(where):
            if not isinstance(predicate, exp.Exists):
                continue
            lookup = predicate.this
            source = lookup.args.get('from_')
            restriction = lookup.args.get('where')
            if (source is not None and source.this.sql() == f'{shard}.application.employee_access AS a'
                    and restriction is not None and _has_predicates(restriction.this, [
                        "a.clientcode = split_part(CURRENT_USER, '|', 2)",
                        "a.username = split_part(CURRENT_USER, '|', 3)",
                        'a.clientcode = e.client_code', 'a.eecode = e.employee_code'])):
                allowed = True
        if not allowed:
            raise SetupError('Reporting wrapper lost its correlated employee-access predicate')
    # This is deliberately checked in parsed SQL, never in explanatory comments.
    body = select.sql(dialect='trino')
    if re.search(r"SPLIT_PART\(CURRENT_USER, '\|', 1\)", body, re.I):
        raise SetupError('The retired proc-center row predicate must not be restored')


def render_views(snapshot, *, proc_center=None, proc_centers=None,
                 view_catalog='reporting_warehouse', templates=None):
    """Validate the complete plan before any mutation; map (shard, stem) to SQL."""
    validate_identifier(view_catalog)
    entries = validate_shard_registry(snapshot)
    if not entries:
        raise SetupError('No shards selected for view creation')
    if any(entry['database'] != 'application' for entry in entries):
        raise SetupError('Bundled reporting views require application; use --catalogs-only for other databases')
    if view_catalog in {entry['catalog'] for entry in entries}:
        raise SetupError('View catalog must be separate from source catalogs')
    templates = templates if templates is not None else load_assets(ASSETS / 'trino', '.txt')
    if set(templates) != set(VIEW_TABLES):
        raise SetupError('View assets must contain exactly the six reporting views')
    centers = {} if proc_centers is None else proc_centers
    if not isinstance(centers, dict) or set(centers) - {e['catalog'] for e in entries}:
        raise SetupError('Proc-center map must be keyed by selected shard catalog names')
    rendered = {}
    for entry in entries:
        shard = entry['catalog']
        center = centers.get(shard) or proc_center
        if not center:
            raise SetupError(f'Missing proc center for {shard}; set --proc-center or --proc-centers')
        validate_identifier(center)
        for stem, template in templates.items():
            if '{{SHARD_CATALOG}}' not in template or '{{PROC_CENTER}}' not in template:
                raise SetupError(f'Missing view placeholders in {stem}')
            text = (template.replace('{{SHARD_CATALOG}}', shard)
                    .replace('{{PROC_CENTER}}', center)
                    .replace('reporting_warehouse.', view_catalog + '.'))
            if '{{' in text or '}}' in text:
                raise SetupError(f'Unresolved view placeholders in {stem}')
            statements = split_statements(text)
            if len(statements) != 2:
                raise SetupError('Each Trino asset must contain CREATE SCHEMA then CREATE VIEW')
            schema = _create(statements[0], 'SCHEMA', f'{view_catalog}.{shard}')
            if not schema.args.get('exists'):
                raise SetupError('View schema creation must use IF NOT EXISTS')
            tree = _create(statements[1], 'VIEW', f'{view_catalog}.{shard}.{VIEW_TABLES[stem]}')
            _validate_rls(tree, shard, VIEW_TABLES[stem], center)
            rendered[shard, stem] = text
    return rendered


def write_rendered(rendered, directory):
    for (shard, stem), text in rendered.items():
        atomic_write(Path(directory) / shard / (stem + '.sql'), text)


def apply_views(connection, rendered, *, or_replace=False):
    """Fail on existing views unless replacement was explicitly requested."""
    for (shard, stem), text in rendered.items():
        try:
            for statement in split_statements(text):
                if or_replace:
                    statement = re.sub(r'^CREATE\s+VIEW\b', 'CREATE OR REPLACE VIEW', statement, count=1)
                query(connection, statement)
        except Exception:
            raise SetupError(f'View creation failed: {shard}/{stem}; prior DDL remains. '
                             'Check prerequisites; use --or-replace only for intentional replacement') from None
        print(f'applied view: {shard}/{VIEW_TABLES[stem]}')


def check_source_views(connection, rendered):
    """Read-only metadata checks through Trino; source views are externally owned."""
    shards = sorted({shard for shard, _ in rendered})
    for shard in shards:
        validate_identifier(shard)
        sources = [f'reporting_{table}' for table in VIEW_TABLES.values()]
        for source in sources + ['employee_access']:
            columns = 'clientcode, username, eecode' if source == 'employee_access' else '*'
            try:
                query(connection, f'SELECT {columns} FROM {shard}.application.{source} LIMIT 0')
            except Exception:
                raise SetupError(f'Precreated MySQL dependency unavailable: {shard}.application.{source}; '
                                 'check reader permissions, source deployment and replication. '
                                 'No Trino wrapper views were applied') from None
