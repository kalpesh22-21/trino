import argparse
import json
import os
from pathlib import Path
import sys

from .catalogs import apply, query, statements
from .registry import build_snapshot
from .runtime import (SetupError, Vault, atomic_write, connect_trino,
                      fetch_registry, required, write_credentials)
from .views import apply_views, check_source_views, render_views, write_rendered


def _view_options(cmd):
    cmd.add_argument('--proc-center', help='Proc-center literal for all selected shards; or TRINO_PROC_CENTER')
    cmd.add_argument('--proc-centers', type=Path, help='JSON object mapping shard catalog names to proc centers')
    cmd.add_argument('--view-catalog', default='reporting_warehouse')
    cmd.add_argument('--rendered-dir', type=Path)
    cmd.add_argument('--create-memory-view-catalog', action='store_true',
                     help='Development only: create a RAM-backed view catalog if missing')
    cmd.add_argument('--or-replace', action='store_true', help='Intentionally replace existing views')


def _view_plan(args, snapshot, env):
    centers = json.loads(args.proc_centers.read_text()) if args.proc_centers else None
    return render_views(snapshot, proc_center=args.proc_center or env.get('TRINO_PROC_CENTER'),
                        proc_centers=centers, view_catalog=args.view_catalog)


def _apply_trino(args, env, planned, rendered):
    connection = connect_trino(Vault(env), env)
    try:
        if rendered:
            if args.create_memory_view_catalog:
                planned = {**planned, args.view_catalog: f'CREATE CATALOG {args.view_catalog} USING memory'}
            else:
                available = {row[0] for row in query(connection, 'SHOW CATALOGS')}
                if args.view_catalog not in available:
                    raise SetupError('Provision the persistent view catalog first, or explicitly select '
                                     '--create-memory-view-catalog for development')
        if planned:
            apply(connection, planned)
        if rendered:
            check_source_views(connection, rendered)
            apply_views(connection, rendered, or_replace=args.or_replace)
    finally:
        connection.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description='Set up catalogs and reporting views on an existing Trino cluster')
    sub = parser.add_subparsers(dest='command', required=True)
    for command in ('plan', 'apply'):
        cmd = sub.add_parser(command, help='Catalogs and Trino reporting wrappers')
        cmd.add_argument('--input', type=Path, help='Offline registry JSON export')
        cmd.add_argument('--output', type=Path, default=Path('output/shards.json'))
        cmd.add_argument('--env-type', action='append', required=True)
        cmd.add_argument('--mysqldb', action='append', choices=['application'])
        cmd.add_argument('--name', choices=['online'], default='online')
        cmd.add_argument('--client', action='append')
        cmd.add_argument('--catalogs-only', action='store_true', help='Skip reporting view creation')
        _view_options(cmd)
    for command in ('plan-views', 'apply-views'):
        cmd = sub.add_parser(command, help='Render/apply Trino views from an existing routing snapshot')
        cmd.add_argument('--snapshot', type=Path, required=True)
        _view_options(cmd)
    creds = sub.add_parser('credentials', help='Run on EVERY Trino node before catalog setup')
    creds.add_argument('--output', type=Path, default=Path('/run/trino-secrets/mysql.properties'))
    args = parser.parse_args(argv)
    env = os.environ
    stage = 'configuration'
    try:
        if args.command == 'credentials':
            stage = 'Vault SQL credential materialization'
            write_credentials(Vault(env), env, args.output)
            print('SQL credential file written (mode 0600)')
            return 0
        if args.command in ('plan-views', 'apply-views'):
            stage = 'snapshot and reporting view validation'
            snapshot = json.loads(args.snapshot.read_text())
            rendered = _view_plan(args, snapshot, env)
            if args.command == 'apply-views':
                stage = 'Vault authentication and Trino view setup'
                _apply_trino(args, env, {}, rendered)
            write_rendered(rendered, args.rendered_dir or Path('output/views'))
            print(f'{args.command}: {len(rendered)} reporting views')
            return 0
        if args.catalogs_only and (args.or_replace or args.create_memory_view_catalog):
            raise SetupError('View creation options cannot be combined with --catalogs-only')
        stage = 'registry fetch and validation'
        if args.input:
            payload = json.loads(args.input.read_text())
            if not isinstance(payload, dict) or not isinstance(payload.get('data'), list):
                raise SetupError('Registry must return an object with a data array')
            rows = payload['data']
            source = 'offline-export'
        else:
            source = required(env, 'REGISTRY_URL')
            rows = fetch_registry(source, env.get('REGISTRY_CA_BUNDLE'))
        snapshot, report = build_snapshot(rows, mysqldbs=args.mysqldb or ['application'],
            name=args.name, source=source, env_types=args.env_type, clients=args.client)
        if report['unusable_rows'] or report['shards_without_eligible_replica']:
            raise SetupError('Registry has unusable rows or shards without GR-Node2/GR-Node3 replicas; repair inventory')
        planned = statements(snapshot, env.get('SQL_CREDENTIAL_FILE', '/run/trino-secrets/mysql.properties'),
                             env.get('MYSQL_SSL_MODE', 'VERIFY_IDENTITY'))
        stage = 'reporting view validation'
        rendered = {} if args.catalogs_only else _view_plan(args, snapshot, env)
        if args.command == 'apply':
            stage = 'Vault authentication and Trino catalog/view setup'
            _apply_trino(args, env, planned, rendered)
        else:
            for name in planned:
                print(f'planned catalog: {name}')
            if rendered and args.create_memory_view_catalog:
                print(f'planned development memory catalog: {args.view_catalog}')
        stage = 'snapshot and rendered-view publication'
        if rendered:
            write_rendered(rendered, args.rendered_dir or args.output.parent / 'views')
        atomic_write(args.output, json.dumps(snapshot, indent=2) + '\n')
        print(f'{args.command}: {len(planned)} source catalogs, {len(rendered)} reporting views; '
              f'snapshot written to {args.output}')
        return 0
    except SetupError as exc:
        print(f'error: {exc}', file=sys.stderr)
    except Exception:
        # Driver errors can embed SQL, authentication headers or server bodies.
        print(f'error: {stage} failed; remote details suppressed', file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())
