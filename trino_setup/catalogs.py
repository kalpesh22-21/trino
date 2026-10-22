"""Catalog DDL never contains source credentials."""
import re
from .registry import validate_identifier, validate_shard_registry
from .runtime import SetupError


def statements(snapshot, credential_file, ssl_mode='VERIFY_IDENTITY'):
    entries = validate_shard_registry(snapshot)
    if not entries:
        raise SetupError('No shards selected; refusing empty setup')
    if not re.fullmatch(r'/[A-Za-z0-9_./-]+', credential_file) or '..' in credential_file.split('/'):
        raise SetupError('Credential file must be an absolute path without traversal')
    if ssl_mode not in ('VERIFY_IDENTITY', 'VERIFY_CA', 'REQUIRED', 'PREFERRED', 'DISABLED'):
        raise SetupError('Invalid MYSQL_SSL_MODE')
    result = {}
    for entry in entries:
        name = validate_identifier(entry['catalog'])
        url = (f"jdbc:mysql://{entry['host']}:{entry['port']}"
               f"?zeroDateTimeBehavior=convertToNull&sslMode={ssl_mode}")
        result[name] = (f'CREATE CATALOG {name} USING mysql WITH (\n'
            f'  "connection-url" = \'{url}\',\n'
            '  "credential-provider.type" = \'FILE\',\n'
            f'  "connection-credential-file" = \'{credential_file}\',\n'
            '  "metadata.cache-ttl" = \'10m\',\n'
            '  "metadata.cache-missing" = \'true\'\n)')
    return result


def query(connection, sql):
    cursor = connection.cursor()
    try:
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        cursor.close()


def apply(connection, planned):
    """Create missing catalogs only. Existing names are reported, never replaced."""
    existing = {row[0] for row in query(connection, 'SHOW CATALOGS')}
    created = 0
    for name, sql in planned.items():
        if name in existing:
            print(f'exists: {name} (configuration not reconciled)')
            continue
        try:
            query(connection, sql)
        except Exception:
            raise SetupError(f'Catalog creation failed: {name}; earlier creations remain, rerun after repair') from None
        created += 1
        print(f'created: {name}')
    return created
