"""Network and secret IO. Errors deliberately omit remote bodies and credentials."""
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import quote, urlsplit

import requests


class SetupError(RuntimeError):
    pass


def required(env, key):
    value = env.get(key, '')
    if not isinstance(value, str) or not value.strip():
        raise SetupError(f'Missing {key}')
    return value


def https_url(value):
    parsed = urlsplit(value)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username
            or parsed.password or parsed.fragment or parsed.query):
        raise SetupError('Expected an HTTPS URL without credentials, query or fragment')
    return value.rstrip('/')


def api_path(value):
    if not re.fullmatch(r'[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*', value):
        raise SetupError('Invalid Vault path')
    return quote(value, safe='/')


def request_json(method, url, **kwargs):
    try:
        response = requests.request(method, url, timeout=30, allow_redirects=False, **kwargs)
        if response.status_code != 200:
            raise SetupError(f'HTTP request failed (status {response.status_code})')
        return response.json()
    except (requests.RequestException, ValueError):
        raise SetupError('HTTPS request failed or returned invalid JSON') from None


class Vault:
    """KV v2 via token, token file, or Kubernetes service-account login."""
    def __init__(self, env=os.environ):
        self.addr = https_url(required(env, 'VAULT_ADDR'))
        self.mount = api_path(env.get('VAULT_KV_MOUNT', 'secret'))
        self.verify = env.get('VAULT_CACERT') or True
        self.headers = {}
        if env.get('VAULT_NAMESPACE'):
            self.headers['X-Vault-Namespace'] = env['VAULT_NAMESPACE']
        token = env.get('VAULT_TOKEN')
        if not token and env.get('VAULT_TOKEN_FILE'):
            token = Path(env['VAULT_TOKEN_FILE']).read_text().strip()
        if not token:
            role = required(env, 'VAULT_ROLE')
            jwt = Path(env.get('VAULT_K8S_TOKEN_FILE',
                '/var/run/secrets/kubernetes.io/serviceaccount/token')).read_text().strip()
            mount = api_path(env.get('VAULT_AUTH_MOUNT', 'kubernetes'))
            result = request_json('POST', f'{self.addr}/v1/auth/{mount}/login',
                headers=self.headers, verify=self.verify, json={'role': role, 'jwt': jwt})
            try:
                token = result['auth']['client_token']
            except (KeyError, TypeError):
                raise SetupError('Vault authentication returned no token') from None
        if not isinstance(token, str) or not token.strip():
            raise SetupError('Vault authentication returned an empty token')
        self.headers['X-Vault-Token'] = token

    def read(self, path):
        result = request_json('GET', f'{self.addr}/v1/{self.mount}/data/{api_path(path)}',
            headers=self.headers, verify=self.verify)
        try:
            data = result['data']['data']
        except (KeyError, TypeError):
            raise SetupError('Vault secret is missing or has an invalid KV v2 response') from None
        if not isinstance(data, dict):
            raise SetupError('Vault secret must be an object')
        return data


def fetch_registry(url, ca=None):
    payload = request_json('GET', https_url(url), verify=ca or True)
    if not isinstance(payload, dict) or not isinstance(payload.get('data'), list):
        raise SetupError('Registry must return an object with a data array')
    return payload['data']


def atomic_write(path, text):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def java_value(value):
    """ASCII Java Properties encoding, including Unicode surrogate pairs."""
    if not isinstance(value, str) or not value:
        raise SetupError('Credential fields must be non-empty strings')
    escaped = {'\\': '\\\\', '\n': '\\n', '\r': '\\r', '\t': '\\t', '\f': '\\f',
               ' ': '\\ ', '=': '\\=', ':': '\\:', '#': '\\#', '!': '\\!'}
    out = ''
    for char in value:
        if char in escaped:
            out += escaped[char]
        elif 32 <= ord(char) < 127:
            out += char
        else:
            encoded = char.encode('utf-16-be')
            out += ''.join('\\u' + encoded[i:i+2].hex() for i in range(0, len(encoded), 2))
    return out


def write_credentials(vault, env, output):
    secret = vault.read(required(env, 'SQL_VAULT_PATH'))
    user = required(secret, env.get('SQL_USER_KEY', 'username'))
    password = required(secret, env.get('SQL_PASSWORD_KEY', 'password'))
    atomic_write(output, f'connection-user={java_value(user)}\nconnection-password={java_value(password)}\n')


def connect_trino(vault, env):
    from trino.auth import BasicAuthentication
    from trino.dbapi import connect
    secret = vault.read(required(env, 'TRINO_VAULT_PATH'))
    user = required(secret, env.get('TRINO_USER_KEY', 'username'))
    password = required(secret, env.get('TRINO_PASSWORD_KEY', 'password'))
    endpoint = urlsplit(https_url(required(env, 'TRINO_URL')))
    if endpoint.path not in ('', '/'):
        raise SetupError('TRINO_URL must contain only scheme, host and port')
    return connect(host=endpoint.hostname, port=endpoint.port or 443,
        user=user, auth=BasicAuthentication(user, password), http_scheme='https',
        verify=env.get('TRINO_CA_BUNDLE') or True, request_timeout=30,
        max_attempts=1, source='trino-catalog-setup')
