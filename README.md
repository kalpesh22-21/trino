# Trino catalog and view setup

A Kubernetes Trino cluster deployment and a separate setup Job that fetches MySQL shard metadata from `registry.json` and creates catalogs and secured reporting views on the Trino cluster. MySQL reporting views are precreated and managed outside this repository. The cluster manifest deploys one coordinator and two workers; the setup Job creates objects in Trino and never performs MySQL DDL. Credentials for the setup connection and MySQL connectors come from Vault KV v2.

## Flow

1. On every Trino coordinator and worker, an init container reads the SQL username/password from Vault and writes a mode-0600 Java properties file to a shared, memory-backed volume.
2. The setup Job fetches the registry once, filters to explicit environments and databases, selects a read replica, and validates all catalog definitions before connecting.
3. The Job reads its Trino admin username/password from Vault and sends `CREATE CATALOG` over verified HTTPS. Catalogs reference the credential **file path**, never inline SQL credentials.
4. The Job renders and creates six Trino reporting wrappers per shard, preserving the client-code and employee-access predicates. MySQL reporting views must already exist. Before creating any wrappers, the Job checks their availability and the required `employee_access` columns through read-only Trino queries.
5. After successful catalog and view application, it atomically writes `shards.json` for the API's `TRINO_SHARD_REGISTRY_PATH`. Restart the API after distributing an updated snapshot because routing is cached.

The SQL credential file must exist at the same path on **all** Trino nodes before creating catalogs, including future workers. Writing it only in the setup Job does not configure a remote Trino server. The init-container example handles this without giving the setup Job SQL credentials.

## Trino prerequisites

Target: Trino 483 and MySQL connector. `deploy/cluster.yaml` supplies these coordinator properties. When using an existing cluster, configure them there:

```properties
catalog.management=dynamic
catalog.store=file
catalog.read-only=false
```

Persist the coordinator's catalog directory and make it writable by Trino so catalogs survive restarts. Grant the Vault-sourced setup user permission to list and create catalogs. Provision the MySQL account with read-only access to the intended schemas. Mount the same SQL credential file on coordinator and workers. MySQL TLS defaults to `VERIFY_IDENTITY`; install the source CA in the Trino JVM truststore if it is private. HTTPS CA settings for the setup client do not configure the MySQL JDBC truststore.

Trino labels dynamic catalog management experimental. Catalog SQL is visible in server history; this deployment uses `credential-provider.type=FILE` to keep passwords out of that SQL. See [catalog management](https://trino.io/docs/current/admin/properties-catalog.html), [CREATE CATALOG](https://trino.io/docs/current/sql/create-catalog.html), and [MySQL credential providers](https://trino.io/docs/current/connector/mysql.html#data-source-authentication).

## Registry contract

Set `REGISTRY_URL` to your HTTPS metadata endpoint. The example endpoint is a placeholder. This implementation accepts the following response contract:

```json
{"data":[{"env_type":"TEST","mysqldb":"application","name":"online","mysqlhost":"mysql-reader.example.internal","mysqlport":3306,"clientstart":"00000","clientend":"ZZZZZ","used_for":"GR-Node3"}]}
```

`mysqlip` is the fallback when `mysqlhost` is empty. Only `name=online` rows in the `application` schema are selected. Only GR-Node3 and GR-Node2 are eligible, in that preference order, in every environment. GR-Node1 and all other roles are never selected. Payroll/bigdata shards are excluded. Unusable registry rows, groups lacking eligible replicas, invalid fields, and empty selections stop setup. The registry request currently requires network access but sends no application authentication; an authenticated endpoint needs an explicit adapter once its contract is known. Redirects are rejected.

Catalog names use the `mysql_shard_<env>_<16-character-range-hash>` format. Filters or replica changes do not rename a range. The snapshot contains `mysql_shards`, filter metadata, source and timestamp. It contains no credentials. This repository owns the Trino wrappers and MySQL connector catalogs. The MySQL reporting views are precreated external dependencies; their definitions and deployment tools are not packaged here. ClickHouse catalogs and cluster access-control/identity configuration remain separate responsibilities. See [view deployment](docs/views.md) for the complete order of operations.

## Local use

```sh
python3.12 -m venv .venv
.venv/bin/pip install -e '.[test]'
# Offline plan: no Vault, Trino, or source database connections.
.venv/bin/trino-setup plan --input examples/registry.json --env-type TEST --proc-center pc1
```

Copy `.env.example` to `.env`, configure endpoints and Vault paths, and export its variables using your deployment tooling. The application intentionally does not auto-load `.env`. Vault auth supports `VAULT_TOKEN`, `VAULT_TOKEN_FILE`, or Kubernetes login using `VAULT_ROLE` and a service-account JWT. Token/file takes precedence. Vault paths are relative to `VAULT_KV_MOUNT`, without `/data/`. Credentials are required, with no plaintext fallback. Vault secrets each contain `username` and `password`; configurable `*_USER_KEY`/`*_PASSWORD_KEY` map existing field names. KV v1 and leased database-engine credentials are not implemented. See [Vault KV v2](https://developer.hashicorp.com/vault/api-docs/secret/kv/kv-v2) and [Kubernetes authentication](https://developer.hashicorp.com/vault/api-docs/auth/kubernetes).

```sh
# On every Trino host, as the Trino OS user; path must be mounted into Trino.
.venv/bin/trino-setup credentials --output /run/trino-secrets/mysql.properties
# In the deployment runner, after the existing cluster is ready:
.venv/bin/trino-setup plan --env-type TEST --proc-center pc1
.venv/bin/trino-setup apply --env-type TEST --proc-center pc1
# Optional client filter: --client C0001
# Skip wrapper creation while retaining the same online-only scope: --catalogs-only
```

`plan` discovers metadata, validates all six templates per shard, and writes a snapshot plus rendered SQL without reading secrets or checking the remote cluster. Supply `--proc-center`, `TRINO_PROC_CENTER`, or a `--proc-centers` JSON map; there is no default identity. `--catalogs-only` retains the catalog-only workflow. Keep plan output separate from the snapshot distributed to the API. `apply` creates missing catalogs and skips existing catalog names, explicitly reporting that their configuration is not reconciled. It then creates reporting views with strict CREATE; use `--or-replace` for intentional view updates. The view catalog must already exist and support views; the default name is `reporting_warehouse`. For development only, `--create-memory-view-catalog` can create a volatile catalog. Memory views require reapplication after every coordinator restart. Do not run concurrent setup Jobs. On partial failure, already-created catalogs remain; no snapshot is published and a nonzero exit is returned. Rerun after repair. Requests are bounded; DDL is not automatically retried.

**Changes and rotation:** existing catalogs are never dropped or replaced automatically. A changed replica endpoint under the same catalog name requires a scheduled manual catalog replacement; rerunning setup alone does not update it. Coordinate catalog replacement and routing-snapshot publication. Vault credential rotation requires rerunning credential initialization on every Trino node (a rolling restart in Kubernetes); do not assume a running connector reloads the file. Keep old/new DB credentials valid across that rollout. Removed registry entries do not delete old catalogs. Review and remove them separately.

## Kubernetes deployment

```sh
docker build -t registry.example.internal/trino-catalog-setup:0.1.0 .
# Push using your normal image release process.
```

Use [the cluster deployment guide](docs/cluster.md) for prerequisite secrets, deployment order, verification, scaling, and migration. `deploy/cluster.yaml` provides one dedicated coordinator, two workers, service discovery, a coordinator data PVC, and Vault credential initialization on every node. `deploy/job.yaml` connects to `https://trino-coordinator:8443` and trusts the CA in `trino-client-ca`.

For an **existing cluster**, retain its workloads and merge `deploy/trino-pod-fragment.yaml` into both coordinator and worker pod specs instead. Configure dynamic catalogs and persistent coordinator storage, and adjust the Job's URL and CA mount to that cluster. Run only one setup Job at a time. The setup Job also requires the precreated MySQL reporting views and a persistent Trino view catalog.

The Job has no Kubernetes API write permissions and does not grant itself Trino/Vault permissions. Logs omit passwords and remote error bodies. Keep snapshot access restricted because it includes internal hostnames.

## Verification

```sh
.venv/bin/python -m pytest -q
```

Tests parse all six bundled Trino templates and mock Vault, registry HTTP and Trino. They verify read-only checks of precreated MySQL dependencies and the absence of a MySQL view-creation command. Live verification requires actual endpoints, Vault paths/roles, and a running cluster with the prerequisites above.

## Generic deployment contract

All bundled names are neutral examples. The metadata endpoint uses `REGISTRY_URL` (and optional `REGISTRY_CA_BUNDLE`); the online source schema is `application`, the view catalog defaults to `reporting_warehouse`, and the bundled wrappers reference `application.reporting_*` plus `application.employee_access`. Allowed replica roles are `GR-Node3` and `GR-Node2` only. Adapt your metadata and source objects to this contract before deployment. Changing environment or database identifiers changes generated catalog hashes; regenerate routing snapshots alongside catalog setup.
