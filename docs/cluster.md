# Trino cluster deployment

`deploy/cluster.yaml` deploys Trino 483 as one dedicated coordinator and two workers. Clients and the catalog setup Job use the `trino-coordinator` Service on HTTPS port 8443. Workers discover the coordinator through the same Service on internal port 8080. Scale only the worker Deployment; multiple independent coordinators behind this Service are not supported.

The official image supplies its JVM configuration and a unique node ID from each pod hostname. All nodes share `node.environment=cluster` and an internal authentication secret. Each pod fetches the MySQL reader credential from Vault before Trino starts, including newly scaled workers. Dynamic catalog definitions are propagated by the coordinator to workers; only the coordinator needs a persistent catalog directory. See [Trino deployment](https://trino.io/docs/current/installation/deployment.html) and [dynamic catalogs](https://trino.io/docs/current/admin/properties-catalog.html).

## Configure before applying

Use the same Kubernetes namespace for the cluster, its secrets, and the setup Job. Replace example images, Vault and registry endpoints, Vault paths/roles, environment, and `TRINO_PROC_CENTER` in the manifests. Build and push the setup image as described in the README. Bind Vault role `trino-sql-reader` to service account `trino` and the selected namespace; bind the setup role separately to `trino-catalog-setup`. The policies remain separate as described in `deploy/vault-policies.hcl`.

Provision these Kubernetes Secrets through your normal secret delivery process. No secret values are bundled:

| Secret | Required key | Contents |
| --- | --- | --- |
| `trino-internal` | `shared-secret` | Strong random shared value used by all Trino nodes |
| `trino-server-tls` | `server.pem` | PEM private key and certificate chain; certificate SAN must include `trino-coordinator` for the Job URL |
| `trino-password-auth` | `password.db` | Trino password file with bcrypt or PBKDF2 hashes; include the admin username/password stored in Vault |
| `trino-access-control` | `rules.json` | Trino file-based authorization rules for setup DDL and consumer access |
| `trino-client-ca` | `ca.crt` | CA bundle used to verify the coordinator certificate from the setup Job |

The coordinator mounts its authentication and authorization files; workers use the shared secret. Configure authorization so consumers cannot impersonate the setup user or bypass reporting wrappers by querying source catalogs directly. See [password files](https://trino.io/docs/current/security/password-file.html), [file-based authorization](https://trino.io/docs/current/security/file-system-access-control.html), and [TLS](https://trino.io/docs/current/security/tls.html).

Internal node traffic uses HTTP with shared-secret authentication, not encryption. Keep port 8080 and worker ports on a trusted cluster network; do not expose them through a public Service or ingress. Encrypted internal traffic requires the additional [internal TLS configuration](https://trino.io/docs/current/security/internal-communication.html). This manifest does not provision ingress or network policies.

If Vault uses a private CA, mount it into the credential init containers and setup Job and set `VAULT_CACERT`. A private MySQL CA must be trusted by the JVM on every Trino node; the setup Job's CA bundle does not configure JDBC trust.

The example requests 1 CPU and 4 GiB per Trino pod, with limits of 2 CPUs and 4 GiB. Size these for your workload. The image's JVM uses 80% of container memory for heap. The coordinator PVC requests 10 GiB using the default StorageClass; choose an appropriate class for your cluster. Its `Recreate` strategy prevents simultaneous coordinators and competing catalog writes to the same volume. Workers use ephemeral local storage. Preferred pod anti-affinity spreads pods when capacity allows; this is not coordinator high availability.

## Deploy and verify

Run these commands in the namespace containing the prerequisite Secrets:

```sh
kubectl apply -f deploy/cluster.yaml
kubectl rollout status deployment/trino-coordinator --timeout=10m
kubectl rollout status deployment/trino-worker --timeout=10m
kubectl get pods -l app=trino
```

Before running setup, connect with a trusted HTTPS SQL client and the setup identity and check membership:

```sql
SELECT node_id, coordinator, state FROM system.runtime.nodes;
```

Expect one active coordinator and two active workers. Pod readiness checks `/v1/info`; it does not prove worker registration or source database connectivity. Run a representative source query after setup to verify distributed execution.

Provision the persistent, view-capable `reporting_warehouse` catalog and its external metastore/storage before running the setup Job. A persisted connector properties file does not itself persist views. The source MySQL reporting views must already exist. Then:

```sh
kubectl apply -f deploy/job.yaml
kubectl wait --for=condition=complete job/trino-catalog-setup --timeout=30m
kubectl logs job/trino-catalog-setup
```

Distribute `/output/shards.json` from the setup output PVC to the API only after Job success, then restart the API to refresh routing. Recreate the completed Job for subsequent runs; do not run setup concurrently.

## Scale and maintain

```sh
kubectl scale deployment/trino-worker --replicas=4
kubectl rollout status deployment/trino-worker --timeout=10m
```

Verify four active workers through `system.runtime.nodes`. New workers initialize their credential files before joining and receive existing catalog configurations from the coordinator. Scaling does not require rerunning the setup Job. Update the manifest replica count too if it is your source of truth; reapplying its default resets the count to two.

ConfigMap files mounted with `subPath` require pod restarts after edits. Vault credential rotation also requires restarting all nodes; keep old and new credentials valid during the transition. Worker removal or restart can fail running queries under normal execution; drain work or use a maintenance window before scale-down and upgrades. Coordinator restart interrupts queries. The catalog PVC survives Deployment replacement; retain and back it up. Shared-secret changes must be coordinated across all nodes.

## Migrate an existing single node

Pause incoming queries and finish active setup work. Back up the old catalog definitions and provision the new persistent view catalog against the existing metastore/storage. Copy existing catalog properties into `/data/trino/catalog` on the coordinator PVC with ownership readable and writable by UID/GID 1000, or intentionally recreate source catalogs with setup. In-memory views require reapplication and cannot be recovered just by copying catalog properties.

Start the cluster, verify all workers have registered, then run setup as needed (`--or-replace` for deliberate wrapper replacement). Validate source queries, wrapper authorization, and routing before pointing API clients at the coordinator HTTPS endpoint. Retire the old single-node deployment after this verification. Keep the coordinator replica count at one.
