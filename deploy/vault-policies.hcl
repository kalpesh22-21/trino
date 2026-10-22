# Setup-job policy: bind ONLY to its service account and namespace.
path "secret/data/trino/test/admin" {
  capabilities = ["read"]
}
# Put the following in a SEPARATE policy for the existing Trino node role.
# The setup Job does not need the SQL secret and Trino nodes do not need admin.
# path "secret/data/trino/test/mysql" {
#   capabilities = ["read"]
# }
