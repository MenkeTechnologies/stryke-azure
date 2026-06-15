# stryke-azure

[![CI](https://github.com/MenkeTechnologies/stryke-azure/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-azure/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Azure client for [stryke](https://github.com/MenkeTechnologies/strykelang) — a
`cdylib` dlopened in-process by the stryke runtime and exposed as the `Azure`
package. Built on the official [Azure SDK for Rust](https://github.com/Azure/azure-sdk-for-rust)
(GA `1.0.0`; Cosmos DB on the `0.35.0` preview).

It holds one shared `tokio` runtime and one Entra credential
(`DeveloperToolsCredential` — the Azure CLI / `azd` chain), reused across every
service client. There is no per-call fork, no per-call credential rebuild.

Docs: <https://menketechnologies.github.io/stryke-azure/> ·
[Engineering report](https://menketechnologies.github.io/stryke-azure/report.html)

## Service map

`stryke-azure` mirrors the surface of [`stryke-aws`](https://github.com/MenkeTechnologies/stryke-aws),
mapped onto Azure's GA Rust SDK:

| AWS service | Azure service | Backing crate |
| --- | --- | --- |
| S3 | Blob Storage | `azure_storage_blob` |
| SQS | Storage Queues | `azure_storage_queue` |
| DynamoDB | Cosmos DB (NoSQL) | `azure_data_cosmos` (preview) |
| Secrets Manager / SSM Parameter Store | Key Vault Secrets | `azure_security_keyvault_secrets` |
| STS | Entra identity token | `azure_identity` |

Lambda (Functions) and SNS (Event Grid) have no Azure Rust SDK and are
intentionally absent rather than shimmed over raw REST.

## Install

```sh
make install        # cargo build --release && s pkg install -g .
```

The release build produces `target/release/libstryke_azure.{dylib,so}`; `s pkg
install -g .` places it in `~/.stryke/store/azure@<ver>/`.

## Usage

```perl
use Azure
use Azure::Blob
use Azure::Cosmos
use Azure::Queue
use Azure::Secrets
use Azure::Keys

# Connectivity probe (Entra token; the token value is never returned).
val $tok = Azure::identity_token()
p "expires: $tok->{expires_on}"

# Blob Storage — az://container/blob URIs.
Azure::Blob::create_container("data", account => "mystorage")
val @containers = Azure::Blob::containers(account => "mystorage")
Azure::Blob::put("az://data/hello.txt", data => "hi", account => "mystorage")
val $body = Azure::Blob::get("az://data/hello.txt", account => "mystorage")
Azure::Blob::delete_container("data", account => "mystorage")

# Storage Queues.
Azure::Queue::create("jobs", account => "mystorage")
Azure::Queue::send("jobs", "payload", account => "mystorage")
val @msgs = Azure::Queue::receive("jobs", max => 10, account => "mystorage")
Azure::Queue::drop("jobs", account => "mystorage")   # delete the whole queue

# Cosmos DB (single-partition).
Azure::Cosmos::create_database("appdb", account => "mycosmos")
Azure::Cosmos::create_container("appdb", "users", "/tenant", account => "mycosmos")
Azure::Cosmos::put("appdb", "users", "acme",
    { id => "u1", name => "ada", tier => "gold" })
val $u = Azure::Cosmos::get("appdb", "users", "acme", "u1")

# Key Vault Secrets.
val $pw = Azure::Secrets::get("db-password", vault => "my-kv")
val @vers = Azure::Secrets::versions("db-password", vault => "my-kv")

# Key Vault Keys — RSA encrypt/decrypt (KMS analog).
val $enc = Azure::Keys::encrypt("wrap-key", "secret data", vault => "my-kv")
val $kid = $enc->{kid}                                  # has the key version
val $clear = Azure::Keys::decrypt("wrap-key", $version, $enc->{ciphertext}, vault => "my-kv")
```

### Connection options

Each call takes a trailing `%opts` hash carrying the target account/vault:

| Service | Options |
| --- | --- |
| Blob / Queue | `account => "<storageacct>"` (or `AZURE_STORAGE_ACCOUNT`), or `endpoint => "https://..."` |
| Cosmos | `account => "<cosmosacct>"` (or `AZURE_COSMOS_ACCOUNT`), or `endpoint`, plus `region => "East US"` |
| Secrets | `vault => "<kvname>"` (or `AZURE_KEYVAULT_NAME`), or `vault_url => "https://<kv>.vault.azure.net/"` |

Authentication uses `DeveloperToolsCredential`: sign in with `az login` (or `azd
auth login`) before running.

### Pure helpers (no Azure)

These open no client — credential-free string parsing/validation:

```stryke
Azure::parse_resource_id($id)            # /subscriptions/.../providers/... → { subscription, resource_group, provider, types, resource_type, name }
Azure::build_resource_id(%opts)          # { subscription, resource_group, provider, types } → resource ID (canonical ARM casing); inverse of parse_resource_id
Azure::parse_connection_string($cs)      # Key=Value;... → { pairs => { Key => Value } } (base64 AccountKey survives)
Azure::build_connection_string(%pairs)   # { Key => Value } → Key=Value;... (byte-identical round-trip; inverse of parse_connection_string)
Azure::parse_blob_uri($uri)              # https://<acct>.<service>.core.windows.net/<container>/<blob> → { account, service, host, container, blob }
Azure::build_blob_uri(%opts)             # account/service/container/blob → blob endpoint URL; inverse of parse_blob_uri
Azure::valid_storage_account_name($name) # { name, valid, reason } — 3-24 lowercase alphanumerics
Azure::valid_container_name($name)       # { name, valid, reason } — Blob container rules
```

## Packages

| Package | Surface |
| --- | --- |
| `Azure` | Flat API over every export (`Azure::blob_*`, `Azure::queue_*`, `Azure::cosmos_*`, `Azure::secrets_*`, `Azure::identity_token`). |
| `Azure::Blob` | `az://container/blob` URI helpers — `ls`, `get`, `put`, `head`, `rm`, `containers`. |
| `Azure::Queue` | `ls`, `send`, `receive`, `delete`, `clear`, `count`, and a `pump` receive→callback→delete loop. |
| `Azure::Cosmos` | Document helpers — `databases`, `containers`, `put`, `get`, `delete`, `query`. |
| `Azure::Secrets` | Key Vault — `get`, `set`, `ls`, `rm`, plus `param_*` aliases for parameter-store-style callers. |

## Build

```sh
make            # release build (default)
make debug      # cargo build
make test       # cargo test, then `s test t/`
cargo test      # Rust unit tests (endpoint + FFI-safety pins; offline)
```

## Examples

- `examples/discover.stk` — credential probe + read-only tour (CI-safe).
- `examples/blob_browse.stk` — list a container prefix.

## License

MIT © MenkeTechnologies
