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

The management plane (Resource Groups, Virtual Machines, Storage account
management, Service Bus, Monitor, Container Instances, AKS) has no GA typed Rust
SDK crate, so those ops reach Azure Resource Manager over REST — using the *same*
shared credential and the *same* `azure_core` HTTP client the typed data-plane
clients use under the hood. One auth path, one runtime, one HTTP stack.

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
| KMS | Key Vault Keys | `azure_security_keyvault_keys` |
| STS | Entra identity token | `azure_identity` |
| EC2 | Virtual Machines | ARM REST |
| ECS / EKS | Container Instances / AKS | ARM REST |
| SNS / SQS (enterprise) | Service Bus | ARM + data-plane REST |
| CloudWatch | Azure Monitor metrics | ARM REST |
| (account mgmt) | Storage account management | ARM REST |
| (resource mgmt) | Resource Groups | ARM REST |

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

# Key Vault Keys — RSA encrypt/decrypt (KMS analog) + list/get.
val $enc = Azure::Keys::encrypt("wrap-key", "secret data", vault => "my-kv")
val $kid = $enc->{kid}                                  # has the key version
val $clear = Azure::Keys::decrypt("wrap-key", $version, $enc->{ciphertext}, vault => "my-kv")
val @keys = Azure::Keys::ls(vault => "my-kv")
val $jwk = Azure::Keys::get("wrap-key", vault => "my-kv")->{jwk}
```

### Management plane (Azure Resource Manager)

These take a subscription from the `subscription =>` opt or
`AZURE_SUBSCRIPTION_ID`; scoped ops add `resource_group => "<rg>"`.

```perl
use Azure::ResourceGroups
use Azure::Compute
use Azure::Storage
use Azure::ServiceBus
use Azure::Monitor
use Azure::Containers
use Azure::Subscriptions

# Subscriptions + regions — tenant-scoped discovery (no subscription opt for ls).
val @subs = Azure::Subscriptions::ls()
val @regions = Azure::Subscriptions::locations(subscription => $sub)

# Resource Groups + provider-agnostic inventory.
val @rgs = Azure::ResourceGroups::ls(subscription => $sub)
Azure::ResourceGroups::create("app-rg", "eastus", subscription => $sub)
val @stores = Azure::ResourceGroups::resources(subscription => $sub,
    filter => "resourceType eq 'Microsoft.Storage/storageAccounts'")

# Virtual Machines — list, get, power actions (long-running), live state + SKUs.
val @vms = Azure::Compute::ls(subscription => $sub, resource_group => "app-rg")
Azure::Compute::start("web1", subscription => $sub, resource_group => "app-rg")
val $st = Azure::Compute::status("web1", subscription => $sub, resource_group => "app-rg")
p "power: $st->{power_state}"                          # running / stopped / deallocated
val @sizes = Azure::Compute::skus(subscription => $sub, location => "eastus")

# Storage account management (distinct from data-plane Blob/Queue).
val @accts = Azure::Storage::ls(subscription => $sub)
val $keys = Azure::Storage::keys("mystorage", subscription => $sub, resource_group => "app-rg")

# Service Bus — queues/topics/namespaces listing (mgmt) + send/receive (data plane).
val @queues = Azure::ServiceBus::queues("myns", subscription => $sub, resource_group => "app-rg")
val @topics = Azure::ServiceBus::topics("myns", subscription => $sub, resource_group => "app-rg")
val @namespaces = Azure::ServiceBus::namespaces(subscription => $sub)
Azure::ServiceBus::send("myns", "orders", "payload")
val $msg = Azure::ServiceBus::receive("myns", "orders")

# Monitor — metrics for any resource by its ARM id.
val $m = Azure::Monitor::metrics($vm_resource_id, metrics => "Percentage CPU")

# Container Instances + AKS (incl. live node pools).
val @groups = Azure::Containers::groups(subscription => $sub)
val @clusters = Azure::Containers::clusters(subscription => $sub)
val @pools = Azure::Containers::node_pools("mycluster", subscription => $sub, resource_group => "app-rg")
```

### Connection options

Each call takes a trailing `%opts` hash carrying the target account/vault:

| Service | Options |
| --- | --- |
| Blob / Queue | `account => "<storageacct>"` (or `AZURE_STORAGE_ACCOUNT`), or `endpoint => "https://..."` |
| Cosmos | `account => "<cosmosacct>"` (or `AZURE_COSMOS_ACCOUNT`), or `endpoint`, plus `region => "East US"` |
| Secrets / Keys | `vault => "<kvname>"` (or `AZURE_KEYVAULT_NAME`), or `vault_url => "https://<kv>.vault.azure.net/"` |
| Management plane (ARM) | `subscription => "<id>"` (or `AZURE_SUBSCRIPTION_ID`), scoped ops add `resource_group => "<rg>"`; sovereign clouds via `arm_endpoint => "..."` |
| Service Bus (data) | `namespace => "<ns>"`, or `sb_endpoint => "https://<ns>.servicebus.windows.net"` |

Authentication uses `DeveloperToolsCredential`: sign in with `az login` (or `azd
auth login`) before running.

### Pure helpers (no Azure)

These open no client — credential-free string parsing/validation:

```stryke
Azure::parse_resource_id($id)            # /subscriptions/.../providers/... → { subscription, resource_group, provider, types, resource_type, name }
Azure::resource_id_parent($id)           # RBAC-scope "dirname" → { id, parent, has_parent }; resource→resource-group→subscription→root (providers/{ns} handled); subscription parent is ""
Azure::build_resource_id(%opts)          # { subscription, resource_group, provider, types } → resource ID (canonical ARM casing); inverse of parse_resource_id
Azure::parse_connection_string($cs)      # Key=Value;... → { pairs => { Key => Value } } (base64 AccountKey survives)
Azure::build_connection_string(%pairs)   # { Key => Value } → Key=Value;... (byte-identical round-trip; inverse of parse_connection_string)
Azure::redact_connection_string($cs, %opts) → { redacted, masked_count }   # mask AccountKey/SharedAccessSignature (opts: mask, default ***) for safe logging
Azure::parse_blob_uri($uri)              # https://<acct>.<service>.core.windows.net/<container>/<blob> → { account, service, host, container, blob }
Azure::build_blob_uri(%opts)             # account/service/container/blob → blob endpoint URL; inverse of parse_blob_uri
Azure::storage_endpoint(%opts)           # account/service/cloud → { endpoint, url, suffix, … }; sovereign clouds (public/china/usgov)
Azure::parse_storage_endpoint($endpoint) # inverse: host/URL → { endpoint, account, service, cloud, suffix, url }
Azure::valid_storage_account_name($name) # { name, valid, reason } — 3-24 lowercase alphanumerics
Azure::valid_container_name($name)       # { name, valid, reason } — Blob container rules
Azure::valid_blob_name($name)            # { name, valid, reason, characters, segments } — Blob name: 1-1024 chars, ≤254 path segments, no trailing dot/slash
Azure::valid_keyvault_secret_name($name) # { name, valid, reason } — Key Vault secret name: 1-127 chars, alphanumeric + hyphens only
Azure::valid_queue_name($name)           # { name, valid, reason } — Queue rules (same DNS-label grammar as a container)
Azure::valid_table_name($name)           # { name, valid, reason } — Table name ^[A-Za-z][A-Za-z0-9]{2,62}$, "tables" reserved
Azure::valid_cosmos_id($id)              # { id, valid, reason } — Cosmos DB database/container id: ≤255 chars, no / or \
Azure::valid_guid($guid)                 # { guid, valid, reason } — 8-4-4-4-12 hex (subscription/tenant/client IDs)
Azure::normalize_guid($guid)             # canonical lowercase 8-4-4-4-12; accepts braces/parens/hyphenless
Azure::format_guid($guid, $format?)      # re-emit in a .NET specifier: N (no hyphens) / D (default) / B {…} / P (…)
```

## Packages

| Package | Surface |
| --- | --- |
| `Azure` | Flat API over every export (`Azure::blob_*`, `Azure::queue_*`, `Azure::cosmos_*`, `Azure::secrets_*`, `Azure::keys_*`, `Azure::vm_*`, `Azure::servicebus_*`, `Azure::identity_token`, plus the pure helpers). |
| `Azure::Blob` | `az://container/blob` URI helpers — `ls`, `get`, `put`, `head`, `rm`, `containers`, `create_container`, `delete_container`, `set_metadata`. |
| `Azure::Queue` | `ls`, `send`, `receive`, `delete`, `clear`, `count`, `create`, `drop`, and a `pump` receive→callback→delete loop. |
| `Azure::Cosmos` | Document helpers — `databases`, `containers`, `put`, `get`, `delete`, `query`, `replace`, `create_database`, `create_container`, `delete_database`, `delete_container`. |
| `Azure::Secrets` | Key Vault — `get`, `set`, `ls`, `rm`, `versions`, `backup`, plus `param_*` aliases for parameter-store-style callers. |
| `Azure::Keys` | Key Vault keys (KMS analog) — `encrypt`, `decrypt`, `ls`, `get`. |
| `Azure::Compute` | Virtual Machines — `ls`, `get`, `start`, `stop`, `deallocate`, `restart`, `status` (live power state), `skus` (VM sizes). |
| `Azure::Containers` | Container Instances + AKS — `groups`, `group`, `clusters`, `cluster`, `node_pools`. |
| `Azure::Storage` | Storage-account management — `ls`, `get`, `keys`. |
| `Azure::ResourceGroups` | Resource-group management — `ls`, `get`, `create`, `rm`, `resources` (provider-agnostic inventory, ARM `$filter`). |
| `Azure::ServiceBus` | Service Bus messaging — `queues`, `topics`, `namespaces`, `send`, `receive`. |
| `Azure::Subscriptions` | Tenant/subscription discovery — `ls` (all subscriptions), `locations` (regions). |
| `Azure::Monitor` | Azure Monitor metrics (CloudWatch analog) — `metrics`. |

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
