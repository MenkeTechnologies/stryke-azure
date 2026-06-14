//! stryke-azure — Azure cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn azure__*` is a JSON-string-in /
//! JSON-string-out wrapper around the official `azure-sdk-for-rust`
//! GA crates (1.0.0; Cosmos is 0.35.0 preview). stryke's FFI bridge
//! (`rust_ffi.rs::load_cdylib`) resolves these symbols at first
//! `use Azure`.
//!
//! Persistent state:
//!   * `RUNTIME` — one shared `tokio` runtime drives every async call.
//!   * `CRED` — one `DeveloperToolsCredential` (Entra/CLI chain),
//!     built once and shared as `Arc<dyn TokenCredential>` across every
//!     service client. The pre-cdylib helper rebuilt the credential
//!     chain per fork — paying the full CLI/Entra token lookup each
//!     call. Service clients are cheap to construct given the cached
//!     credential, so they are built per call.
//!
//! Service map (AWS → Azure): S3 → Blob Storage, SQS → Storage Queues,
//! DynamoDB → Cosmos DB (NoSQL), Secrets Manager / SSM Parameter Store
//! → Key Vault Secrets, STS → Entra identity token. Lambda (Functions)
//! and SNS (Event Grid) have no Azure Rust SDK and are intentionally
//! absent.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use azure_core::credentials::TokenCredential;
use azure_identity::DeveloperToolsCredential;
use futures::TryStreamExt;
use once_cell::sync::OnceCell;
use serde_json::{json, Value};
use tokio::runtime::{Builder, Runtime};
use url::Url;

// ── runtime + credential cache ───────────────────────────────────────────────

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn rt() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

static CRED: OnceCell<Arc<dyn TokenCredential>> = OnceCell::new();

/// One shared developer/Entra credential, reused across every client.
fn cred() -> Result<Arc<dyn TokenCredential>> {
    CRED.get_or_try_init(|| {
        let c = DeveloperToolsCredential::new(None)
            .map_err(|e| anyhow!("azure credential init: {e}"))?;
        Ok::<Arc<dyn TokenCredential>, anyhow::Error>(c as Arc<dyn TokenCredential>)
    })
    .map(Arc::clone)
}

// ── option / endpoint helpers ────────────────────────────────────────────────

fn opt_str<'a>(opts: &'a Value, key: &str) -> Option<&'a str> {
    opts.get(key).and_then(|v| v.as_str())
}

fn req_str<'a>(opts: &'a Value, key: &str) -> Result<&'a str> {
    opt_str(opts, key).ok_or_else(|| anyhow!("missing {key}"))
}

fn storage_account(opts: &Value) -> Result<String> {
    opt_str(opts, "account")
        .map(String::from)
        .or_else(|| std::env::var("AZURE_STORAGE_ACCOUNT").ok())
        .ok_or_else(|| anyhow!("missing account (or set AZURE_STORAGE_ACCOUNT)"))
}

/// `https://<account>.blob.core.windows.net/` — or an explicit `endpoint`.
fn blob_service_url(opts: &Value) -> Result<Url> {
    if let Some(e) = opt_str(opts, "endpoint") {
        return Ok(Url::parse(e)?);
    }
    let a = storage_account(opts)?;
    Ok(Url::parse(&format!("https://{a}.blob.core.windows.net/"))?)
}

/// `https://<account>.queue.core.windows.net/` — or an explicit `endpoint`.
fn queue_service_url(opts: &Value) -> Result<Url> {
    if let Some(e) = opt_str(opts, "endpoint") {
        return Ok(Url::parse(e)?);
    }
    let a = storage_account(opts)?;
    Ok(Url::parse(&format!("https://{a}.queue.core.windows.net/"))?)
}

/// `https://<account>.documents.azure.com:443/` — or an explicit `endpoint`.
fn cosmos_endpoint(opts: &Value) -> Result<String> {
    if let Some(e) = opt_str(opts, "endpoint") {
        return Ok(e.to_string());
    }
    let a = opt_str(opts, "account")
        .map(String::from)
        .or_else(|| std::env::var("AZURE_COSMOS_ACCOUNT").ok())
        .ok_or_else(|| anyhow!("missing account/endpoint (or set AZURE_COSMOS_ACCOUNT)"))?;
    Ok(format!("https://{a}.documents.azure.com:443/"))
}

/// `https://<vault>.vault.azure.net/` — or an explicit `vault_url`/`endpoint`.
fn vault_url(opts: &Value) -> Result<String> {
    if let Some(e) = opt_str(opts, "vault_url").or_else(|| opt_str(opts, "endpoint")) {
        return Ok(e.to_string());
    }
    let v = opt_str(opts, "vault")
        .map(String::from)
        .or_else(|| std::env::var("AZURE_KEYVAULT_NAME").ok())
        .ok_or_else(|| anyhow!("missing vault (or set AZURE_KEYVAULT_NAME)"))?;
    Ok(format!("https://{v}.vault.azure.net/"))
}

// ── Identity (STS analog) ────────────────────────────────────────────────────

/// Acquire an Entra access token for `scope` (default ARM). Proves the
/// credential chain resolves. The token itself is never returned —
/// only its expiry, mirroring a connectivity probe.
async fn op_identity_token(opts: Value) -> Result<Value> {
    let scope = opt_str(&opts, "scope").unwrap_or("https://management.azure.com/.default");
    let tok = cred()?
        .get_token(&[scope], None)
        .await
        .map_err(|e| anyhow!("get_token: {e}"))?;
    Ok(json!({
        "acquired": true,
        "scope": scope,
        "expires_on": tok.expires_on.to_string(),
    }))
}

// ── Blob Storage (S3 analog) ─────────────────────────────────────────────────

fn blob_service(opts: &Value) -> Result<azure_storage_blob::clients::BlobServiceClient> {
    use azure_storage_blob::clients::BlobServiceClient;
    Ok(BlobServiceClient::new(
        blob_service_url(opts)?,
        Some(cred()?),
        None,
    )?)
}

async fn op_blob_list_containers(opts: Value) -> Result<Value> {
    let client = blob_service(&opts)?;
    let mut pager = client.list_containers(None)?;
    let mut names = Vec::new();
    while let Some(item) = pager.try_next().await? {
        if let Some(n) = item.name {
            names.push(n);
        }
    }
    Ok(json!({ "containers": names }))
}

async fn op_blob_list_blobs(opts: Value) -> Result<Value> {
    let client = blob_service(&opts)?;
    let container = req_str(&opts, "container")?.to_string();
    // `list_blobs` exposes no server-side prefix filter in 1.0.0 — filter
    // the flattened items client-side to preserve the s3-style `prefix` arg.
    let prefix = opt_str(&opts, "prefix").unwrap_or("");
    let cc = client.blob_container_client(&container);
    let mut pager = cc.list_blobs(None)?;
    let mut blobs = Vec::new();
    while let Some(b) = pager.try_next().await? {
        let name = b.name.unwrap_or_default();
        if !prefix.is_empty() && !name.starts_with(prefix) {
            continue;
        }
        let size = b.properties.as_ref().and_then(|p| p.content_length);
        blobs.push(json!({ "name": name, "size": size }));
    }
    Ok(json!({ "container": container, "blobs": blobs }))
}

async fn op_blob_get(opts: Value) -> Result<Value> {
    let client = blob_service(&opts)?;
    let container = req_str(&opts, "container")?;
    let name = req_str(&opts, "name")?;
    let bc = client.blob_client(container, name);
    let r = bc.download(None).await?;
    let bytes = r.body.collect().await?;
    let body = match std::str::from_utf8(bytes.as_ref()) {
        Ok(s) => Value::String(s.to_string()),
        Err(_) => {
            use base64::Engine as _;
            Value::String(format!(
                "base64:{}",
                base64::engine::general_purpose::STANDARD.encode(bytes.as_ref())
            ))
        }
    };
    Ok(json!({
        "container": container,
        "name": name,
        "content_length": r.properties.content_length,
        "content_type": r.properties.content_type,
        "body": body,
    }))
}

async fn op_blob_put(opts: Value) -> Result<Value> {
    use azure_core::http::RequestContent;
    use azure_core::Bytes;
    let client = blob_service(&opts)?;
    let container = req_str(&opts, "container")?.to_string();
    let name = req_str(&opts, "name")?.to_string();
    let body = opt_str(&opts, "body").unwrap_or("").as_bytes().to_vec();
    let bc = client.blob_client(&container, &name);
    let content: RequestContent<Bytes, azure_core::http::NoFormat> = RequestContent::from(body);
    bc.upload(content, None).await?;
    Ok(json!({ "container": container, "name": name, "uploaded": true }))
}

async fn op_blob_delete(opts: Value) -> Result<Value> {
    let client = blob_service(&opts)?;
    let container = req_str(&opts, "container")?.to_string();
    let name = req_str(&opts, "name")?.to_string();
    let bc = client.blob_client(&container, &name);
    bc.delete(None).await?;
    Ok(json!({ "container": container, "name": name, "deleted": true }))
}

async fn op_blob_properties(opts: Value) -> Result<Value> {
    use azure_storage_blob::models::BlobClientGetPropertiesResultHeaders;
    let client = blob_service(&opts)?;
    let container = req_str(&opts, "container")?;
    let name = req_str(&opts, "name")?;
    let bc = client.blob_client(container, name);
    let r = bc.get_properties(None).await?;
    Ok(json!({
        "container": container,
        "name": name,
        "content_length": r.content_length()?,
        "content_type": r.content_type()?,
        "etag": r.etag()?.map(|e| e.to_string()),
        "last_modified": r.last_modified()?.map(|t| t.to_string()),
        "blob_type": r.blob_type()?.map(|b| format!("{b:?}")),
    }))
}

async fn op_blob_create_container(opts: Value) -> Result<Value> {
    let client = blob_service(&opts)?;
    let container = req_str(&opts, "container")?.to_string();
    client
        .blob_container_client(&container)
        .create(None)
        .await?;
    Ok(json!({ "container": container, "created": true }))
}

async fn op_blob_delete_container(opts: Value) -> Result<Value> {
    let client = blob_service(&opts)?;
    let container = req_str(&opts, "container")?.to_string();
    client
        .blob_container_client(&container)
        .delete(None)
        .await?;
    Ok(json!({ "container": container, "deleted": true }))
}

async fn op_blob_set_metadata(opts: Value) -> Result<Value> {
    use std::collections::HashMap;
    let client = blob_service(&opts)?;
    let container = req_str(&opts, "container")?.to_string();
    let name = req_str(&opts, "name")?.to_string();
    let meta = opts
        .get("metadata")
        .and_then(|m| m.as_object())
        .ok_or_else(|| anyhow!("missing metadata (an object of string => string)"))?;
    // Azure blob metadata is a flat string→string map; non-string values are
    // skipped rather than silently stringified.
    let map: HashMap<String, String> = meta
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
        .collect();
    client
        .blob_client(&container, &name)
        .set_metadata(&map, None)
        .await?;
    Ok(json!({ "container": container, "name": name, "metadata": map.len() }))
}

// ── Storage Queues (SQS analog) ──────────────────────────────────────────────

fn queue_service(opts: &Value) -> Result<azure_storage_queue::clients::QueueServiceClient> {
    use azure_storage_queue::clients::QueueServiceClient;
    Ok(QueueServiceClient::new(
        queue_service_url(opts)?,
        Some(cred()?),
        None,
    )?)
}

async fn op_queue_list(opts: Value) -> Result<Value> {
    let client = queue_service(&opts)?;
    let mut pager = client.list_queues(None)?;
    let mut names = Vec::new();
    while let Some(q) = pager.try_next().await? {
        if let Some(n) = q.name {
            names.push(n);
        }
    }
    Ok(json!({ "queues": names }))
}

async fn op_queue_send(opts: Value) -> Result<Value> {
    use azure_storage_queue::models::QueueMessage;
    let client = queue_service(&opts)?;
    let queue = req_str(&opts, "queue")?.to_string();
    let body = req_str(&opts, "body")?.to_string();
    let qc = client.queue_client(&queue)?;
    let msg = QueueMessage {
        message_text: Some(body),
    };
    let r = qc.send_message(msg.try_into()?, None).await?;
    let sent = r.into_model()?;
    Ok(json!({
        "queue": queue,
        "sent": serde_json::to_value(&sent).unwrap_or(Value::Null),
    }))
}

async fn op_queue_receive(opts: Value) -> Result<Value> {
    use azure_storage_queue::models::QueueClientReceiveMessagesOptions;
    let client = queue_service(&opts)?;
    let queue = req_str(&opts, "queue")?.to_string();
    let qc = client.queue_client(&queue)?;
    let o = QueueClientReceiveMessagesOptions {
        number_of_messages: opts.get("max").and_then(|v| v.as_i64()).map(|n| n as i32),
        visibility_timeout: opts
            .get("visibility")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32),
        ..Default::default()
    };
    let r = qc.receive_messages(Some(o)).await?;
    let msgs = r.into_model()?.items.unwrap_or_default();
    let out: Vec<Value> = msgs
        .iter()
        .map(|m| {
            json!({
                "message_id": m.message_id,
                "pop_receipt": m.pop_receipt,
                "body": m.message_text,
                "dequeue_count": m.dequeue_count,
            })
        })
        .collect();
    Ok(json!({ "queue": queue, "messages": out }))
}

async fn op_queue_delete_message(opts: Value) -> Result<Value> {
    let client = queue_service(&opts)?;
    let queue = req_str(&opts, "queue")?.to_string();
    let message_id = req_str(&opts, "message_id")?;
    let pop_receipt = req_str(&opts, "pop_receipt")?;
    let qc = client.queue_client(&queue)?;
    qc.delete_message(message_id, pop_receipt, None).await?;
    Ok(json!({ "queue": queue, "deleted": true }))
}

async fn op_queue_clear(opts: Value) -> Result<Value> {
    let client = queue_service(&opts)?;
    let queue = req_str(&opts, "queue")?.to_string();
    let qc = client.queue_client(&queue)?;
    qc.clear(None).await?;
    Ok(json!({ "queue": queue, "cleared": true }))
}

async fn op_queue_properties(opts: Value) -> Result<Value> {
    use azure_storage_queue::models::QueueClientGetPropertiesResultHeaders;
    let client = queue_service(&opts)?;
    let queue = req_str(&opts, "queue")?.to_string();
    let qc = client.queue_client(&queue)?;
    let r = qc.get_properties(None).await?;
    Ok(json!({
        "queue": queue,
        "approximate_message_count": r.approximate_messages_count()?,
    }))
}

async fn op_queue_create(opts: Value) -> Result<Value> {
    let client = queue_service(&opts)?;
    let queue = req_str(&opts, "queue")?.to_string();
    client.queue_client(&queue)?.create(None).await?;
    Ok(json!({ "queue": queue, "created": true }))
}

async fn op_queue_delete(opts: Value) -> Result<Value> {
    let client = queue_service(&opts)?;
    let queue = req_str(&opts, "queue")?.to_string();
    client.queue_client(&queue)?.delete(None).await?;
    Ok(json!({ "queue": queue, "deleted": true }))
}

// ── Cosmos DB NoSQL (DynamoDB analog) ────────────────────────────────────────

async fn cosmos_client(opts: &Value) -> Result<azure_data_cosmos::CosmosClient> {
    use azure_data_cosmos::{
        AccountEndpoint, AccountReference, CosmosClient, Region, RoutingStrategy,
    };
    let endpoint: AccountEndpoint = cosmos_endpoint(opts)?
        .parse()
        .map_err(|e| anyhow!("cosmos endpoint parse: {e:?}"))?;
    let account = AccountReference::with_credential(endpoint, cred()?);
    let region = opt_str(opts, "region")
        .map(|s| Region::from(s.to_string()))
        .unwrap_or(Region::EAST_US);
    let client = CosmosClient::builder()
        .build(account, RoutingStrategy::ProximityTo(region))
        .await?;
    Ok(client)
}

async fn op_cosmos_list_databases(opts: Value) -> Result<Value> {
    let client = cosmos_client(&opts).await?;
    let mut it = client.query_databases("SELECT * FROM root", None).await?;
    let mut dbs = Vec::new();
    while let Some(db) = it.try_next().await? {
        dbs.push(db.id);
    }
    Ok(json!({ "databases": dbs }))
}

async fn op_cosmos_list_containers(opts: Value) -> Result<Value> {
    let client = cosmos_client(&opts).await?;
    let database = req_str(&opts, "database")?.to_string();
    let db = client.database_client(&database);
    let mut it = db.query_containers("SELECT * FROM root", None).await?;
    let mut containers = Vec::new();
    while let Some(c) = it.try_next().await? {
        containers.push(c.id.to_string());
    }
    Ok(json!({ "database": database, "containers": containers }))
}

async fn op_cosmos_create_database(opts: Value) -> Result<Value> {
    let client = cosmos_client(&opts).await?;
    let id = req_str(&opts, "database")?.to_string();
    client.create_database(&id, None).await?;
    Ok(json!({ "database": id, "created": true }))
}

async fn op_cosmos_create_container(opts: Value) -> Result<Value> {
    use azure_data_cosmos::models::{ContainerProperties, PartitionKeyDefinition};
    let client = cosmos_client(&opts).await?;
    let database = req_str(&opts, "database")?.to_string();
    let id = req_str(&opts, "container")?.to_string();
    // Partition key path, e.g. "/tenantId". Required by Cosmos for new containers.
    let pk = req_str(&opts, "partition_key")?;
    let props = ContainerProperties::new(id.clone(), PartitionKeyDefinition::from(pk));
    client
        .database_client(&database)
        .create_container(props, None)
        .await?;
    Ok(json!({ "database": database, "container": id, "created": true }))
}

async fn op_cosmos_delete_database(opts: Value) -> Result<Value> {
    let client = cosmos_client(&opts).await?;
    let database = req_str(&opts, "database")?.to_string();
    client.database_client(&database).delete(None).await?;
    Ok(json!({ "database": database, "deleted": true }))
}

/// Resolve the `ContainerClient` for `database`/`container` opts.
async fn cosmos_container(opts: &Value) -> Result<azure_data_cosmos::clients::ContainerClient> {
    let client = cosmos_client(opts).await?;
    let database = req_str(opts, "database")?;
    let container = req_str(opts, "container")?;
    Ok(client
        .database_client(database)
        .container_client(container)
        .await?)
}

async fn op_cosmos_upsert_item(opts: Value) -> Result<Value> {
    use azure_data_cosmos::PartitionKey;
    let cc = cosmos_container(&opts).await?;
    let pk = req_str(&opts, "partition_key")?.to_string();
    let id = req_str(&opts, "id")?.to_string();
    let item = opts
        .get("item")
        .cloned()
        .ok_or_else(|| anyhow!("missing item (object)"))?;
    let r = cc
        .upsert_item(PartitionKey::from(pk), &id, item, None)
        .await?;
    let body: Value = r.into_model()?;
    Ok(json!({ "id": id, "item": body }))
}

async fn op_cosmos_read_item(opts: Value) -> Result<Value> {
    use azure_data_cosmos::PartitionKey;
    let cc = cosmos_container(&opts).await?;
    let pk = req_str(&opts, "partition_key")?.to_string();
    let id = req_str(&opts, "id")?.to_string();
    let r = cc.read_item(PartitionKey::from(pk), &id, None).await?;
    let body: Value = r.into_model()?;
    Ok(json!({ "id": id, "item": body }))
}

async fn op_cosmos_delete_item(opts: Value) -> Result<Value> {
    use azure_data_cosmos::PartitionKey;
    let cc = cosmos_container(&opts).await?;
    let pk = req_str(&opts, "partition_key")?.to_string();
    let id = req_str(&opts, "id")?.to_string();
    cc.delete_item(PartitionKey::from(pk), &id, None).await?;
    Ok(json!({ "id": id, "deleted": true }))
}

async fn op_cosmos_replace_item(opts: Value) -> Result<Value> {
    use azure_data_cosmos::PartitionKey;
    let cc = cosmos_container(&opts).await?;
    let pk = req_str(&opts, "partition_key")?.to_string();
    let id = req_str(&opts, "id")?.to_string();
    let item = opts
        .get("item")
        .cloned()
        .ok_or_else(|| anyhow!("missing item (object)"))?;
    // Unlike upsert, replace fails if the item doesn't already exist.
    let r = cc
        .replace_item(PartitionKey::from(pk), &id, item, None)
        .await?;
    let body: Value = r.into_model()?;
    Ok(json!({ "id": id, "item": body }))
}

async fn op_cosmos_delete_container(opts: Value) -> Result<Value> {
    let cc = cosmos_container(&opts).await?;
    let database = req_str(&opts, "database")?.to_string();
    let container = req_str(&opts, "container")?.to_string();
    cc.delete(None).await?;
    Ok(json!({ "database": database, "container": container, "deleted": true }))
}

async fn op_cosmos_query(opts: Value) -> Result<Value> {
    use azure_data_cosmos::query::FeedScope;
    use azure_data_cosmos::PartitionKey;
    let cc = cosmos_container(&opts).await?;
    let query = req_str(&opts, "query")?.to_string();
    // `query_items` supports single-partition queries — a partition key is
    // required to scope the feed.
    let pk = req_str(&opts, "partition_key")
        .map_err(|_| anyhow!("query requires partition_key (single-partition only)"))?
        .to_string();
    let mut it = cc
        .query_items::<Value>(query, FeedScope::partition(PartitionKey::from(pk)), None)
        .await?;
    let mut items = Vec::new();
    while let Some(v) = it.try_next().await? {
        items.push(v);
    }
    let count = items.len();
    Ok(json!({ "items": items, "count": count }))
}

// ── Key Vault Secrets (Secrets Manager / SSM Parameter Store analog) ─────────

fn secret_client(opts: &Value) -> Result<azure_security_keyvault_secrets::SecretClient> {
    use azure_security_keyvault_secrets::SecretClient;
    Ok(SecretClient::new(&vault_url(opts)?, cred()?, None)?)
}

async fn op_secrets_get(opts: Value) -> Result<Value> {
    let client = secret_client(&opts)?;
    let name = req_str(&opts, "name")?;
    let secret = client.get_secret(name, None).await?.into_model()?;
    Ok(json!({
        "name": name,
        "value": secret.value,
        "id": secret.id,
    }))
}

async fn op_secrets_set(opts: Value) -> Result<Value> {
    use azure_security_keyvault_secrets::models::SetSecretParameters;
    let client = secret_client(&opts)?;
    let name = req_str(&opts, "name")?.to_string();
    let value = req_str(&opts, "value")?.to_string();
    let params = SetSecretParameters {
        value: Some(value),
        content_type: opt_str(&opts, "content_type").map(String::from),
        secret_attributes: None,
        tags: None,
    };
    let secret = client
        .set_secret(&name, params.try_into()?, None)
        .await?
        .into_model()?;
    Ok(json!({ "name": name, "id": secret.id }))
}

async fn op_secrets_list(opts: Value) -> Result<Value> {
    let client = secret_client(&opts)?;
    let mut pager = client.list_secret_properties(None)?;
    let mut out = Vec::new();
    while let Some(p) = pager.try_next().await? {
        out.push(json!({ "id": p.id }));
    }
    Ok(json!({ "secrets": out }))
}

async fn op_secrets_delete(opts: Value) -> Result<Value> {
    let client = secret_client(&opts)?;
    let name = req_str(&opts, "name")?.to_string();
    client.delete_secret(&name, None).await?;
    Ok(json!({ "name": name, "deleted": true }))
}

async fn op_secrets_list_versions(opts: Value) -> Result<Value> {
    let client = secret_client(&opts)?;
    let name = req_str(&opts, "name")?.to_string();
    let mut pager = client.list_secret_properties_versions(&name, None)?;
    let mut out = Vec::new();
    while let Some(p) = pager.try_next().await? {
        out.push(json!({ "id": p.id }));
    }
    Ok(json!({ "name": name, "versions": out }))
}

async fn op_secrets_backup(opts: Value) -> Result<Value> {
    use base64::Engine as _;
    let client = secret_client(&opts)?;
    let name = req_str(&opts, "name")?.to_string();
    let r = client.backup_secret(&name, None).await?.into_model()?;
    // The backup blob is opaque; hand it back base64-encoded for restore later.
    let backup = r
        .value
        .map(|b| base64::engine::general_purpose::STANDARD.encode(b));
    Ok(json!({ "name": name, "backup": backup }))
}

// ── Key Vault Keys (KMS analog) ──────────────────────────────────────────────

fn key_client(opts: &Value) -> Result<azure_security_keyvault_keys::KeyClient> {
    use azure_security_keyvault_keys::KeyClient;
    Ok(KeyClient::new(&vault_url(opts)?, cred()?, None)?)
}

/// Map a caller algorithm name to the SDK enum (RSA asymmetric algorithms —
/// the common Key Vault encrypt/decrypt case). Defaults to RSA-OAEP-256.
fn enc_algo(s: &str) -> Result<azure_security_keyvault_keys::models::EncryptionAlgorithm> {
    use azure_security_keyvault_keys::models::EncryptionAlgorithm as E;
    Ok(match s.to_ascii_uppercase().replace('_', "-").as_str() {
        "RSA-OAEP" => E::RsaOaep,
        "RSA-OAEP-256" => E::RsaOaep256,
        "RSA1-5" | "RSA15" => E::Rsa1_5,
        other => {
            return Err(anyhow!(
                "unsupported algorithm `{other}` (use RSA-OAEP, RSA-OAEP-256, or RSA1_5)"
            ))
        }
    })
}

async fn op_keys_encrypt(opts: Value) -> Result<Value> {
    use azure_security_keyvault_keys::models::KeyOperationParameters;
    use base64::Engine as _;
    let client = key_client(&opts)?;
    let key = req_str(&opts, "key")?.to_string();
    let algorithm = opt_str(&opts, "algorithm").unwrap_or("RSA-OAEP-256");
    let plaintext = req_str(&opts, "plaintext")?.as_bytes().to_vec();
    let params = KeyOperationParameters {
        algorithm: Some(enc_algo(algorithm)?),
        value: Some(plaintext),
        ..Default::default()
    };
    let r = client
        .encrypt(&key, params.try_into()?, None)
        .await?
        .into_model()?;
    let ciphertext = r
        .result
        .map(|b| base64::engine::general_purpose::STANDARD.encode(b));
    // `kid` carries the key version (trailing path segment) needed for decrypt.
    Ok(json!({ "key": key, "algorithm": algorithm, "ciphertext": ciphertext, "kid": r.kid }))
}

async fn op_keys_decrypt(opts: Value) -> Result<Value> {
    use azure_security_keyvault_keys::models::KeyOperationParameters;
    use base64::Engine as _;
    let client = key_client(&opts)?;
    let key = req_str(&opts, "key")?.to_string();
    // decrypt (unlike encrypt) targets a specific key version — required by the
    // REST path. The version is the trailing segment of the key id `kid`.
    let version = req_str(&opts, "version")?.to_string();
    let algorithm = opt_str(&opts, "algorithm").unwrap_or("RSA-OAEP-256");
    let ciphertext = base64::engine::general_purpose::STANDARD
        .decode(req_str(&opts, "ciphertext")?)
        .map_err(|e| anyhow!("ciphertext base64: {e}"))?;
    let params = KeyOperationParameters {
        algorithm: Some(enc_algo(algorithm)?),
        value: Some(ciphertext),
        ..Default::default()
    };
    let r = client
        .decrypt(&key, &version, params.try_into()?, None)
        .await?
        .into_model()?;
    let plaintext = r.result.map(|b| match String::from_utf8(b.clone()) {
        Ok(s) => Value::String(s),
        Err(_) => Value::String(format!(
            "base64:{}",
            base64::engine::general_purpose::STANDARD.encode(&b)
        )),
    });
    Ok(json!({ "key": key, "plaintext": plaintext }))
}

// ── FFI plumbing ─────────────────────────────────────────────────────────────

fn ffi_call_async<F, Fut>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Fut,
    Fut: std::future::Future<Output = Result<Value>>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let fut = handler(input);
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| rt().block_on(fut)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-azure handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── exports ──────────────────────────────────────────────────────────────────

macro_rules! export {
    ($name:ident, $handler:path) => {
        #[no_mangle]
        pub extern "C" fn $name(args: *const c_char) -> *const c_char {
            ffi_call_async(args, $handler)
        }
    };
}

#[no_mangle]
pub extern "C" fn azure__pkg_version(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |_| async {
        Ok(json!({ "version": env!("CARGO_PKG_VERSION") }))
    })
}

export!(azure__identity_token, op_identity_token);

export!(azure__blob_list_containers, op_blob_list_containers);
export!(azure__blob_list_blobs, op_blob_list_blobs);
export!(azure__blob_get, op_blob_get);
export!(azure__blob_put, op_blob_put);
export!(azure__blob_delete, op_blob_delete);
export!(azure__blob_properties, op_blob_properties);
export!(azure__blob_create_container, op_blob_create_container);
export!(azure__blob_delete_container, op_blob_delete_container);
export!(azure__blob_set_metadata, op_blob_set_metadata);

export!(azure__queue_list, op_queue_list);
export!(azure__queue_send, op_queue_send);
export!(azure__queue_receive, op_queue_receive);
export!(azure__queue_delete_message, op_queue_delete_message);
export!(azure__queue_clear, op_queue_clear);
export!(azure__queue_properties, op_queue_properties);
export!(azure__queue_create, op_queue_create);
export!(azure__queue_delete, op_queue_delete);

export!(azure__cosmos_list_databases, op_cosmos_list_databases);
export!(azure__cosmos_list_containers, op_cosmos_list_containers);
export!(azure__cosmos_create_database, op_cosmos_create_database);
export!(azure__cosmos_create_container, op_cosmos_create_container);
export!(azure__cosmos_delete_database, op_cosmos_delete_database);
export!(azure__cosmos_delete_container, op_cosmos_delete_container);
export!(azure__cosmos_replace_item, op_cosmos_replace_item);
export!(azure__cosmos_upsert_item, op_cosmos_upsert_item);
export!(azure__cosmos_read_item, op_cosmos_read_item);
export!(azure__cosmos_delete_item, op_cosmos_delete_item);
export!(azure__cosmos_query, op_cosmos_query);

export!(azure__secrets_get, op_secrets_get);
export!(azure__secrets_set, op_secrets_set);
export!(azure__secrets_list, op_secrets_list);
export!(azure__secrets_delete, op_secrets_delete);
export!(azure__secrets_list_versions, op_secrets_list_versions);
export!(azure__secrets_backup, op_secrets_backup);

export!(azure__keys_encrypt, op_keys_encrypt);
export!(azure__keys_decrypt, op_keys_decrypt);

// ── tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod endpoint_tests {
    use super::*;

    #[test]
    fn blob_url_from_account() {
        let u = blob_service_url(&json!({ "account": "acme" })).unwrap();
        assert_eq!(u.as_str(), "https://acme.blob.core.windows.net/");
    }

    #[test]
    fn blob_url_explicit_endpoint_wins() {
        let u = blob_service_url(&json!({
            "account": "ignored",
            "endpoint": "https://custom.blob.core.windows.net/"
        }))
        .unwrap();
        assert_eq!(u.as_str(), "https://custom.blob.core.windows.net/");
    }

    #[test]
    fn queue_url_from_account() {
        let u = queue_service_url(&json!({ "account": "acme" })).unwrap();
        assert_eq!(u.as_str(), "https://acme.queue.core.windows.net/");
    }

    #[test]
    fn cosmos_endpoint_from_account() {
        let e = cosmos_endpoint(&json!({ "account": "acme" })).unwrap();
        assert_eq!(e, "https://acme.documents.azure.com:443/");
    }

    #[test]
    fn vault_url_from_name_and_explicit() {
        assert_eq!(
            vault_url(&json!({ "vault": "kv1" })).unwrap(),
            "https://kv1.vault.azure.net/"
        );
        assert_eq!(
            vault_url(&json!({ "vault_url": "https://kv2.vault.azure.net/" })).unwrap(),
            "https://kv2.vault.azure.net/"
        );
    }

    #[test]
    fn missing_account_errors() {
        // No `account` arg and (in CI) no env var → a clear error, not a panic.
        std::env::remove_var("AZURE_STORAGE_ACCOUNT");
        assert!(blob_service_url(&json!({})).is_err());
    }
}

#[cfg(test)]
mod ffi_tests {
    //! FFI safety pins. The cdylib is dlopened in-process by stryke; a
    //! panic across the C ABI or a null deref here corrupts the host
    //! shell. These defend the C-ABI contract against regressions that
    //! would only surface at runtime on a caller's machine.
    use super::*;
    use std::ffi::CStr;
    use std::ptr;

    unsafe fn drain(p: *const c_char) -> String {
        assert!(!p.is_null(), "export returned null pointer");
        let s = CStr::from_ptr(p).to_string_lossy().into_owned();
        stryke_free_cstring(p as *mut c_char);
        s
    }

    #[test]
    fn null_args_returns_version_not_segfault() {
        let raw = azure__pkg_version(ptr::null());
        let s = unsafe { drain(raw) };
        let v: Value = serde_json::from_str(&s).expect("valid JSON");
        assert_eq!(v["version"], json!(env!("CARGO_PKG_VERSION")));
        assert!(v.get("error").is_none());
    }

    #[test]
    fn invalid_json_args_coerced_to_null_not_crash() {
        let garbage = std::ffi::CString::new("this is not json {[ ").unwrap();
        let raw = azure__pkg_version(garbage.as_ptr());
        let s = unsafe { drain(raw) };
        let v: Value = serde_json::from_str(&s).expect("valid JSON");
        assert_eq!(v["version"], json!(env!("CARGO_PKG_VERSION")));
        assert!(v.get("error").is_none());
    }

    #[test]
    fn handler_panic_is_caught_and_returned_as_error_json() {
        let raw = ffi_call_async(ptr::null(), |_v| async {
            panic!("intentional test panic — must not cross FFI");
        });
        let s = unsafe { drain(raw) };
        let v: Value = serde_json::from_str(&s).expect("valid JSON");
        assert_eq!(v["error"], json!("stryke-azure handler panicked"));
    }

    #[test]
    fn free_cstring_tolerates_null() {
        unsafe {
            stryke_free_cstring(ptr::null_mut());
            stryke_free_cstring(ptr::null_mut());
        }
    }

    #[test]
    fn handler_err_maps_to_error_key_verbatim() {
        let raw = ffi_call_async(ptr::null(), |_v| async {
            Err(anyhow!("missing container"))
        });
        let s = unsafe { drain(raw) };
        let v: Value = serde_json::from_str(&s).expect("valid JSON");
        assert_eq!(v["error"], json!("missing container"));
        assert!(v["error"].is_string());
    }

    #[test]
    fn args_are_parsed_and_threaded_to_handler_intact() {
        let doc = std::ffi::CString::new(r#"{"region":"eu-wést-1","nested":{"n":42}}"#).unwrap();
        let raw = ffi_call_async(doc.as_ptr(), |v| async move { Ok(v) });
        let s = unsafe { drain(raw) };
        let v: Value = serde_json::from_str(&s).expect("valid JSON");
        assert_eq!(v["region"], json!("eu-wést-1"));
        assert_eq!(v["nested"]["n"], json!(42));
        assert!(v.get("error").is_none());
    }
}
