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

// ── pure helpers (no Azure) ──────────────────────────────────────────────────

/// Parse an Azure resource ID `/subscriptions/{sub}/resourceGroups/{rg}/
/// providers/{provider}/{type}/{name}[/{type}/{name}…]` into its parts.
/// `resource_type`/`name` are the last type/name pair. Pure.
fn op_parse_resource_id(opts: Value) -> Result<Value> {
    let id = opts
        .get("id")
        .or_else(|| opts.get("resource_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing id"))?;
    let segs: Vec<&str> = id.split('/').filter(|s| !s.is_empty()).collect();
    if segs.is_empty() {
        return Err(anyhow!("empty resource id"));
    }
    let mut subscription = Value::Null;
    let mut resource_group = Value::Null;
    let mut provider = Value::Null;
    let mut types: Vec<Value> = Vec::new();
    let mut i = 0;
    let mut in_resource = false;
    while i < segs.len() {
        if !in_resource {
            match segs[i].to_ascii_lowercase().as_str() {
                "subscriptions" if i + 1 < segs.len() => {
                    subscription = json!(segs[i + 1]);
                    i += 2;
                }
                "resourcegroups" if i + 1 < segs.len() => {
                    resource_group = json!(segs[i + 1]);
                    i += 2;
                }
                "providers" if i + 1 < segs.len() => {
                    provider = json!(segs[i + 1]);
                    i += 2;
                    in_resource = true;
                }
                _ => i += 1,
            }
        } else if i + 1 < segs.len() {
            types.push(json!({"type": segs[i], "name": segs[i + 1]}));
            i += 2;
        } else {
            types.push(json!({"type": segs[i], "name": Value::Null}));
            i += 1;
        }
    }
    let (resource_type, name) = match types.last() {
        Some(last) => (last["type"].clone(), last["name"].clone()),
        None => (Value::Null, Value::Null),
    };
    Ok(json!({
        "subscription": subscription,
        "resource_group": resource_group,
        "provider": provider,
        "types": types,
        "resource_type": resource_type,
        "name": name,
    }))
}

/// Assemble an Azure resource ID from parts, emitting canonical ARM casing
/// (`/subscriptions/…/resourceGroups/…/providers/…/type/name…`). opts:
/// subscription, resource_group, provider (all optional strings), and types — an
/// array of `{type, name?}` objects appended in order (a final entry may omit
/// `name`, e.g. a list endpoint). `provider` is required when `types` is
/// non-empty. Inverse of `parse_resource_id`. Returns `{id}`. Pure.
fn op_build_resource_id(opts: Value) -> Result<Value> {
    let mut id = String::new();
    let push = |id: &mut String, seg: &str| {
        id.push('/');
        id.push_str(seg);
    };
    if let Some(sub) = opts
        .get("subscription")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        push(&mut id, "subscriptions");
        push(&mut id, sub);
    }
    if let Some(rg) = opts
        .get("resource_group")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        push(&mut id, "resourceGroups");
        push(&mut id, rg);
    }
    let types = opts.get("types").and_then(Value::as_array);
    let has_types = types.is_some_and(|t| !t.is_empty());
    let provider = opts
        .get("provider")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    if has_types {
        let provider =
            provider.ok_or_else(|| anyhow!("provider required when types is non-empty"))?;
        push(&mut id, "providers");
        push(&mut id, provider);
        for t in types.unwrap() {
            let ty = t
                .get("type")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| anyhow!("type entry missing `type`"))?;
            push(&mut id, ty);
            if let Some(name) = t
                .get("name")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                push(&mut id, name);
            }
        }
    } else if let Some(provider) = provider {
        push(&mut id, "providers");
        push(&mut id, provider);
    }
    if id.is_empty() {
        return Err(anyhow!("no resource id parts supplied"));
    }
    Ok(json!({ "id": id }))
}

/// Parse an Azure connection string `Key=Value;Key=Value…` into a map. The
/// value keeps everything after the first `=`, so base64 `AccountKey` padding
/// (`==`) survives. Pure.
fn op_parse_connection_string(opts: Value) -> Result<Value> {
    let cs = opts
        .get("connection_string")
        .or_else(|| opts.get("dsn"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing connection_string"))?;
    let mut map = serde_json::Map::new();
    for pair in cs.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        match pair.split_once('=') {
            Some((k, v)) => {
                map.insert(k.trim().to_string(), json!(v));
            }
            None => return Err(anyhow!("malformed pair (no `=`): {pair}")),
        }
    }
    if map.is_empty() {
        return Err(anyhow!("empty connection string"));
    }
    Ok(json!({"pairs": Value::Object(map)}))
}

/// Assemble an Azure connection string `Key=Value;Key=Value…` from a `pairs`
/// object — the inverse of `parse_connection_string`. Keys keep their given
/// order (serde preserves insertion order), so a parsed string rebuilds
/// byte-identically. Keys must not contain `=` or `;`; values may contain `=`
/// (base64 `AccountKey` padding) but not the `;` delimiter. Returns
/// `{connection_string}`. Pure.
fn op_build_connection_string(opts: Value) -> Result<Value> {
    let pairs = opts
        .get("pairs")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("missing pairs (object)"))?;
    if pairs.is_empty() {
        return Err(anyhow!("pairs must not be empty"));
    }
    let mut parts = Vec::with_capacity(pairs.len());
    for (k, v) in pairs {
        if k.is_empty() || k.contains('=') || k.contains(';') {
            return Err(anyhow!("invalid key `{k}` (non-empty, no `=` or `;`)"));
        }
        let val = match v {
            Value::String(s) => s.clone(),
            Value::Number(_) | Value::Bool(_) => v.to_string(),
            _ => return Err(anyhow!("value for `{k}` must be a string, number, or bool")),
        };
        if val.contains(';') {
            return Err(anyhow!("value for `{k}` must not contain `;`"));
        }
        parts.push(format!("{k}={val}"));
    }
    Ok(json!({ "connection_string": parts.join(";") }))
}

/// Validate an Azure storage account name: 3–24 chars, lowercase letters and
/// numbers only. Returns `{valid, reason}`. Pure.
fn op_valid_storage_account_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let reason: Option<&str> = if name.len() < 3 || name.len() > 24 {
        Some("must be 3-24 characters")
    } else if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    {
        Some("only lowercase letters and numbers")
    } else {
        None
    };
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason}))
}

/// Validate an Azure Blob container name: 3–63 chars of `[a-z0-9-]`, start
/// alphanumeric, no consecutive hyphens, no trailing hyphen. Returns
/// `{valid, reason}`. Pure.
/// Shared rule for Azure storage names that must be valid DNS labels: 3–63
/// characters of lowercase letters, digits and hyphens, starting with an
/// alphanumeric, no consecutive hyphens, no trailing hyphen. Both Blob
/// containers and Queues share this exact grammar, so both validators route
/// through here. Returns `None` when valid, else the failure reason.
fn dns_label_reason(name: &str) -> Option<&'static str> {
    let bytes = name.as_bytes();
    if name.len() < 3 || name.len() > 63 {
        Some("must be 3-63 characters")
    } else if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        Some("only lowercase letters, numbers, and hyphens")
    } else if !bytes[0].is_ascii_alphanumeric() {
        Some("must start with a letter or number")
    } else if name.contains("--") {
        Some("must not contain consecutive hyphens")
    } else if bytes[bytes.len() - 1] == b'-' {
        Some("must not end with a hyphen")
    } else {
        None
    }
}

fn op_valid_container_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let reason = dns_label_reason(name);
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason}))
}

/// Validate an Azure Storage queue name. Queue names share the Blob-container
/// DNS-label grammar exactly (3–63 chars, lowercase alphanumeric and hyphens,
/// start alphanumeric, no consecutive/trailing hyphens) per the Queue service
/// naming rules, so this routes through `dns_label_reason`. Returns `{name,
/// valid, reason}`. Pure.
fn op_valid_queue_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let reason = dns_label_reason(name);
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason}))
}

/// Validate an Azure Table Storage table name per the documented grammar
/// `^[A-Za-z][A-Za-z0-9]{2,62}$`: 3–63 characters, alphanumeric only (no hyphens,
/// unlike a container), must begin with a letter (not a digit, unlike a storage
/// account), case-insensitive; `tables` is reserved. Returns `{name, valid,
/// reason}`. Pure.
fn op_valid_table_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let reason: Option<&str> = if name.len() < 3 || name.len() > 63 {
        Some("must be 3-63 characters")
    } else if !name.as_bytes()[0].is_ascii_alphabetic() {
        Some("must begin with a letter")
    } else if !name.bytes().all(|b| b.is_ascii_alphanumeric()) {
        Some("only alphanumeric characters (no hyphens or underscores)")
    } else if name.eq_ignore_ascii_case("tables") {
        Some("`tables` is a reserved table name")
    } else {
        None
    };
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason}))
}

/// Validate an Azure Cosmos DB resource ID — a database or container name —
/// against the documented limits
/// (learn.microsoft.com/azure/cosmos-db/concepts-limits): 1–255 characters, and
/// it may not contain `/` or `\` (the only characters the service forbids in an
/// id). Microsoft additionally recommends sticking to alphanumeric ASCII for
/// SDK/connector interoperability, which this does not enforce. opts: `id` (or
/// `name`). Returns `{id, valid, reason}`. Pure.
fn op_valid_cosmos_id(opts: Value) -> Result<Value> {
    let id = opts
        .get("id")
        .or_else(|| opts.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing id"))?;
    let reason: Option<&str> = if id.is_empty() {
        Some("must not be empty")
    } else if id.chars().count() > 255 {
        Some("must be at most 255 characters")
    } else if id.contains('/') || id.contains('\\') {
        Some("must not contain `/` or `\\`")
    } else {
        None
    };
    Ok(json!({"id": id, "valid": reason.is_none(), "reason": reason}))
}

/// Validate an Azure GUID — the canonical `8-4-4-4-12` hex form Azure uses for
/// subscription, tenant, client, and object IDs (e.g. the `subscription` that
/// feeds `build_resource_id`). Hex is case-insensitive; braces/URN prefixes are
/// not accepted. Returns `{guid, valid, reason}`. Pure.
fn op_valid_guid(opts: Value) -> Result<Value> {
    let guid = opts
        .get("guid")
        .or_else(|| opts.get("id"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing guid"))?;
    let groups: Vec<&str> = guid.split('-').collect();
    let reason: Option<&str> = if guid.len() != 36 {
        Some("must be 36 characters (8-4-4-4-12)")
    } else if groups.len() != 5
        || groups[0].len() != 8
        || groups[1].len() != 4
        || groups[2].len() != 4
        || groups[3].len() != 4
        || groups[4].len() != 12
    {
        Some("must be five hyphen-separated groups of 8-4-4-4-12")
    } else if !groups
        .iter()
        .all(|g| g.bytes().all(|b| b.is_ascii_hexdigit()))
    {
        Some("must contain only hexadecimal digits")
    } else {
        None
    };
    Ok(json!({"guid": guid, "valid": reason.is_none(), "reason": reason}))
}

/// Normalize a GUID/UUID to the canonical lowercase `8-4-4-4-12` form, accepting
/// the formats Azure IDs arrive in: hyphenated, hyphenless (32 hex), or wrapped
/// in braces `{…}` or parens `(…)`. Strips the wrapper and any hyphens, lowercases
/// the 32 hex digits, and re-groups them. opts: `guid` (or `id`). Returns
/// `{input, guid}`; errors unless exactly 32 hex digits remain. Pure.
/// Extract the 32 lowercase hex digits of a GUID given in any accepted input
/// format (bare `N`, hyphenated `D`, brace-wrapped `B`, or paren-wrapped `P`).
/// Shared by `normalize_guid` and `format_guid`; errors when the input is not 32
/// hexadecimal digits.
fn guid_hex(raw: &str) -> Result<String> {
    let s = raw.trim();
    // Strip a single pair of surrounding braces or parens.
    let s = s
        .strip_prefix('{')
        .and_then(|x| x.strip_suffix('}'))
        .or_else(|| s.strip_prefix('(').and_then(|x| x.strip_suffix(')')))
        .unwrap_or(s);
    let hex: String = s
        .chars()
        .filter(|c| *c != '-')
        .map(|c| c.to_ascii_lowercase())
        .collect();
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(anyhow!("not a GUID `{raw}` (need 32 hexadecimal digits)"));
    }
    Ok(hex)
}

/// Hyphenate 32 hex digits into the canonical `8-4-4-4-12` (`D`) form.
fn guid_hyphenate(hex: &str) -> String {
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn op_normalize_guid(opts: Value) -> Result<Value> {
    let raw = opts
        .get("guid")
        .or_else(|| opts.get("id"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing guid"))?;
    let guid = guid_hyphenate(&guid_hex(raw)?);
    Ok(json!({"input": raw, "guid": guid}))
}

/// Re-emit a GUID in one of the .NET `Guid.ToString` format specifiers — the
/// formatting companion of `normalize_guid` (which always produces the `D` form).
/// Accepts any input format and a target `format`: `N` (32 digits, no hyphens),
/// `D` (hyphenated, the default), `B` (`{hyphenated}`), or `P` (`(hyphenated)`).
/// Output hex is lowercase, matching .NET; the `X` specifier is not supported.
/// opts: `guid` (or `id`, required), `format` (default `D`, case-insensitive).
/// Returns `{input, format, guid}`. Pure.
fn op_format_guid(opts: Value) -> Result<Value> {
    let raw = opts
        .get("guid")
        .or_else(|| opts.get("id"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing guid"))?;
    let fmt = opts
        .get("format")
        .and_then(Value::as_str)
        .unwrap_or("D")
        .to_ascii_uppercase();
    let d = guid_hyphenate(&guid_hex(raw)?);
    let out = match fmt.as_str() {
        "N" => d.replace('-', ""),
        "D" => d,
        "B" => format!("{{{d}}}"),
        "P" => format!("({d})"),
        other => {
            return Err(anyhow!(
                "unsupported GUID format `{other}` (want N, D, B, or P)"
            ))
        }
    };
    Ok(json!({"input": raw, "format": fmt, "guid": out}))
}

/// Parse an Azure storage blob endpoint URL into its parts. Handles the
/// `https://<account>.<service>.core.windows.net/<container>/<blob>` form, where
/// `service` is `blob`, `dfs` (ADLS Gen2), `queue`, `table`, or `file`. Returns
/// `{account, service, host, container, blob}` (container/blob null when the URL
/// stops short). The `blob` keeps any nested path after the container. Pure.
/// The Azure storage services that share the `<account>.<service>.<suffix>` host
/// shape. Single source of truth for parse/build_blob_uri and storage_endpoint.
fn is_storage_service(s: &str) -> bool {
    matches!(s, "blob" | "dfs" | "queue" | "table" | "file")
}

/// The core storage DNS suffix for an Azure cloud (Microsoft "independent
/// clouds"): public → core.windows.net, china → core.chinacloudapi.cn, usgov →
/// core.usgovcloudapi.net.
fn storage_suffix_of(cloud: &str) -> Option<&'static str> {
    match cloud {
        "public" | "azure" | "azurecloud" => Some("core.windows.net"),
        "china" | "azurechina" | "azurechinacloud" => Some("core.chinacloudapi.cn"),
        "usgov" | "usgovernment" | "azureusgovernment" => Some("core.usgovcloudapi.net"),
        _ => None,
    }
}

/// Map a storage DNS suffix back to its canonical cloud name — the inverse of
/// `storage_suffix_of`, used by `parse_storage_endpoint`.
fn cloud_for_suffix(suffix: &str) -> Option<&'static str> {
    match suffix {
        "core.windows.net" => Some("public"),
        "core.chinacloudapi.cn" => Some("china"),
        "core.usgovcloudapi.net" => Some("usgov"),
        _ => None,
    }
}

fn op_parse_blob_uri(opts: Value) -> Result<Value> {
    let uri = opts
        .get("uri")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing uri"))?;
    let rest = uri
        .strip_prefix("https://")
        .or_else(|| uri.strip_prefix("http://"))
        .ok_or_else(|| anyhow!("not an http(s) blob URL: {uri}"))?;
    let (host, path) = match rest.split_once('/') {
        Some((h, p)) => (h, p),
        None => (rest, ""),
    };
    // Host is `<account>.<service>.core.windows.net`; account and service are the
    // first two labels.
    let labels: Vec<&str> = host.split('.').collect();
    if labels.len() < 3 || labels[0].is_empty() {
        return Err(anyhow!("not an Azure storage host: {host}"));
    }
    let account = labels[0];
    let service = labels[1];
    if !is_storage_service(service) {
        return Err(anyhow!(
            "unknown storage service `{service}` in host {host}"
        ));
    }
    let (container, blob) = match path.split_once('/') {
        Some((c, b)) if !b.is_empty() => (json!(c), json!(b)),
        Some((c, _)) => (json!(c), Value::Null),
        None if !path.is_empty() => (json!(path), Value::Null),
        None => (Value::Null, Value::Null),
    };
    Ok(json!({
        "account": account,
        "service": service,
        "host": host,
        "container": container,
        "blob": blob,
    }))
}

/// Build an Azure storage blob endpoint URL from parts — the inverse of
/// `parse_blob_uri`. opts: `account` (required), `service` (default `blob`; one
/// of blob/dfs/queue/table/file), `container` and `blob` (optional, in that
/// order). Produces `https://<account>.<service>.core.windows.net/[container[/blob]]`.
/// Pure.
fn op_build_blob_uri(opts: Value) -> Result<Value> {
    let account = opts
        .get("account")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing account"))?;
    let service = opts
        .get("service")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("blob");
    if !is_storage_service(service) {
        return Err(anyhow!("unknown storage service `{service}`"));
    }
    let opt = |k: &str| {
        opts.get(k)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
    };
    let mut uri = format!("https://{account}.{service}.core.windows.net");
    if let Some(container) = opt("container") {
        uri.push('/');
        uri.push_str(container);
        if let Some(blob) = opt("blob") {
            uri.push('/');
            uri.push_str(blob.trim_start_matches('/'));
        }
    }
    Ok(json!({"uri": uri}))
}

/// Build an Azure storage service endpoint `<account>.<service>.<suffix>` with
/// sovereign-cloud awareness — what `build_blob_uri` (hardwired to public
/// `core.windows.net`) can't reach. opts: `account` (required), `service`
/// (default `blob`; blob/dfs/queue/table/file), `cloud` (default `public`;
/// public/china/usgov). Returns `{account, service, cloud, suffix, endpoint,
/// url}`. Pure.
fn op_storage_endpoint(opts: Value) -> Result<Value> {
    let account = opts
        .get("account")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing account"))?;
    let service = opts
        .get("service")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("blob");
    if !is_storage_service(service) {
        return Err(anyhow!("unknown storage service `{service}`"));
    }
    let cloud = opts
        .get("cloud")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or("public")
        .to_ascii_lowercase();
    let suffix = storage_suffix_of(&cloud)
        .ok_or_else(|| anyhow!("unknown cloud `{cloud}` (public/china/usgov)"))?;
    let endpoint = format!("{account}.{service}.{suffix}");
    Ok(json!({
        "account": account,
        "service": service,
        "cloud": cloud,
        "suffix": suffix,
        "endpoint": endpoint,
        "url": format!("https://{endpoint}"),
    }))
}

/// Parse an Azure Storage endpoint back into its parts — the inverse of
/// `storage_endpoint`. Accepts a bare host (`acct.blob.core.windows.net`) or a
/// full URL (scheme and any path/query are stripped). The host is split into
/// `<account>.<service>.<suffix>`; the service is validated against the storage
/// services (blob/dfs/queue/table/file) and the suffix is mapped back to its
/// canonical cloud (public/china/usgov). opts: `endpoint` (or `url`). Returns
/// `{endpoint, account, service, cloud, suffix, url}`. Pure.
fn op_parse_storage_endpoint(opts: Value) -> Result<Value> {
    let raw = opts
        .get("endpoint")
        .or_else(|| opts.get("url"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing endpoint"))?;
    // Tolerate a full URL: drop the scheme and any path/query after the host.
    let host = raw
        .strip_prefix("https://")
        .or_else(|| raw.strip_prefix("http://"))
        .unwrap_or(raw);
    let host = host.split(['/', '?']).next().unwrap_or(host);
    let (account, rest) = host
        .split_once('.')
        .ok_or_else(|| anyhow!("not a storage endpoint host `{host}`"))?;
    if account.is_empty() {
        return Err(anyhow!("storage endpoint has no account: `{host}`"));
    }
    let (service, suffix) = rest
        .split_once('.')
        .ok_or_else(|| anyhow!("not a storage endpoint host `{host}`"))?;
    if !is_storage_service(service) {
        return Err(anyhow!("unknown storage service `{service}`"));
    }
    let cloud =
        cloud_for_suffix(suffix).ok_or_else(|| anyhow!("unknown storage suffix `{suffix}`"))?;
    let endpoint = format!("{account}.{service}.{suffix}");
    Ok(json!({
        "endpoint": endpoint,
        "account": account,
        "service": service,
        "cloud": cloud,
        "suffix": suffix,
        "url": format!("https://{endpoint}"),
    }))
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

#[no_mangle]
pub extern "C" fn azure__parse_resource_id(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_resource_id(opts) })
}

#[no_mangle]
pub extern "C" fn azure__build_resource_id(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_resource_id(opts) })
}

#[no_mangle]
pub extern "C" fn azure__parse_connection_string(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_connection_string(opts) })
}

#[no_mangle]
pub extern "C" fn azure__build_connection_string(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_connection_string(opts) })
}

#[no_mangle]
pub extern "C" fn azure__parse_blob_uri(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_blob_uri(opts) })
}

#[no_mangle]
pub extern "C" fn azure__build_blob_uri(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_blob_uri(opts) })
}

#[no_mangle]
pub extern "C" fn azure__storage_endpoint(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_storage_endpoint(opts) })
}

#[no_mangle]
pub extern "C" fn azure__parse_storage_endpoint(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_storage_endpoint(opts) })
}

#[no_mangle]
pub extern "C" fn azure__valid_storage_account_name(args: *const c_char) -> *const c_char {
    ffi_call_async(
        args,
        |opts| async move { op_valid_storage_account_name(opts) },
    )
}

#[no_mangle]
pub extern "C" fn azure__valid_container_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_container_name(opts) })
}

#[no_mangle]
pub extern "C" fn azure__valid_queue_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_queue_name(opts) })
}

#[no_mangle]
pub extern "C" fn azure__valid_table_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_table_name(opts) })
}

#[no_mangle]
pub extern "C" fn azure__valid_cosmos_id(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_cosmos_id(opts) })
}

#[no_mangle]
pub extern "C" fn azure__valid_guid(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_guid(opts) })
}

#[no_mangle]
pub extern "C" fn azure__normalize_guid(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_normalize_guid(opts) })
}

#[no_mangle]
pub extern "C" fn azure__format_guid(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_format_guid(opts) })
}

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

    // ── pure helpers (no Azure) ──────────────────────────────────────────────

    #[test]
    fn parse_resource_id_full_path() {
        let v = op_parse_resource_id(json!({
            "id": "/subscriptions/abc-123/resourceGroups/my-rg/providers/Microsoft.Storage/storageAccounts/mystore"
        }))
        .unwrap();
        assert_eq!(v["subscription"], json!("abc-123"));
        assert_eq!(v["resource_group"], json!("my-rg"));
        assert_eq!(v["provider"], json!("Microsoft.Storage"));
        assert_eq!(v["resource_type"], json!("storageAccounts"));
        assert_eq!(v["name"], json!("mystore"));
    }

    #[test]
    fn parse_resource_id_nested_type_takes_last_pair() {
        let v = op_parse_resource_id(json!({
            "id": "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/acct/blobServices/default"
        }))
        .unwrap();
        let types = v["types"].as_array().unwrap();
        assert_eq!(types.len(), 2, "two type/name pairs");
        assert_eq!(
            v["resource_type"],
            json!("blobServices"),
            "last pair is the resource"
        );
        assert_eq!(v["name"], json!("default"));
    }

    #[test]
    fn build_resource_id_inverts_parse_resource_id() {
        // Full path emits canonical ARM casing.
        let id = op_build_resource_id(json!({
            "subscription": "abc-123",
            "resource_group": "my-rg",
            "provider": "Microsoft.Storage",
            "types": [{"type": "storageAccounts", "name": "mystore"}]
        }))
        .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            id,
            "/subscriptions/abc-123/resourceGroups/my-rg/providers/Microsoft.Storage/storageAccounts/mystore"
        );
        // Round-trips through parse_resource_id, including a nested name-less tail.
        let original = "/subscriptions/s/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/acct/blobServices/default";
        let p = op_parse_resource_id(json!({ "id": original })).unwrap();
        let rebuilt = op_build_resource_id(json!({
            "subscription": p["subscription"],
            "resource_group": p["resource_group"],
            "provider": p["provider"],
            "types": p["types"],
        }))
        .unwrap()["id"]
            .clone();
        assert_eq!(rebuilt, json!(original));
        // Subscription-only id is valid (no provider/types).
        assert_eq!(
            op_build_resource_id(json!({"subscription": "s"})).unwrap()["id"],
            json!("/subscriptions/s")
        );
        // types without a provider is an error; empty input is an error.
        assert!(op_build_resource_id(json!({"types": [{"type": "x", "name": "y"}]})).is_err());
        assert!(op_build_resource_id(json!({})).is_err());
    }

    #[test]
    fn parse_connection_string_keeps_base64_account_key() {
        let v = op_parse_connection_string(json!({
            "connection_string": "DefaultEndpointsProtocol=https;AccountName=mystore;AccountKey=Zm9vYmFyYmF6==;EndpointSuffix=core.windows.net"
        }))
        .unwrap();
        assert_eq!(v["pairs"]["AccountName"], json!("mystore"));
        assert_eq!(
            v["pairs"]["AccountKey"],
            json!("Zm9vYmFyYmF6=="),
            "value keeps everything after the first = (base64 padding survives)"
        );
        assert_eq!(v["pairs"]["EndpointSuffix"], json!("core.windows.net"));
        assert!(
            op_parse_connection_string(json!({"connection_string": "no-equals-here"})).is_err()
        );
    }

    #[test]
    fn build_connection_string_inverts_parse_connection_string() {
        // Round-trips byte-identically thanks to preserve_order — base64 `==` and
        // key order both survive.
        let cs = "DefaultEndpointsProtocol=https;AccountName=mystore;AccountKey=Zm9vYmFyYmF6==;EndpointSuffix=core.windows.net";
        let parsed = op_parse_connection_string(json!({ "connection_string": cs })).unwrap();
        assert_eq!(
            op_build_connection_string(json!({ "pairs": parsed["pairs"] })).unwrap()
                ["connection_string"],
            json!(cs),
            "parse → build is byte-identical"
        );
        // Number / bool values stringify without JSON quoting.
        assert_eq!(
            op_build_connection_string(json!({"pairs": {"Port": 443, "Secure": true}})).unwrap()
                ["connection_string"],
            json!("Port=443;Secure=true")
        );
        // Reject `;` in a value, `=`/`;` in a key, and an empty map.
        assert!(op_build_connection_string(json!({"pairs": {"K": "a;b"}})).is_err());
        assert!(op_build_connection_string(json!({"pairs": {"bad=key": "v"}})).is_err());
        assert!(op_build_connection_string(json!({"pairs": {}})).is_err());
    }

    #[test]
    fn parse_blob_uri_splits_account_container_and_nested_blob() {
        let v = op_parse_blob_uri(json!({
            "uri": "https://mystore.blob.core.windows.net/images/2025/cat.png"
        }))
        .unwrap();
        assert_eq!(v["account"], json!("mystore"));
        assert_eq!(v["service"], json!("blob"));
        assert_eq!(v["container"], json!("images"));
        assert_eq!(v["blob"], json!("2025/cat.png"), "nested path kept in blob");
        // ADLS Gen2 dfs endpoint, container only (no blob).
        let dfs = op_parse_blob_uri(json!({
            "uri": "https://lake.dfs.core.windows.net/fs"
        }))
        .unwrap();
        assert_eq!(dfs["service"], json!("dfs"));
        assert_eq!(dfs["container"], json!("fs"));
        assert_eq!(dfs["blob"], Value::Null, "container-only URL has null blob");
        // Account endpoint with no container.
        let bare = op_parse_blob_uri(json!({
            "uri": "https://acct.blob.core.windows.net/"
        }))
        .unwrap();
        assert_eq!(bare["container"], Value::Null);
        // Non-https and unknown service rejected.
        assert!(op_parse_blob_uri(json!({"uri": "gs://x/y"})).is_err());
        assert!(op_parse_blob_uri(json!({
            "uri": "https://acct.bogus.core.windows.net/c"
        }))
        .is_err());
    }

    #[test]
    fn build_blob_uri_inverts_parse_blob_uri() {
        // Full account/container/nested-blob round-trips through parse.
        let built = op_build_blob_uri(json!({
            "account": "mystore", "container": "images", "blob": "2025/cat.png"
        }))
        .unwrap()["uri"]
            .clone();
        assert_eq!(
            built,
            json!("https://mystore.blob.core.windows.net/images/2025/cat.png")
        );
        let back = op_parse_blob_uri(json!({"uri": built})).unwrap();
        assert_eq!(back["account"], json!("mystore"));
        assert_eq!(back["container"], json!("images"));
        assert_eq!(back["blob"], json!("2025/cat.png"));
        // Service defaults to blob; dfs honored.
        assert_eq!(
            op_build_blob_uri(json!({"account": "lake", "service": "dfs", "container": "fs"}))
                .unwrap()["uri"],
            json!("https://lake.dfs.core.windows.net/fs")
        );
        // Account only → bare endpoint; blob ignored without a container.
        assert_eq!(
            op_build_blob_uri(json!({"account": "acct"})).unwrap()["uri"],
            json!("https://acct.blob.core.windows.net")
        );
        assert_eq!(
            op_build_blob_uri(json!({"account": "acct", "blob": "x"})).unwrap()["uri"],
            json!("https://acct.blob.core.windows.net"),
            "blob without container is dropped"
        );
        // Missing account and unknown service rejected.
        assert!(op_build_blob_uri(json!({"container": "c"})).is_err());
        assert!(op_build_blob_uri(json!({"account": "a", "service": "bogus"})).is_err());
    }

    #[test]
    fn storage_endpoint_supports_sovereign_clouds() {
        // Public cloud default.
        let v = op_storage_endpoint(json!({"account": "mystore"})).unwrap();
        assert_eq!(v["endpoint"], json!("mystore.blob.core.windows.net"));
        assert_eq!(v["url"], json!("https://mystore.blob.core.windows.net"));
        assert_eq!(v["cloud"], json!("public"));
        // Per-service.
        assert_eq!(
            op_storage_endpoint(json!({"account": "acct", "service": "queue"})).unwrap()
                ["endpoint"],
            json!("acct.queue.core.windows.net")
        );
        // China + US Gov suffixes — the differentiator vs build_blob_uri.
        assert_eq!(
            op_storage_endpoint(json!({"account": "acct", "cloud": "china"})).unwrap()["endpoint"],
            json!("acct.blob.core.chinacloudapi.cn")
        );
        assert_eq!(
            op_storage_endpoint(json!({"account": "acct", "service": "table", "cloud": "usgov"}))
                .unwrap()["endpoint"],
            json!("acct.table.core.usgovcloudapi.net")
        );
        // Cloud aliases are case-insensitive.
        assert_eq!(
            op_storage_endpoint(json!({"account": "a", "cloud": "AzureChinaCloud"})).unwrap()
                ["suffix"],
            json!("core.chinacloudapi.cn")
        );
        // Missing account, unknown service/cloud rejected.
        assert!(op_storage_endpoint(json!({})).is_err());
        assert!(op_storage_endpoint(json!({"account": "a", "service": "bogus"})).is_err());
        assert!(op_storage_endpoint(json!({"account": "a", "cloud": "mars"})).is_err());
    }

    #[test]
    fn parse_storage_endpoint_inverts_storage_endpoint() {
        // Bare host: every part recovered, suffix mapped to its canonical cloud.
        let v = op_parse_storage_endpoint(json!({"endpoint": "mystore.blob.core.windows.net"}))
            .unwrap();
        assert_eq!(v["account"], json!("mystore"));
        assert_eq!(v["service"], json!("blob"));
        assert_eq!(v["cloud"], json!("public"));
        assert_eq!(v["suffix"], json!("core.windows.net"));
        assert_eq!(v["url"], json!("https://mystore.blob.core.windows.net"));
        // A full URL with a path resolves to the same host.
        assert_eq!(
            op_parse_storage_endpoint(
                json!({"url": "https://acct.queue.core.windows.net/q/messages"})
            )
            .unwrap()["service"],
            json!("queue")
        );
        // Sovereign clouds map back to their canonical names.
        assert_eq!(
            op_parse_storage_endpoint(json!({"endpoint": "acct.table.core.usgovcloudapi.net"}))
                .unwrap()["cloud"],
            json!("usgov")
        );
        assert_eq!(
            op_parse_storage_endpoint(json!({"endpoint": "acct.blob.core.chinacloudapi.cn"}))
                .unwrap()["cloud"],
            json!("china")
        );
        // Round-trips storage_endpoint for every service × cloud.
        for (service, cloud) in [
            ("blob", "public"),
            ("queue", "china"),
            ("table", "usgov"),
            ("dfs", "public"),
            ("file", "public"),
        ] {
            let built =
                op_storage_endpoint(json!({"account": "acct", "service": service, "cloud": cloud}))
                    .unwrap()["endpoint"]
                    .as_str()
                    .unwrap()
                    .to_string();
            let p = op_parse_storage_endpoint(json!({ "endpoint": built })).unwrap();
            assert_eq!(p["account"], json!("acct"));
            assert_eq!(
                p["service"],
                json!(service),
                "service round-trip {service}/{cloud}"
            );
            assert_eq!(
                p["cloud"],
                json!(cloud),
                "cloud round-trip {service}/{cloud}"
            );
        }
        // Errors: not a storage host, unknown service, unknown suffix, missing.
        assert!(op_parse_storage_endpoint(json!({"endpoint": "acct"})).is_err());
        assert!(
            op_parse_storage_endpoint(json!({"endpoint": "acct.bogus.core.windows.net"})).is_err()
        );
        assert!(op_parse_storage_endpoint(json!({"endpoint": "acct.blob.example.com"})).is_err());
        assert!(op_parse_storage_endpoint(json!({})).is_err());
    }

    #[test]
    fn valid_storage_account_name_rules() {
        assert_eq!(
            op_valid_storage_account_name(json!({"name": "mystore123"})).unwrap()["valid"],
            json!(true)
        );
        for (name, want) in [
            ("ab", "3-24"),
            ("My-Store", "lowercase letters and numbers"),
        ] {
            let v = op_valid_storage_account_name(json!({"name": name})).unwrap();
            assert_eq!(v["valid"], json!(false), "{name}");
            assert!(v["reason"].as_str().unwrap().contains(want), "{name}");
        }
    }

    #[test]
    fn valid_container_name_rules() {
        assert_eq!(
            op_valid_container_name(json!({"name": "my-logs-2025"})).unwrap()["valid"],
            json!(true)
        );
        for (name, want) in [
            ("ab", "3-63"),
            ("Bad", "lowercase"),
            ("a--b", "consecutive hyphens"),
            ("bad-", "end with a hyphen"),
        ] {
            let v = op_valid_container_name(json!({"name": name})).unwrap();
            assert_eq!(v["valid"], json!(false), "{name}");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "{name}: {}",
                v["reason"]
            );
        }
    }

    #[test]
    fn valid_queue_name_shares_container_grammar() {
        // Queue names use the same DNS-label rule as containers.
        for name in ["my-queue-1", "orders", "abc"] {
            assert_eq!(
                op_valid_queue_name(json!({ "name": name })).unwrap()["valid"],
                json!(true),
                "{name}"
            );
            // Identical verdict to the container validator (shared helper).
            assert_eq!(
                op_valid_queue_name(json!({ "name": name })).unwrap(),
                op_valid_container_name(json!({ "name": name })).unwrap(),
                "{name}"
            );
        }
        for (name, want) in [
            ("ab", "3-63"),
            ("Queue", "lowercase"),
            ("-lead", "start with a letter or number"),
            ("a--b", "consecutive hyphens"),
            ("trail-", "end with a hyphen"),
        ] {
            let v = op_valid_queue_name(json!({ "name": name })).unwrap();
            assert_eq!(v["valid"], json!(false), "{name}");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "{name}: {}",
                v["reason"]
            );
        }
        assert!(op_valid_queue_name(json!({})).is_err());
    }

    #[test]
    fn valid_table_name_matches_documented_regex() {
        let chk = |n: &str| op_valid_table_name(json!({ "name": n })).unwrap();
        // Alphanumeric, letter-start, case preserved; min/max boundaries.
        for ok in [
            "MyTable",
            "abc",
            "Logs2025",
            &format!("a{}", "b".repeat(62)),
        ] {
            assert_eq!(chk(ok)["valid"], json!(true), "`{ok}` should be valid");
        }
        // Invalid by each rule, with a reason naming it.
        for (name, want) in [
            ("ab", "3-63"),
            (&"a".repeat(64), "3-63"),
            ("1table", "begin with a letter"),
            ("my-table", "alphanumeric"),
            ("my_table", "alphanumeric"),
            ("tables", "reserved"),
            ("TABLES", "reserved"),
        ] {
            let v = chk(name);
            assert_eq!(v["valid"], json!(false), "`{name}` should be invalid");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "`{name}`: reason `{}` should mention `{want}`",
                v["reason"]
            );
        }
        // A name that merely contains "tables" is fine.
        assert_eq!(chk("tablesx")["valid"], json!(true));
        assert!(op_valid_table_name(json!({})).is_err());
    }

    #[test]
    fn valid_cosmos_id_enforces_length_and_forbidden_slashes() {
        let chk = |id: &str| op_valid_cosmos_id(json!({ "id": id })).unwrap();
        // Valid: ordinary names, including ones with chars Table Storage forbids
        // (hyphens, dots) but Cosmos allows.
        assert_eq!(chk("my-database")["valid"], json!(true));
        assert_eq!(chk("Orders.2025_v2")["valid"], json!(true));
        assert_eq!(chk("a")["valid"], json!(true), "single char is fine");
        assert_eq!(chk(&"a".repeat(255))["valid"], json!(true));
        // 256 chars is too long.
        let long = chk(&"a".repeat(256));
        assert_eq!(long["valid"], json!(false));
        assert!(long["reason"].as_str().unwrap().contains("255"));
        // The two forbidden characters.
        let fwd = chk("a/b");
        assert_eq!(fwd["valid"], json!(false));
        assert!(fwd["reason"].as_str().unwrap().contains("/"));
        assert_eq!(chk("a\\b")["valid"], json!(false));
        // Empty rejected; `name` is an alias for `id`.
        assert_eq!(chk("")["valid"], json!(false));
        assert_eq!(
            op_valid_cosmos_id(json!({"name": "Inventory"})).unwrap()["valid"],
            json!(true)
        );
        assert!(op_valid_cosmos_id(json!({})).is_err());
    }

    #[test]
    fn valid_guid_enforces_8_4_4_4_12_hex() {
        let ok = |g: &str| {
            op_valid_guid(json!({ "guid": g })).unwrap()["valid"]
                .as_bool()
                .unwrap()
        };
        // A real subscription-style GUID, and uppercase hex.
        assert!(ok("3f2504e0-4f89-41d3-9a0c-0305e82c3301"));
        assert!(
            ok("3F2504E0-4F89-41D3-9A0C-0305E82C3301"),
            "hex is case-insensitive"
        );
        // Wrong length, wrong grouping, non-hex, and braces all reject.
        for (g, want) in [
            ("3f2504e0-4f89-41d3-9a0c-0305e82c330", "36 characters"),
            ("3f2504e04f8941d39a0c0305e82c3301xxxx", "8-4-4-4-12"),
            ("3g2504e0-4f89-41d3-9a0c-0305e82c3301", "hexadecimal"),
            ("{3f2504e0-4f89-41d3-9a0c-0305e82c3301}", "36 characters"),
        ] {
            let v = op_valid_guid(json!({ "guid": g })).unwrap();
            assert_eq!(v["valid"], json!(false), "{g} should be invalid");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "{g}: reason `{}` should mention `{want}`",
                v["reason"]
            );
        }
        assert!(op_valid_guid(json!({})).is_err());
    }

    #[test]
    fn normalize_guid_canonicalizes_azure_formats() {
        let canon = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
        let norm = |g: &str| {
            op_normalize_guid(json!({ "guid": g })).unwrap()["guid"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Already canonical, uppercase, braces, parens, and hyphenless all map
        // to the same lowercase 8-4-4-4-12 form.
        assert_eq!(norm(canon), canon);
        assert_eq!(norm("3F2504E0-4F89-41D3-9A0C-0305E82C3301"), canon);
        assert_eq!(norm("{3f2504e0-4f89-41d3-9a0c-0305e82c3301}"), canon);
        assert_eq!(norm("(3F2504E0-4F89-41D3-9A0C-0305E82C3301)"), canon);
        assert_eq!(norm("3f2504e04f8941d39a0c0305e82c3301"), canon);
        assert_eq!(norm("  3f2504e0-4f89-41d3-9a0c-0305e82c3301  "), canon);
        // The output validates under valid_guid.
        assert_eq!(
            op_valid_guid(json!({ "guid": norm("3f2504e04f8941d39a0c0305e82c3301") })).unwrap()
                ["valid"],
            json!(true)
        );
        // Too few/many hex digits, or non-hex, error.
        assert!(op_normalize_guid(json!({"guid": "3f2504e0"})).is_err());
        assert!(op_normalize_guid(json!({"guid": "3g2504e04f8941d39a0c0305e82c3301"})).is_err());
        assert!(op_normalize_guid(json!({})).is_err());
    }

    #[test]
    fn format_guid_emits_dotnet_specifiers() {
        let canon = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
        let fmt = |g: &str, f: &str| {
            op_format_guid(json!({ "guid": g, "format": f })).unwrap()["guid"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // The four punctuation forms, output lowercase like .NET.
        assert_eq!(fmt(canon, "N"), "3f2504e04f8941d39a0c0305e82c3301");
        assert_eq!(fmt(canon, "D"), canon);
        assert_eq!(fmt(canon, "B"), format!("{{{canon}}}"));
        assert_eq!(fmt(canon, "P"), format!("({canon})"));
        // The specifier is case-insensitive and any input format is accepted.
        assert_eq!(
            fmt("3F2504E04F8941D39A0C0305E82C3301", "b"),
            format!("{{{canon}}}")
        );
        // Default format is D.
        assert_eq!(
            op_format_guid(json!({"guid": canon})).unwrap()["guid"],
            json!(canon)
        );
        // normalize_guid is exactly format_guid in the D form.
        assert_eq!(
            fmt("{3f2504e0-4f89-41d3-9a0c-0305e82c3301}", "D"),
            op_normalize_guid(json!({"guid": canon})).unwrap()["guid"]
                .as_str()
                .unwrap()
        );
        // Bad format and bad GUID error.
        assert!(op_format_guid(json!({"guid": canon, "format": "X"})).is_err());
        assert!(op_format_guid(json!({"guid": "nope", "format": "N"})).is_err());
        assert!(op_format_guid(json!({})).is_err());
    }
}
