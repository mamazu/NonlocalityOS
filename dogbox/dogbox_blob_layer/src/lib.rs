#![feature(array_chunks)]
use astraea::tree::BlobDigest;
use async_trait::async_trait;

#[test]
fn test_calculate_digest_empty() {
    // empty input digest matches example from https://en.wikipedia.org/wiki/SHA-3#Examples_of_SHA-3_variants
    let digest: [u8; 64] = BlobDigest::hash(&[]).into();
    assert_eq!("a69f73cca23a9ac5c8b567dc185a756e97c982164fe25859e0d1dcc1475c80a615b2123af1f5f94c11e3e9402c3ac558f500199d95b6d3e301758586281dcd26",hex::encode( digest  ));
}

#[test]
fn test_calculate_digest_non_empty() {
    let digest: [u8; 64] = BlobDigest::hash("Hello, world!".as_bytes()).into();
    assert_eq!("8e47f1185ffd014d238fabd02a1a32defe698cbf38c037a90e3c0a0a32370fb52cbd641250508502295fcabcbf676c09470b27443868c8e5f70e26dc337288af",hex::encode( digest  ));
}

#[async_trait]
pub trait ReadBlob: Send + Sync {
    async fn read_blob(&self, digest: &BlobDigest) -> Option<Vec<u8>>;
}

#[async_trait]
pub trait WriteBlob {
    async fn write_blob(&mut self, content: &[u8]) -> BlobDigest;
}

pub struct MemoryBlobStore {
    entries: std::collections::HashMap<BlobDigest, Vec<u8>>,
}

impl Default for MemoryBlobStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBlobStore {
    pub fn new() -> MemoryBlobStore {
        MemoryBlobStore {
            entries: std::collections::HashMap::new(),
        }
    }
}

#[async_trait]
impl ReadBlob for MemoryBlobStore {
    async fn read_blob(&self, digest: &BlobDigest) -> Option<Vec<u8>> {
        self.entries.get(digest).cloned()
    }
}

#[async_trait]
impl WriteBlob for MemoryBlobStore {
    async fn write_blob(&mut self, content: &[u8]) -> BlobDigest {
        let key = BlobDigest::hash(content);
        self.entries.entry(key).or_insert_with(|| content.into());
        key
    }
}

#[tokio::test]
async fn test_memory_blob_store_read_unknown() {
    let store = MemoryBlobStore::new();
    let result = store.read_blob(&BlobDigest::hash("test".as_bytes())).await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_memory_blob_store_write() {
    let mut store = MemoryBlobStore::new();
    let message = "1234".as_bytes();
    let expected_digest = BlobDigest::hash(message);
    let digest = store.write_blob(message).await;
    assert_eq!(expected_digest, digest);
    let result = store.read_blob(&expected_digest).await.unwrap();
    assert_eq!(message, &result[..]);
}
