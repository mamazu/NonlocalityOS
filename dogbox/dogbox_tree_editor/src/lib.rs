use astraea::{
    storage::{LoadStoreValue, StoreError},
    tree::{BlobDigest, Reference, ReferenceIndex, TypeId, TypedReference, Value},
};
use async_stream::stream;
use bytes::Buf;
use dogbox_tree::serialization::{self, DirectoryTree, FileName};
use std::{
    collections::{BTreeMap, VecDeque},
    pin::Pin,
    sync::Arc,
};
use tokio::sync::Mutex;
use tracing::info;

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    NotFound,
    CannotOpenRegularFileAsDirectory,
    CannotOpenDirectoryAsRegularFile,
    Postcard(postcard::Error),
    ReferenceIndexOutOfRange,
}

pub type Result<T> = std::result::Result<T, Error>;
pub type Future<'a, T> = Pin<Box<dyn core::future::Future<Output = Result<T>> + Send + 'a>>;
pub type Stream<T> = Pin<Box<dyn futures_core::stream::Stream<Item = T> + Send>>;

#[derive(Clone, Debug, PartialEq)]
pub enum DirectoryEntryKind {
    Directory,
    File(u64),
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectoryEntry {
    pub name: String,
    pub kind: DirectoryEntryKind,
    pub digest: BlobDigest,
}

impl DirectoryEntry {
    pub fn new(name: String, kind: DirectoryEntryKind, digest: BlobDigest) -> Self {
        Self { name, kind, digest }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MutableDirectoryEntry {
    pub name: String,
    pub kind: DirectoryEntryKind,
}

impl MutableDirectoryEntry {
    pub fn new(name: String, kind: DirectoryEntryKind) -> Self {
        Self { name, kind }
    }
}

#[derive(Clone, Debug)]
pub enum NamedEntry {
    NotOpen(DirectoryEntryKind, BlobDigest),
    OpenRegularFile(Arc<OpenFile>),
    OpenSubdirectory(Arc<OpenDirectory>),
}

impl NamedEntry {
    async fn get_meta_data(&self) -> DirectoryEntryKind {
        match self {
            NamedEntry::NotOpen(kind, _) => kind.clone(),
            NamedEntry::OpenRegularFile(open_file) => open_file.get_meta_data().await,
            NamedEntry::OpenSubdirectory(_) => DirectoryEntryKind::Directory,
        }
    }

    fn poll_status<'t>(
        &'t self,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = std::result::Result<
                        (serialization::DirectoryEntryKind, BlobDigest),
                        StoreError,
                    >,
                > + Send
                + 't,
        >,
    > {
        match self {
            NamedEntry::NotOpen(directory_entry_kind, blob_digest) => {
                Box::pin(std::future::ready(Ok((
                    match *directory_entry_kind {
                        DirectoryEntryKind::Directory => {
                            serialization::DirectoryEntryKind::Directory
                        }
                        DirectoryEntryKind::File(size) => {
                            serialization::DirectoryEntryKind::File(size)
                        }
                    },
                    *blob_digest,
                ))))
            }
            NamedEntry::OpenRegularFile(open_file) => Box::pin(async move {
                let open_file_status = open_file.poll_status().await?;
                Ok((
                    serialization::DirectoryEntryKind::File(open_file_status.size),
                    open_file_status.content,
                ))
            }),
            NamedEntry::OpenSubdirectory(directory) => Box::pin(async move {
                let committed = directory.poll_status().await?;
                Ok((serialization::DirectoryEntryKind::Directory, committed))
            }),
        }
    }
}

#[derive(Debug)]
pub struct OpenDirectory {
    // TODO: support really big directories. We may not be able to hold all entries in memory at the same time.
    names: tokio::sync::Mutex<BTreeMap<String, NamedEntry>>,
    storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
}

impl OpenDirectory {
    pub fn new(
        names: tokio::sync::Mutex<BTreeMap<String, NamedEntry>>,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Self {
        Self { names, storage }
    }

    pub fn from_entries(
        entries: Vec<DirectoryEntry>,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> OpenDirectory {
        let names = BTreeMap::from_iter(entries.iter().map(|entry| {
            (
                entry.name.clone(),
                NamedEntry::NotOpen(entry.kind.clone(), entry.digest),
            )
        }));
        OpenDirectory {
            names: tokio::sync::Mutex::new(names),
            storage: storage.clone(),
        }
    }

    async fn read(&self) -> Stream<MutableDirectoryEntry> {
        let names_locked = self.names.lock().await;
        let snapshot = names_locked.clone();
        Box::pin(stream! {
            for cached_entry in snapshot {
                let kind = cached_entry.1.get_meta_data().await;
                yield MutableDirectoryEntry{name: cached_entry.0, kind: kind};
            }
        })
    }

    async fn get_meta_data(&self, name: &str) -> Result<DirectoryEntryKind> {
        let names_locked = self.names.lock().await;
        match names_locked.get(name) {
            Some(found) => {
                let found_clone = (*found).clone();
                Ok(found_clone.get_meta_data().await)
            }
            None => Err(Error::NotFound),
        }
    }

    async fn open_file(&self, name: &str) -> Result<Arc<OpenFile>> {
        let mut names_locked = self.names.lock().await;
        match names_locked.get_mut(name) {
            Some(found) => match found {
                NamedEntry::NotOpen(kind, _digest) => match kind {
                    DirectoryEntryKind::Directory => todo!(),
                    DirectoryEntryKind::File(length) => {
                        // TODO: read file contents. For now we assume that the example file is empty at the start.
                        assert_eq!(0, *length);
                        let open_file =
                            Arc::new(OpenFile::new(vec![], false, self.storage.clone()));
                        *found = NamedEntry::OpenRegularFile(open_file.clone());
                        Ok(open_file)
                    }
                },
                NamedEntry::OpenRegularFile(open_file) => Ok(open_file.clone()),
                NamedEntry::OpenSubdirectory(_) => Err(Error::CannotOpenDirectoryAsRegularFile),
            },
            None => {
                let open_file = Arc::new(OpenFile::new(vec![], true, self.storage.clone()));
                info!("Adding file {} to the directory", &name);
                names_locked.insert(
                    name.to_string(),
                    NamedEntry::OpenRegularFile(open_file.clone()),
                );
                Ok(open_file)
            }
        }
    }

    pub async fn load_directory(
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
        digest: &BlobDigest,
    ) -> Result<Arc<OpenDirectory>> {
        match storage.load_value(&Reference::new(*digest)) {
            Some(loaded) => {
                let parsed_directory: DirectoryTree = match postcard::from_bytes(&loaded.serialized)
                {
                    Ok(success) => success,
                    Err(error) => return Err(Error::Postcard(error)),
                };
                let mut entries = vec![];
                entries.reserve(parsed_directory.children.len());
                for maybe_entry in parsed_directory.children.iter().map(|child| {
                    let kind = match child.1.kind {
                        serialization::DirectoryEntryKind::Directory => {
                            DirectoryEntryKind::Directory
                        }
                        serialization::DirectoryEntryKind::File(size) => {
                            DirectoryEntryKind::File(size)
                        }
                    };
                    let index: usize = usize::try_from(child.1.digest.0)
                        .map_err(|_error| Error::ReferenceIndexOutOfRange)?;
                    if index >= loaded.references.len() {
                        return Err(Error::ReferenceIndexOutOfRange);
                    }
                    let digest = loaded.references[index].reference.digest;
                    Ok(DirectoryEntry::new(child.0.clone().into(), kind, digest))
                }) {
                    let entry = maybe_entry?;
                    entries.push(entry);
                }
                Ok(Arc::new(OpenDirectory::from_entries(entries, storage)))
            }
            None => todo!(),
        }
    }

    async fn open_subdirectory(&self, name: String) -> Result<Arc<OpenDirectory>> {
        let mut names_locked = self.names.lock().await;
        match names_locked.get_mut(&name) {
            Some(found) => match found {
                NamedEntry::NotOpen(kind, digest) => match kind {
                    DirectoryEntryKind::Directory => {
                        let subdirectory =
                            Self::load_directory(self.storage.clone(), digest).await?;
                        *found = NamedEntry::OpenSubdirectory(subdirectory.clone());
                        Ok(subdirectory)
                    }
                    DirectoryEntryKind::File(_) => Err(Error::CannotOpenRegularFileAsDirectory),
                },
                NamedEntry::OpenRegularFile(_) => Err(Error::CannotOpenRegularFileAsDirectory),
                NamedEntry::OpenSubdirectory(subdirectory) => Ok(subdirectory.clone()),
            },
            None => Err(Error::NotFound),
        }
    }

    async fn open_directory(
        self: &Arc<OpenDirectory>,
        path: NormalizedPath,
    ) -> Result<Arc<OpenDirectory>> {
        match path.split_left() {
            PathSplitLeftResult::Root => Ok(self.clone()),
            PathSplitLeftResult::Leaf(name) => self.open_subdirectory(name).await,
            PathSplitLeftResult::Directory(directory_name, tail) => {
                let subdirectory = self.open_subdirectory(directory_name).await?;
                Box::pin(subdirectory.open_directory(tail)).await
            }
        }
    }

    async fn create_directory(&self, name: String) -> Result<()> {
        let mut names_locked = self.names.lock().await;
        match names_locked.get(&name) {
            Some(_found) => todo!(),
            None => {
                names_locked.insert(
                    name,
                    NamedEntry::OpenSubdirectory(Arc::new(OpenDirectory::new(
                        Mutex::new(BTreeMap::new()),
                        self.storage.clone(),
                    ))),
                );
                Ok(())
            }
        }
    }

    pub async fn poll_status(&self) -> std::result::Result<BlobDigest, StoreError> {
        let names_locked = self.names.lock().await;
        let mut children = std::collections::BTreeMap::new();
        let mut references = Vec::new();
        for entry in names_locked.iter() {
            let name = FileName::try_from(entry.0.as_str()).unwrap();
            let (kind, digest) = entry.1.poll_status().await?;
            let reference_index = ReferenceIndex(references.len() as u64);
            references.push(TypedReference::new(
                TypeId(/*TODO get rid of this ID*/ 0),
                Reference::new(digest),
            ));
            children.insert(
                name,
                serialization::DirectoryEntry {
                    kind: kind,
                    digest: reference_index,
                },
            );
        }
        let tree = DirectoryTree { children };
        let serialized = postcard::to_allocvec(&tree).unwrap();
        let reference = self
            .storage
            .store_value(Arc::new(Value::new(serialized, references)))?;
        Ok(reference.digest)
    }
}

pub enum PathSplitLeftResult {
    Root,
    Leaf(String),
    Directory(String, NormalizedPath),
}

pub enum PathSplitRightResult {
    Root,
    Entry(NormalizedPath, String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedPath {
    components: VecDeque<String>,
}

impl NormalizedPath {
    pub fn new(input: &relative_path::RelativePath) -> NormalizedPath {
        NormalizedPath {
            components: input
                .normalize()
                .components()
                .map(|component| match component {
                    relative_path::Component::CurDir => todo!(),
                    relative_path::Component::ParentDir => todo!(),
                    relative_path::Component::Normal(name) => name.to_string(),
                })
                .collect(),
        }
    }

    pub fn root() -> NormalizedPath {
        NormalizedPath {
            components: VecDeque::new(),
        }
    }

    pub fn split_left(mut self) -> PathSplitLeftResult {
        let head = match self.components.pop_front() {
            Some(head) => head,
            None => return PathSplitLeftResult::Root,
        };
        if self.components.is_empty() {
            PathSplitLeftResult::Leaf(head)
        } else {
            PathSplitLeftResult::Directory(
                head,
                NormalizedPath {
                    components: self.components,
                },
            )
        }
    }

    pub fn split_right(mut self) -> PathSplitRightResult {
        let tail = match self.components.pop_back() {
            Some(tail) => tail,
            None => return PathSplitRightResult::Root,
        };
        PathSplitRightResult::Entry(self, tail)
    }
}

#[test]
fn test_normalized_path_new() {
    assert_eq!(
        NormalizedPath::root(),
        NormalizedPath::new(&relative_path::RelativePath::new(""))
    );
}

#[derive(Debug)]
pub struct FileContentDescription {
    content: BlobDigest,
    size: u64,
}

impl FileContentDescription {
    pub fn new(content: BlobDigest, size: u64) -> Self {
        Self { content, size }
    }
}

#[derive(Debug)]
struct OpenFileContentBuffer {
    data: Vec<u8>,
    has_uncommitted_changes: bool,
}

#[derive(Debug)]
pub struct OpenFile {
    content: tokio::sync::Mutex<OpenFileContentBuffer>,
    storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
}

impl OpenFile {
    pub fn new(
        content: Vec<u8>,
        has_uncommitted_changes: bool,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> OpenFile {
        OpenFile {
            content: tokio::sync::Mutex::new(OpenFileContentBuffer {
                data: content,
                has_uncommitted_changes: has_uncommitted_changes,
            }),
            storage: storage,
        }
    }

    pub async fn get_meta_data(&self) -> DirectoryEntryKind {
        DirectoryEntryKind::File(self.content.lock().await.data.len() as u64)
    }

    pub fn write_bytes(&self, position: u64, buf: bytes::Bytes) -> Future<()> {
        Box::pin(async move {
            let position_usize = position.try_into().unwrap();
            let mut content_locked = self.content.lock().await;
            let data = &mut content_locked.data;
            let previous_content_length = data.len();
            match data.split_at_mut_checked(position_usize) {
                Some((_, overwriting)) => {
                    let can_overwrite = usize::min(overwriting.len(), buf.len());
                    let (mut for_overwriting, for_extending) = buf.split_at(can_overwrite);
                    for_overwriting.copy_to_slice(overwriting.split_at_mut(can_overwrite).0);
                    data.extend(for_extending);
                }
                None => {
                    data.extend(
                        std::iter::repeat(0u8).take(position_usize - previous_content_length),
                    );
                    data.extend(buf);
                }
            };
            content_locked.has_uncommitted_changes = true;
            Ok(())
        })
    }

    pub fn read_bytes(&self, position: u64, count: usize) -> Future<bytes::Bytes> {
        Box::pin(async move {
            let content_locked = self.content.lock().await;
            let data = &content_locked.data;
            match data.split_at_checked(position.try_into().unwrap()) {
                Some((_, from_position)) => Ok(bytes::Bytes::copy_from_slice(match from_position
                    .split_at_checked(count)
                {
                    Some((result, _)) => result,
                    None => from_position,
                })),
                None => todo!(),
            }
        })
    }

    pub fn flush(&self) {}

    pub async fn poll_status(&self) -> std::result::Result<FileContentDescription, StoreError> {
        let mut content_locked = self.content.lock().await;
        let size = content_locked.data.len();
        let content_reference = self
            .storage
            .store_value(Arc::new(Value::new(content_locked.data.clone(), vec![])))?;
        content_locked.has_uncommitted_changes = false;
        Ok(FileContentDescription {
            content: content_reference.digest,
            size: size as u64,
        })
    }
}

pub struct TreeEditor {
    root: Arc<OpenDirectory>,
}

impl TreeEditor {
    pub fn new(root: Arc<OpenDirectory>) -> TreeEditor {
        Self { root }
    }

    pub async fn read_directory(
        &self,
        path: NormalizedPath,
    ) -> Result<Stream<MutableDirectoryEntry>> {
        let directory = match self.root.open_directory(path).await {
            Ok(opened) => opened,
            Err(error) => return Err(error),
        };
        Ok(directory.read().await)
    }

    pub fn get_meta_data<'a>(&self, path: NormalizedPath) -> Future<'a, DirectoryEntryKind> {
        match path.split_right() {
            PathSplitRightResult::Root => {
                Box::pin(std::future::ready(Ok(DirectoryEntryKind::Directory)))
            }
            PathSplitRightResult::Entry(directory_path, leaf_name) => {
                let root = self.root.clone();
                Box::pin(async move {
                    match root.open_directory(directory_path).await {
                        Ok(directory) => directory.get_meta_data(&leaf_name).await,
                        Err(error) => return Err(error),
                    }
                })
            }
        }
    }

    pub fn open_file<'a>(&self, path: NormalizedPath) -> Future<'a, Arc<OpenFile>> {
        match path.split_right() {
            PathSplitRightResult::Root => todo!(),
            PathSplitRightResult::Entry(directory_path, file_name) => {
                let root = self.root.clone();
                Box::pin(async move {
                    let directory = match root.open_directory(directory_path).await {
                        Ok(opened) => opened,
                        Err(error) => return Err(error),
                    };
                    directory.open_file(&file_name).await
                })
            }
        }
    }

    pub fn create_directory<'a>(&self, path: NormalizedPath) -> Future<'a, ()> {
        match path.split_right() {
            PathSplitRightResult::Root => todo!(),
            PathSplitRightResult::Entry(directory_path, file_name) => {
                let root = self.root.clone();
                Box::pin(async move {
                    match root.open_directory(directory_path).await {
                        Ok(directory) => directory.create_directory(file_name).await,
                        Err(error) => return Err(error),
                    }
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use astraea::storage::{InMemoryValueStorage, LoadValue, StoreValue};

    #[tokio::test]
    async fn test_open_directory_get_meta_data() {
        let expected = DirectoryEntryKind::File(12);
        let directory = OpenDirectory {
            names: tokio::sync::Mutex::new(BTreeMap::from([(
                "test.txt".to_string(),
                NamedEntry::NotOpen(expected.clone(), BlobDigest::hash(&[])),
            )])),
            storage: Arc::new(NeverUsedStorage {}),
        };
        let meta_data = directory.get_meta_data("test.txt").await.unwrap();
        assert_eq!(expected, meta_data);
    }

    #[tokio::test]
    async fn test_open_directory_poll_status() {
        let expected = DirectoryEntryKind::File(12);
        let storage = Arc::new(InMemoryValueStorage::empty());
        let directory = OpenDirectory {
            names: tokio::sync::Mutex::new(BTreeMap::from([(
                "test.txt".to_string(),
                NamedEntry::NotOpen(expected.clone(), BlobDigest::hash(&[])),
            )])),
            storage: storage.clone(),
        };
        let status: BlobDigest = directory.poll_status().await.unwrap();
        assert_eq!(
            BlobDigest::new(&[
                104, 239, 112, 74, 159, 151, 115, 53, 77, 79, 0, 61, 0, 255, 60, 199, 108, 6, 169,
                103, 74, 159, 244, 189, 32, 88, 122, 64, 159, 105, 106, 157, 205, 186, 47, 210,
                169, 3, 196, 19, 48, 211, 86, 202, 96, 177, 113, 146, 195, 171, 48, 102, 23, 244,
                236, 205, 2, 38, 202, 233, 41, 2, 52, 27
            ]),
            status
        );
        assert_eq!(1, storage.len());
    }

    #[tokio::test]
    async fn test_open_directory_open_file() {
        let directory = OpenDirectory {
            names: tokio::sync::Mutex::new(BTreeMap::new()),
            storage: Arc::new(NeverUsedStorage {}),
        };
        let file_name = "test.txt";
        let opened = directory.open_file(file_name).await.unwrap();
        opened.flush();
        assert_eq!(
            DirectoryEntryKind::File(0),
            directory.get_meta_data(file_name).await.unwrap()
        );
        use futures::StreamExt;
        let directory_entries: Vec<MutableDirectoryEntry> = directory.read().await.collect().await;
        assert_eq!(
            &[MutableDirectoryEntry {
                name: file_name.to_string(),
                kind: DirectoryEntryKind::File(0)
            }][..],
            &directory_entries[..]
        );
    }

    #[tokio::test]
    async fn test_read_directory_after_file_write() {
        let directory = OpenDirectory {
            names: tokio::sync::Mutex::new(BTreeMap::new()),
            storage: Arc::new(NeverUsedStorage {}),
        };
        let file_name = "test.txt";
        let opened = directory.open_file(file_name).await.unwrap();
        let file_content = &b"hello world"[..];
        opened.write_bytes(0, file_content.into()).await.unwrap();
        use futures::StreamExt;
        let directory_entries: Vec<MutableDirectoryEntry> = directory.read().await.collect().await;
        assert_eq!(
            &[MutableDirectoryEntry {
                name: file_name.to_string(),
                kind: DirectoryEntryKind::File(file_content.len() as u64)
            }][..],
            &directory_entries[..]
        );
    }

    #[tokio::test]
    async fn test_get_meta_data_after_file_write() {
        let directory = OpenDirectory {
            names: tokio::sync::Mutex::new(BTreeMap::new()),
            storage: Arc::new(NeverUsedStorage {}),
        };
        let file_name = "test.txt";
        let opened = directory.open_file(file_name).await.unwrap();
        let file_content = &b"hello world"[..];
        opened.write_bytes(0, file_content.into()).await.unwrap();
        assert_eq!(
            DirectoryEntryKind::File(file_content.len() as u64),
            directory.get_meta_data(file_name).await.unwrap()
        );
    }

    #[tokio::test]
    async fn test_read_empty_root() {
        use futures::StreamExt;
        let editor = TreeEditor::new(Arc::new(OpenDirectory::from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )));
        let mut directory = editor
            .read_directory(NormalizedPath::new(relative_path::RelativePath::new("/")))
            .await
            .unwrap();
        let end = directory.next().await;
        assert!(end.is_none());
    }

    struct NeverUsedStorage {}

    impl LoadValue for NeverUsedStorage {
        fn load_value(
            &self,
            _reference: &astraea::tree::Reference,
        ) -> Option<Arc<astraea::tree::Value>> {
            panic!()
        }
    }

    impl StoreValue for NeverUsedStorage {
        fn store_value(
            &self,
            _value: Arc<astraea::tree::Value>,
        ) -> std::result::Result<astraea::tree::Reference, StoreError> {
            panic!()
        }
    }

    impl LoadStoreValue for NeverUsedStorage {}

    #[tokio::test]
    async fn test_get_meta_data_of_root() {
        let editor = TreeEditor::new(Arc::new(OpenDirectory::from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )));
        let meta_data = editor
            .get_meta_data(NormalizedPath::new(relative_path::RelativePath::new("/")))
            .await
            .unwrap();
        assert_eq!(DirectoryEntryKind::Directory, meta_data);
    }

    #[tokio::test]
    async fn test_get_meta_data_of_non_normalized_path() {
        let editor = TreeEditor::new(Arc::new(OpenDirectory::from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )));
        let error = editor
            .get_meta_data(NormalizedPath::new(relative_path::RelativePath::new(
                "unknown.txt",
            )))
            .await
            .unwrap_err();
        assert_eq!(Error::NotFound, error);
    }

    #[tokio::test]
    async fn test_get_meta_data_of_unknown_path() {
        let editor = TreeEditor::new(Arc::new(OpenDirectory::from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )));
        let error = editor
            .get_meta_data(NormalizedPath::new(relative_path::RelativePath::new(
                "/unknown.txt",
            )))
            .await
            .unwrap_err();
        assert_eq!(Error::NotFound, error);
    }

    #[tokio::test]
    async fn test_get_meta_data_of_unknown_path_in_unknown_directory() {
        let editor = TreeEditor::new(Arc::new(OpenDirectory::from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )));
        let error = editor
            .get_meta_data(NormalizedPath::new(relative_path::RelativePath::new(
                "/unknown/file.txt",
            )))
            .await
            .unwrap_err();
        assert_eq!(Error::NotFound, error);
    }

    #[tokio::test]
    async fn test_read_directory_on_closed_regular_file() {
        let editor = TreeEditor::new(Arc::new(OpenDirectory::from_entries(
            vec![DirectoryEntry {
                name: "test.txt".to_string(),
                kind: DirectoryEntryKind::File(4),
                digest: BlobDigest::hash(b"TEST"),
            }],
            Arc::new(NeverUsedStorage {}),
        )));
        let result = editor
            .read_directory(NormalizedPath::new(relative_path::RelativePath::new(
                "/test.txt",
            )))
            .await;
        assert_eq!(Some(Error::CannotOpenRegularFileAsDirectory), result.err());
    }

    #[tokio::test]
    async fn test_read_directory_on_open_regular_file() {
        use relative_path::RelativePath;
        let editor = TreeEditor::new(Arc::new(OpenDirectory::from_entries(
            vec![DirectoryEntry {
                name: "test.txt".to_string(),
                kind: DirectoryEntryKind::File(0),
                digest: BlobDigest::hash(b""),
            }],
            Arc::new(NeverUsedStorage {}),
        )));
        let _open_file = editor
            .open_file(NormalizedPath::new(RelativePath::new("/test.txt")))
            .await
            .unwrap();
        let result = editor
            .read_directory(NormalizedPath::new(RelativePath::new("/test.txt")))
            .await;
        assert_eq!(Some(Error::CannotOpenRegularFileAsDirectory), result.err());
    }

    #[tokio::test]
    async fn test_create_directory() {
        use futures::StreamExt;
        use relative_path::RelativePath;
        let editor = TreeEditor::new(Arc::new(OpenDirectory::from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )));
        editor
            .create_directory(NormalizedPath::new(RelativePath::new("/test")))
            .await
            .unwrap();
        let mut reading = editor
            .read_directory(NormalizedPath::new(RelativePath::new("/")))
            .await
            .unwrap();
        let entry: MutableDirectoryEntry = reading.next().await.unwrap();
        assert_eq!(
            MutableDirectoryEntry {
                name: "test".to_string(),
                kind: DirectoryEntryKind::Directory,
            },
            entry
        );
        let end = reading.next().await;
        assert!(end.is_none());
    }

    #[tokio::test]
    async fn test_read_created_directory() {
        use futures::StreamExt;
        use relative_path::RelativePath;
        let editor = TreeEditor::new(Arc::new(OpenDirectory::from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )));
        editor
            .create_directory(NormalizedPath::new(RelativePath::new("/test")))
            .await
            .unwrap();
        let mut reading = editor
            .read_directory(NormalizedPath::new(RelativePath::new("/test")))
            .await
            .unwrap();
        let end = reading.next().await;
        assert!(end.is_none());
    }

    #[tokio::test]
    async fn test_nested_create_directory() {
        use futures::StreamExt;
        use relative_path::RelativePath;
        let editor = TreeEditor::new(Arc::new(OpenDirectory::from_entries(
            vec![],
            Arc::new(NeverUsedStorage {}),
        )));
        editor
            .create_directory(NormalizedPath::new(RelativePath::new("/test")))
            .await
            .unwrap();
        editor
            .create_directory(NormalizedPath::new(RelativePath::new("/test/subdir")))
            .await
            .unwrap();
        {
            let mut reading = editor
                .read_directory(NormalizedPath::new(relative_path::RelativePath::new(
                    "/test/subdir",
                )))
                .await
                .unwrap();
            let end = reading.next().await;
            assert!(end.is_none());
        }
        {
            let mut reading = editor
                .read_directory(NormalizedPath::new(relative_path::RelativePath::new(
                    "/test",
                )))
                .await
                .unwrap();
            let entry: MutableDirectoryEntry = reading.next().await.unwrap();
            assert_eq!(
                MutableDirectoryEntry {
                    name: "subdir".to_string(),
                    kind: DirectoryEntryKind::Directory
                },
                entry
            );
            let end = reading.next().await;
            assert!(end.is_none());
        }
        {
            let mut reading = editor
                .read_directory(NormalizedPath::new(relative_path::RelativePath::new("/")))
                .await
                .unwrap();
            let entry: MutableDirectoryEntry = reading.next().await.unwrap();
            assert_eq!(
                MutableDirectoryEntry {
                    name: "test".to_string(),
                    kind: DirectoryEntryKind::Directory
                },
                entry
            );
            let end = reading.next().await;
            assert!(end.is_none());
        }
    }
}
