use astraea::{
    storage::{LoadStoreValue, StoreError},
    tree::{
        BlobDigest, Reference, ReferenceIndex, TypeId, TypedReference, Value, ValueBlob,
        VALUE_BLOB_MAX_LENGTH,
    },
};
use async_stream::stream;
use bytes::Buf;
use dogbox_tree::serialization::{self, DirectoryTree, FileName};
use futures::FutureExt;
use std::{
    collections::{BTreeMap, VecDeque},
    pin::Pin,
    sync::Arc,
};
use tokio::sync::{Mutex, MutexGuard};
use tracing::info;

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    NotFound(String),
    CannotOpenRegularFileAsDirectory(String),
    CannotOpenDirectoryAsRegularFile,
    Postcard(postcard::Error),
    ReferenceIndexOutOfRange,
    FileSizeMismatch,
    CannotRename,
    MissingValue(BlobDigest),
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

pub enum OpenNamedEntryStatus {
    Directory(OpenDirectoryStatus),
    File(OpenFileStatus),
}

pub enum NamedEntryStatus {
    Closed(serialization::DirectoryEntryKind, BlobDigest),
    Open(OpenNamedEntryStatus),
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

    async fn wait_for_next_change(
        &self,
    ) -> (
        std::result::Result<NamedEntryStatus, StoreError>,
        Option<
            Pin<Box<dyn std::future::Future<Output = std::result::Result<(), StoreError>> + Send>>,
        >,
    ) {
        match self {
            NamedEntry::NotOpen(directory_entry_kind, blob_digest) => (
                Ok(NamedEntryStatus::Closed(
                    match *directory_entry_kind {
                        DirectoryEntryKind::Directory => {
                            serialization::DirectoryEntryKind::Directory
                        }
                        DirectoryEntryKind::File(size) => {
                            serialization::DirectoryEntryKind::File(size)
                        }
                    },
                    *blob_digest,
                )),
                None,
            ),
            NamedEntry::OpenRegularFile(open_file) => {
                let (maybe_open_file_status, change_event_future) =
                    open_file.wait_for_next_change().await;
                (
                    maybe_open_file_status.map(|open_file_status| {
                        NamedEntryStatus::Open(OpenNamedEntryStatus::File(open_file_status))
                    }),
                    Some(Box::pin(change_event_future.map(|success| Ok(success)))),
                )
            }
            NamedEntry::OpenSubdirectory(directory) => {
                let (maybe_open_directory_status, change_event_future) =
                    directory.wait_for_next_change().await;
                (
                    maybe_open_directory_status.map(|open_directory_status| {
                        NamedEntryStatus::Open(OpenNamedEntryStatus::Directory(
                            open_directory_status,
                        ))
                    }),
                    Some(change_event_future),
                )
            }
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct OpenDirectoryStatus {
    pub digest: BlobDigest,
    pub directories_open_count: usize,
    pub files_open_count: usize,
    pub files_open_for_writing_count: usize,
    pub files_unflushed_count: usize,
    pub bytes_unflushed_count: u64,
}

impl OpenDirectoryStatus {
    pub fn new(
        digest: BlobDigest,
        directories_open_count: usize,
        files_open_count: usize,
        files_open_for_writing_count: usize,
        files_unflushed_count: usize,
        bytes_unflushed_count: u64,
    ) -> Self {
        Self {
            digest,
            directories_open_count,
            files_open_count,
            files_open_for_writing_count,
            files_unflushed_count,
            bytes_unflushed_count,
        }
    }
}

#[derive(Debug)]
pub struct OpenDirectory {
    // TODO: support really big directories. We may not be able to hold all entries in memory at the same time.
    names: tokio::sync::Mutex<BTreeMap<String, NamedEntry>>,
    storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    change_event_sender: tokio::sync::watch::Sender<()>,
    _change_event_receiver: tokio::sync::watch::Receiver<()>,
}

impl OpenDirectory {
    pub fn new(
        names: tokio::sync::Mutex<BTreeMap<String, NamedEntry>>,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Self {
        let (change_event_sender, change_event_receiver) = tokio::sync::watch::channel(());
        Self {
            names,
            storage,
            change_event_sender,
            _change_event_receiver: change_event_receiver,
        }
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
        OpenDirectory::new(tokio::sync::Mutex::new(names), storage.clone())
    }

    async fn read(&self) -> Stream<MutableDirectoryEntry> {
        let names_locked = self.names.lock().await;
        let snapshot = names_locked.clone();
        info!("Reading directory with {} entries", snapshot.len());
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
            None => Err(Error::NotFound(name.to_string())),
        }
    }

    async fn open_file(&self, name: &str) -> Result<Arc<OpenFile>> {
        let mut names_locked = self.names.lock().await;
        match names_locked.get_mut(name) {
            Some(found) => match found {
                NamedEntry::NotOpen(kind, digest) => match kind {
                    DirectoryEntryKind::Directory => todo!(),
                    DirectoryEntryKind::File(length) => {
                        let open_file = Arc::new(OpenFile::new(
                            OpenFileContentBuffer::from_storage(digest.clone(), *length),
                            self.storage.clone(),
                        ));
                        *found = NamedEntry::OpenRegularFile(open_file.clone());
                        info!(
                            "Opening file {} sends a change event for its parent directory.",
                            &name
                        );
                        self.change_event_sender.send(()).unwrap();
                        Ok(open_file)
                    }
                },
                NamedEntry::OpenRegularFile(open_file) => Ok(open_file.clone()),
                NamedEntry::OpenSubdirectory(_) => Err(Error::CannotOpenDirectoryAsRegularFile),
            },
            None => {
                let open_file = Arc::new(OpenFile::new(
                    OpenFileContentBuffer::from_data(vec![]).unwrap(),
                    self.storage.clone(),
                ));
                info!("Adding file {} to the directory which sends a change event for its parent directory.", &name);
                names_locked.insert(
                    name.to_string(),
                    NamedEntry::OpenRegularFile(open_file.clone()),
                );
                self.change_event_sender.send(()).unwrap();
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
                let parsed_directory: DirectoryTree =
                    match postcard::from_bytes(loaded.blob.as_slice()) {
                        Ok(success) => success,
                        Err(error) => return Err(Error::Postcard(error)),
                    };
                let mut entries = vec![];
                info!(
                    "Loading directory with {} entries",
                    parsed_directory.children.len()
                );
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
            Some(found) => {
                match found {
                    NamedEntry::NotOpen(kind, digest) => match kind {
                        DirectoryEntryKind::Directory => {
                            let subdirectory =
                                Self::load_directory(self.storage.clone(), digest).await?;
                            *found = NamedEntry::OpenSubdirectory(subdirectory.clone());
                            info!("Opening directory {} sends a change event for its parent directory.", &name);
                            self.change_event_sender.send(()).unwrap();
                            Ok(subdirectory)
                        }
                        DirectoryEntryKind::File(_) => {
                            Err(Error::CannotOpenRegularFileAsDirectory(name.to_string()))
                        }
                    },
                    NamedEntry::OpenRegularFile(_) => {
                        Err(Error::CannotOpenRegularFileAsDirectory(name.to_string()))
                    }
                    NamedEntry::OpenSubdirectory(subdirectory) => Ok(subdirectory.clone()),
                }
            }
            None => Err(Error::NotFound(name.to_string())),
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
                info!(
                    "Creating directory {} sends a change event for its parent directory.",
                    &name
                );
                self.change_event_sender.send(()).unwrap();
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

    pub async fn remove(&self, name_here: &str) -> Result<()> {
        let mut names_locked = self.names.lock().await;
        if !names_locked.contains_key(name_here) {
            return Err(Error::NotFound(name_here.to_string()));
        }

        self.change_event_sender.send(()).unwrap();
        names_locked.remove(name_here);
        Ok(())
    }

    pub async fn rename(
        &self,
        name_here: &str,
        there: &OpenDirectory,
        name_there: &str,
    ) -> Result<()> {
        let mut names_locked: MutexGuard<'_, BTreeMap<String, NamedEntry>>;
        let names_there_locked: Option<MutexGuard<'_, BTreeMap<String, NamedEntry>>>;

        let comparison = std::ptr::from_ref(self).cmp(&std::ptr::from_ref(there));
        match comparison {
            std::cmp::Ordering::Less => {
                names_locked = self.names.lock().await;
                names_there_locked = Some(there.names.lock().await);
            }
            std::cmp::Ordering::Equal => {
                names_locked = self.names.lock().await;
                names_there_locked = None;
            }
            std::cmp::Ordering::Greater => {
                names_there_locked = Some(there.names.lock().await);
                names_locked = self.names.lock().await;
            }
        }

        match names_locked.get(name_here) {
            Some(_) => {}
            None => return Err(Error::NotFound(name_here.to_string())),
        }

        info!(
            "Renaming from {} to {} sending a change event to the directory.",
            name_here, name_there
        );

        self.change_event_sender.send(()).unwrap();
        if names_there_locked.is_some() {
            there.change_event_sender.send(()).unwrap();
        }

        let (_obsolete_name, entry) = names_locked.remove_entry(name_here).unwrap();
        match names_there_locked {
            Some(value) => Self::write_into_directory(value, name_there, entry),
            None => Self::write_into_directory(names_locked, name_there, entry),
        }
        Ok(())
    }

    fn write_into_directory(
        mut names: MutexGuard<'_, BTreeMap<String, NamedEntry>>,
        name_there: &str,
        entry: NamedEntry,
    ) {
        match names.get_mut(name_there) {
            Some(existing_name) => *existing_name = entry,
            None => {
                names.insert(name_there.to_string(), entry);
            }
        };
    }

    pub fn wait_for_next_change<'t>(
        &'t self,
    ) -> Pin<
        Box<
            (dyn std::future::Future<
                Output = (
                    std::result::Result<OpenDirectoryStatus, StoreError>,
                    Pin<
                        Box<
                            (dyn std::future::Future<Output = std::result::Result<(), StoreError>>
                                 + Send),
                        >,
                    >,
                ),
            > + Send
                 + 't),
        >,
    > {
        Box::pin(async move {
            let names_locked = self.names.lock().await;
            let mut children = std::collections::BTreeMap::new();
            let mut references = Vec::new();
            let mut directories_open_count: usize=/*count self*/ 1;
            let mut files_open_count: usize = 0;
            let mut files_open_for_writing_count: usize = 0;
            let mut files_unflushed_count: usize = 0;
            let mut bytes_unflushed_count: u64 = 0;
            let mut futures: Vec<
                Pin<
                    Box<
                        dyn std::future::Future<Output = std::result::Result<(), StoreError>>
                            + Send,
                    >,
                >,
            > = {
                let mut receiver = self.change_event_sender.subscribe();
                vec![Box::pin(async move {
                    receiver.changed().await.unwrap();
                    info!("Something about the directory itself changed.");
                    Ok(())
                })]
            };
            let mut store_error: Option<StoreError> = None;
            for entry in names_locked.iter() {
                let name = FileName::try_from(entry.0.as_str()).unwrap();
                let (maybe_named_entry_status, maybe_next_change_future) =
                    entry.1.wait_for_next_change().await;
                let named_entry_status = match maybe_named_entry_status {
                    Ok(success) => success,
                    Err(error) => {
                        store_error = Some(error);
                        break;
                    }
                };
                if let Some(next_change_future) = maybe_next_change_future {
                    futures.push(next_change_future);
                }
                let (kind, digest) = match named_entry_status {
                    NamedEntryStatus::Closed(directory_entry_kind, blob_digest) => {
                        (directory_entry_kind, blob_digest)
                    }
                    NamedEntryStatus::Open(open_named_entry_status) => {
                        match open_named_entry_status {
                            OpenNamedEntryStatus::Directory(open_directory_status) => {
                                directories_open_count +=
                                    open_directory_status.directories_open_count;
                                files_open_count += open_directory_status.files_open_count;
                                files_open_for_writing_count =
                                    open_directory_status.files_open_for_writing_count;
                                files_unflushed_count +=
                                    open_directory_status.files_unflushed_count;
                                bytes_unflushed_count +=
                                    open_directory_status.bytes_unflushed_count;
                                (
                                    serialization::DirectoryEntryKind::Directory,
                                    open_directory_status.digest,
                                )
                            }
                            OpenNamedEntryStatus::File(open_file_status) => {
                                files_open_count += 1;
                                if open_file_status.is_writeable {
                                    files_open_for_writing_count += 1;
                                }
                                if open_file_status.bytes_unflushed_count > 0 {
                                    files_unflushed_count += 1;
                                }
                                bytes_unflushed_count += open_file_status.bytes_unflushed_count;
                                (
                                    serialization::DirectoryEntryKind::File(open_file_status.size),
                                    open_file_status.digest,
                                )
                            }
                        }
                    }
                };
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
            let change_event_future_result: Pin<
                Box<(dyn std::future::Future<Output = std::result::Result<(), StoreError>> + Send)>,
            > = {
                let join_any = async move {
                    let (selected, index, _) = futures::future::select_all(futures).await;
                    info!("Selected future at index {}.", index);
                    selected
                };
                Box::pin(join_any)
            };
            let status_result: std::result::Result<OpenDirectoryStatus, StoreError> =
                match store_error {
                    Some(error) => Err(error),
                    None => {
                        info!("Storing directory with {} entries", children.len());
                        let maybe_value_blob = ValueBlob::try_from(
                            postcard::to_allocvec(&DirectoryTree { children }).unwrap(),
                        );
                        match maybe_value_blob {
                            Some(value_blob) => self
                                .storage
                                .store_value(Arc::new(Value::new(value_blob, references)))
                                .map(|reference| {
                                    OpenDirectoryStatus::new(
                                        reference.digest,
                                        directories_open_count,
                                        files_open_count,
                                        files_open_for_writing_count,
                                        files_unflushed_count,
                                        bytes_unflushed_count,
                                    )
                                }),
                            None => todo!(),
                        }
                    }
                };
            (status_result, change_event_future_result)
        })
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

pub struct OpenFileStatus {
    pub digest: BlobDigest,
    pub size: u64,
    pub is_writeable: bool,
    pub bytes_unflushed_count: u64,
}

#[derive(Debug)]
pub enum OpenFileContentBlock {
    NotLoaded(BlobDigest, u16),
    Loaded(BlobDigest, Vec<u8>),
    Edited(Option<BlobDigest>, Vec<u8>),
}

impl OpenFileContentBlock {
    pub async fn access_content<'t>(
        &'t mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<&'t mut Vec<u8>> {
        match self {
            OpenFileContentBlock::NotLoaded(blob_digest, size) => {
                let loaded = match storage.load_value(&Reference::new(*blob_digest)) {
                    Some(success) => success,
                    None => return Err(Error::MissingValue(*blob_digest)),
                };
                if loaded.blob.as_slice().len() != *size as usize {
                    return Err(Error::FileSizeMismatch);
                }
                *self = OpenFileContentBlock::Loaded(
                    *blob_digest,
                    /*TODO: avoid cloning*/ loaded.blob.as_slice().to_vec(),
                );
            }
            OpenFileContentBlock::Loaded(_blob_digest, _vec) => {}
            OpenFileContentBlock::Edited(_blob_digest, _vec) => {}
        }
        Ok(match self {
            OpenFileContentBlock::NotLoaded(_blob_digest, _) => panic!(),
            OpenFileContentBlock::Loaded(_blob_digest, vec) => vec,
            OpenFileContentBlock::Edited(_blob_digest, vec) => vec,
        })
    }

    pub async fn store(
        &mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> std::result::Result<BlobDigest, StoreError> {
        match self {
            OpenFileContentBlock::NotLoaded(blob_digest, _) => Ok(*blob_digest),
            OpenFileContentBlock::Loaded(blob_digest, _vec) => Ok(*blob_digest),
            OpenFileContentBlock::Edited(_blob_digest, vec) => {
                storage
                    .store_value(Arc::new(Value::new(
                        ValueBlob::try_from( vec.clone()).unwrap(/*TODO*/),
                        vec![],
                    )))
                    .map(|success| success.digest)
            }
        }
    }

    pub fn size(&self) -> u16 {
        match self {
            OpenFileContentBlock::NotLoaded(_blob_digest, size) => *size,
            OpenFileContentBlock::Loaded(_blob_digest, vec) => vec.len() as u16,
            OpenFileContentBlock::Edited(_blob_digest, vec) => vec.len() as u16,
        }
    }
}

#[derive(Debug)]
pub struct OpenFileContentBufferLoaded {
    size: u64,
    blocks: Vec<OpenFileContentBlock>,
}

impl OpenFileContentBufferLoaded {
    pub fn new(size: u64, blocks: Vec<OpenFileContentBlock>) -> Self {
        Self { size, blocks }
    }

    pub async fn store(
        &mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> std::result::Result<(Reference, u64), StoreError> {
        let mut blocks_stored = Vec::new();
        for block in self.blocks.iter_mut() {
            let block_stored = block.store(storage.clone()).await?;
            blocks_stored.push(block_stored);
        }
        if blocks_stored.len() != 1 {
            todo!()
        }
        Ok((Reference::new(blocks_stored[0]), self.size))
    }
}

#[derive(Debug)]
pub enum OpenFileContentBuffer {
    NotLoaded { digest: BlobDigest, size: u64 },
    Loaded(OpenFileContentBufferLoaded),
}

impl OpenFileContentBuffer {
    pub fn from_storage(digest: BlobDigest, size: u64) -> Self {
        Self::NotLoaded {
            digest: digest,
            size: size,
        }
    }

    pub fn from_data(data: Vec<u8>) -> Option<Self> {
        if data.len() > VALUE_BLOB_MAX_LENGTH {
            None
        } else {
            Some(Self::Loaded(OpenFileContentBufferLoaded {
                size: data.len() as u64,
                blocks: vec![OpenFileContentBlock::Edited(None, data)],
            }))
        }
    }

    pub fn size(&self) -> u64 {
        match self {
            OpenFileContentBuffer::NotLoaded { digest: _, size } => *size,
            OpenFileContentBuffer::Loaded(OpenFileContentBufferLoaded { size, blocks: _ }) => *size,
        }
    }

    pub async fn read(
        &mut self,
        position: u64,
        count: usize,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<bytes::Bytes> {
        let loaded = self.require_loaded(storage.clone()).await?;
        Self::read_from_blocks(&mut loaded.blocks, position, count, storage).await
    }

    async fn require_loaded<'t>(
        &'t mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<&'t mut OpenFileContentBufferLoaded> {
        match self {
            OpenFileContentBuffer::NotLoaded { digest, size } => {
                let blocks = vec![OpenFileContentBlock::Loaded(
                    *digest,
                    if *size > 0 {
                        let file_value =
                            storage.load_value(&Reference::new(digest.clone())).unwrap(/*TODO*/);
                        if *size != file_value.blob.as_slice().len() as u64 {
                            return Err(Error::FileSizeMismatch);
                        }
                        // TODO: avoid clone
                        file_value.blob.as_slice().to_vec()
                    } else {
                        // No need to load the content of a file we already know is empty.
                        vec![]
                    },
                )];
                *self = Self::Loaded(OpenFileContentBufferLoaded {
                    size: *size,
                    blocks: blocks,
                });
            }
            OpenFileContentBuffer::Loaded(_loaded) => {}
        }
        match self {
            OpenFileContentBuffer::NotLoaded { digest: _, size: _ } => panic!(),
            OpenFileContentBuffer::Loaded(open_file_content_buffer_loaded) => {
                Ok(open_file_content_buffer_loaded)
            }
        }
    }

    async fn read_from_blocks(
        blocks: &mut Vec<OpenFileContentBlock>,
        position: u64,
        count: usize,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<bytes::Bytes> {
        let block_size = VALUE_BLOB_MAX_LENGTH;
        let first_block_index = position / (block_size as u64);
        if first_block_index >= (blocks.len() as u64) {
            todo!()
        }
        let next_block_index = first_block_index as usize;
        let block = &mut blocks[next_block_index];
        let data = block.access_content(storage).await?;
        match data.split_at_checked(position.try_into().unwrap()) {
            Some((_, from_position)) => {
                return Ok(bytes::Bytes::copy_from_slice(
                    match from_position.split_at_checked(count) {
                        Some((result, _)) => result,
                        None => from_position,
                    },
                ))
            }
            None => todo!(),
        }
    }

    pub async fn write(
        &mut self,
        position: u64,
        buf: bytes::Bytes,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> Result<()> {
        let loaded = self.require_loaded(storage.clone()).await?;
        let first_block_index = position / (VALUE_BLOB_MAX_LENGTH as u64);
        if first_block_index >= (loaded.blocks.len() as u64) {
            todo!()
        }
        let position_in_block = (position % (VALUE_BLOB_MAX_LENGTH as u64)) as usize;
        let block = &mut loaded.blocks[first_block_index as usize];
        let previous_content_length = block.size();
        let data = block.access_content(storage).await?;
        match data.split_at_mut_checked(position_in_block) {
            Some((_, overwriting)) => {
                let can_overwrite = usize::min(overwriting.len(), buf.len());
                let (mut for_overwriting, for_extending) = buf.split_at(can_overwrite);
                for_overwriting.copy_to_slice(overwriting.split_at_mut(can_overwrite).0);
                data.extend(for_extending);
            }
            None => {
                data.extend(
                    std::iter::repeat(0u8)
                        .take(position_in_block - (previous_content_length as usize)),
                );
                data.extend(buf);
            }
        };
        loaded.size = data.len() as u64;
        Ok(())
    }

    pub async fn store(
        &mut self,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> std::result::Result<(Reference, u64), StoreError> {
        match self {
            OpenFileContentBuffer::Loaded(open_file_content_buffer_loaded) => {
                open_file_content_buffer_loaded.store(storage).await
            }
            OpenFileContentBuffer::NotLoaded { digest, size } => {
                Ok((Reference::new(*digest), *size))
            }
        }
    }
}

#[derive(Debug)]
pub struct OpenFile {
    content: tokio::sync::Mutex<OpenFileContentBuffer>,
    storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    change_event_sender: tokio::sync::watch::Sender<()>,
    _change_event_receiver: tokio::sync::watch::Receiver<()>,
}

impl OpenFile {
    pub fn new(
        content: OpenFileContentBuffer,
        storage: Arc<(dyn LoadStoreValue + Send + Sync)>,
    ) -> OpenFile {
        let (sender, receiver) = tokio::sync::watch::channel(());
        OpenFile {
            content: tokio::sync::Mutex::new(content),
            storage: storage,
            change_event_sender: sender,
            _change_event_receiver: receiver,
        }
    }

    pub async fn get_meta_data(&self) -> DirectoryEntryKind {
        DirectoryEntryKind::File(self.content.lock().await.size())
    }

    pub fn write_bytes(&self, position: u64, buf: bytes::Bytes) -> Future<()> {
        info!("Write at {}: {} bytes", position, buf.len());
        Box::pin(async move {
            let mut content_locked = self.content.lock().await;
            content_locked
                .write(position, buf, self.storage.clone())
                .await?;
            info!("Writing to file sends a change event for this file.");
            self.change_event_sender.send(()).unwrap();
            Ok(())
        })
    }

    pub fn read_bytes(&self, position: u64, count: usize) -> Future<bytes::Bytes> {
        info!("Read at {}: Up to {} bytes", position, count);
        Box::pin(async move {
            let mut content_locked = self.content.lock().await;
            content_locked
                .read(position, count, self.storage.clone())
                .await
        })
    }

    pub fn flush(&self) {
        // TODO: mark as flushed or something
        info!("Flush sends a change event");
        self.change_event_sender.send(()).unwrap();
    }

    pub async fn wait_for_next_change(
        &self,
    ) -> (
        std::result::Result<OpenFileStatus, StoreError>,
        Pin<Box<(dyn std::future::Future<Output = ()> + Send)>>,
    ) {
        let mut content_locked = self.content.lock().await;
        let maybe_content_reference = content_locked.store(self.storage.clone()).await;
        let mut receiver = self.change_event_sender.subscribe();
        let change_event_future = async move { receiver.changed().await.unwrap() };
        (
            maybe_content_reference.map(|(content_reference, size)| OpenFileStatus {
                digest: content_reference.digest,
                size: size as u64,
                // TODO
                is_writeable: true,
                // TODO
                bytes_unflushed_count: 0,
            }),
            Box::pin(change_event_future),
        )
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

    pub fn rename<'a>(&'a self, from: NormalizedPath, to: NormalizedPath) -> Future<'a, ()> {
        let opening_directory_from = match from.split_right() {
            PathSplitRightResult::Root => {
                return Box::pin(std::future::ready(Err(Error::CannotRename)))
            }
            PathSplitRightResult::Entry(directory_path, leaf_name) => {
                (self.root.open_directory(directory_path), leaf_name)
            }
        };
        let opening_directory_to = match to.split_right() {
            PathSplitRightResult::Root => {
                return Box::pin(std::future::ready(Err(Error::CannotRename)))
            }
            PathSplitRightResult::Entry(directory_path, leaf_name) => {
                (self.root.open_directory(directory_path), leaf_name)
            }
        };
        Box::pin(async move {
            let (maybe_directory_from, maybe_directory_to) =
                futures::join!(opening_directory_from.0, opening_directory_to.0);
            let directory_from = maybe_directory_from?;
            let directory_to = maybe_directory_to?;
            directory_from
                .rename(
                    &opening_directory_from.1,
                    &directory_to,
                    &opening_directory_to.1,
                )
                .await
        })
    }

    pub fn remove<'a>(&'a self, path: NormalizedPath) -> Future<'a, ()> {
        let opening_directory = match path.split_right() {
            PathSplitRightResult::Root => {
                return Box::pin(std::future::ready(Err(Error::CannotRename)))
            }
            PathSplitRightResult::Entry(directory_path, leaf_name) => {
                (self.root.open_directory(directory_path), leaf_name)
            }
        };
        return Box::pin(async move {
            let directory = opening_directory.0.await?;

            directory.remove(&opening_directory.1).await
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use astraea::storage::{InMemoryValueStorage, LoadValue, StoreValue};

    #[tokio::test]
    async fn test_open_directory_get_meta_data() {
        let expected = DirectoryEntryKind::File(12);
        let directory = OpenDirectory::new(
            tokio::sync::Mutex::new(BTreeMap::from([(
                "test.txt".to_string(),
                NamedEntry::NotOpen(expected.clone(), BlobDigest::hash(&[])),
            )])),
            Arc::new(NeverUsedStorage {}),
        );
        let meta_data = directory.get_meta_data("test.txt").await.unwrap();
        assert_eq!(expected, meta_data);
    }

    #[tokio::test]
    async fn test_open_directory_wait_for_next_change() {
        let expected = DirectoryEntryKind::File(12);
        let storage = Arc::new(InMemoryValueStorage::empty());
        let directory = OpenDirectory::new(
            tokio::sync::Mutex::new(BTreeMap::from([(
                "test.txt".to_string(),
                NamedEntry::NotOpen(expected.clone(), BlobDigest::hash(&[])),
            )])),
            storage.clone(),
        );
        let (maybe_status, _change_event_future) = directory.wait_for_next_change().await;
        assert_eq!(
            Ok(OpenDirectoryStatus::new(
                BlobDigest::new(&[
                    104, 239, 112, 74, 159, 151, 115, 53, 77, 79, 0, 61, 0, 255, 60, 199, 108, 6,
                    169, 103, 74, 159, 244, 189, 32, 88, 122, 64, 159, 105, 106, 157, 205, 186, 47,
                    210, 169, 3, 196, 19, 48, 211, 86, 202, 96, 177, 113, 146, 195, 171, 48, 102,
                    23, 244, 236, 205, 2, 38, 202, 233, 41, 2, 52, 27
                ]),
                1,
                0,
                0,
                0,
                0
            )),
            maybe_status
        );
        assert_eq!(1, storage.len());
    }

    #[tokio::test]
    async fn test_open_directory_open_file() {
        let directory = OpenDirectory::new(
            tokio::sync::Mutex::new(BTreeMap::new()),
            Arc::new(NeverUsedStorage {}),
        );
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
        let directory = OpenDirectory::new(
            tokio::sync::Mutex::new(BTreeMap::new()),
            Arc::new(NeverUsedStorage {}),
        );
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
        let directory = OpenDirectory::new(
            tokio::sync::Mutex::new(BTreeMap::new()),
            Arc::new(NeverUsedStorage {}),
        );
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
        assert_eq!(Error::NotFound("unknown.txt".to_string()), error);
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
        assert_eq!(Error::NotFound("unknown.txt".to_string()), error);
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
        assert_eq!(Error::NotFound("unknown".to_string()), error);
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
        assert_eq!(
            Some(Error::CannotOpenRegularFileAsDirectory(
                "test.txt".to_string()
            )),
            result.err()
        );
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
        assert_eq!(
            Some(Error::CannotOpenRegularFileAsDirectory(
                "test.txt".to_string()
            )),
            result.err()
        );
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
