use std::collections::HashMap;
use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::runtime::Handle;
use tokio::signal;
use tokio::sync::{mpsc, RwLock};
use tokio_util::task::TaskTracker;

pub type DatasetId = [u8; 32];
pub type ChunkId = [u8; 32];

/// data chunk description
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataChunk {
    id: ChunkId,
    /// Dataset (blockchain) id
    pub dataset_id: DatasetId,
    /// Block range this chunk is responsible for (around 100 - 10000 blocks)
    block_range: Range<u64>,
    /// Data chunk files.
    /// A mapping between file names and HTTP URLs to download files from.
    /// Usually contains 1 - 10 files of various sizes.
    /// The total size of all files in the chunk is about 200 MB.
    files: HashMap<String, String>,
}

impl DataChunk {
    pub fn new(
        id: ChunkId,
        dataset_id: DatasetId,
        block_range: Range<u64>,
        files: HashMap<String, String>,
    ) -> Self {
        DataChunk {
            id,
            dataset_id,
            block_range,
            files,
        }
    }
}

pub trait DataManager: Send + Sync {
    /// Create a new `DataManager` instance, that will use `data_dir` to store the data.
    ///
    /// When `data_dir` is not empty, this method should create a list of fully downloaded chunks
    /// and use it as initial state.
    fn new(data_dir: PathBuf) -> Self;
    /// Schedule `chunk` download in background
    fn download_chunk(&self, chunk: DataChunk);
    // List chunks, that are currently available
    fn list_chunks(&self) -> Vec<ChunkId>;
    /// Find a chunk from a given dataset, that is responsible for `block_number`.
    fn find_chunk(&self, dataset_id: [u8; 32], block_number: u64) -> Option<impl DataChunkRef>;
    /// Schedule data chunk for deletion in background
    fn delete_chunk(&self, chunk_id: [u8; 32]);
}

// Data chunk must remain available and untouched till this reference is not dropped
pub trait DataChunkRef: Send + Sync + Clone {
    // Data chunk directory
    fn path(&self) -> &Path;
}

// Display implementation for DataChunk
impl std::fmt::Display for DataChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataChunk id: {:?}", hex::encode(self.id),)
    }
}

#[derive(Debug, Clone)]
struct ArcDataChunkRef {
    path_buf: PathBuf,
    chunk: Arc<DataChunk>,
}

impl ArcDataChunkRef {
    fn new(chunk: DataChunk, data_dir: PathBuf) -> Self {
        let path_buf = data_dir.join(hex::encode(chunk.id));

        ArcDataChunkRef {
            chunk: Arc::new(chunk),
            path_buf,
        }
    }

    fn get_counter(&self) -> usize {
        Arc::strong_count(&self.chunk)
    }

    // Get chunk
    // This is not used in the current implementation.
    #[allow(dead_code)]
    pub fn chunk(&self) -> Arc<DataChunk> {
        self.chunk.clone()
    }
}

impl DataChunkRef for ArcDataChunkRef {
    fn path(&self) -> &Path {
        self.path_buf.as_path()
    }
}

#[derive(Debug, PartialEq, Eq)]
enum DataManagerState {
    Initialized,
    ShuttingDown,
    Stopped,
}

#[derive(Serialize, Deserialize)]
struct DataChunkMetadata {
    chunk: DataChunk,
    size: usize,
}

const MAX_STORAGE_SIZE: usize = 1000 * 1024 * 1024 * 1024;
const ESTIMATED_CHUNK_SIZE: usize = 200 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct MyDataManager {
    data_dir: PathBuf,
    chunks: Arc<Mutex<HashMap<ChunkId, ArcDataChunkRef>>>,
    downloaders: mpsc::UnboundedSender<DataChunk>,
    deleters: mpsc::UnboundedSender<ChunkId>,
    shutdown: mpsc::UnboundedSender<()>,
    state: Arc<RwLock<DataManagerState>>,
    task_tracker: TaskTracker,
    storage_size: Arc<RwLock<usize>>,
    current_downloads: Arc<RwLock<usize>>,
}

impl MyDataManager {
    pub fn shutdown(&self) {
        self.shutdown.send(()).unwrap();
    }

    async fn download_chunk_task(&self, chunk: DataChunk) {
        log::info!("📡 Downloading chunk {:}", chunk);

        // Check if storage size is exceeded
        let storage_size = *self.storage_size.read().await;

        if (storage_size
            + ESTIMATED_CHUNK_SIZE
            + *self.current_downloads.read().await * ESTIMATED_CHUNK_SIZE)
            > MAX_STORAGE_SIZE
        {
            log::info!("🚫 Storage size exceeded");
            return;
        }

        // Set current downloads plus one
        *self.current_downloads.write().await += 1;

        let mut tasks = vec![];

        for (file_name, url) in chunk.files.iter() {
            let file_name = file_name.clone();
            let url = url.clone();
            let data_dir = self.data_dir.clone();
            let hex_id = hex::encode(chunk.id);

            fs::create_dir_all(data_dir.clone().join(&hex_id)).unwrap();

            let task = tokio::task::spawn_blocking(move || {
                let handle = Handle::current();

                let mut size = 0;

                handle.block_on(async {
                    let response = reqwest::get(&url).await.map_err(|e| e.to_string())?;

                    let mut file = fs::File::create(data_dir.join(hex_id).join(&file_name))
                        .map_err(|e| e.to_string())?;

                    let content = response.bytes().await.map_err(|e| e.to_string())?;

                    size = content.len();

                    std::io::copy(&mut content.as_ref(), &mut file).map_err(|e| e.to_string())?;

                    log::info!("💾 Downloaded file {:?} from {:?}", &file_name, &url);

                    Ok::<usize, String>(size)
                })
            });
            tasks.push(task);
        }

        let data_manager = self.clone();

        self.task_tracker.spawn(async move {
            let mut metadata = DataChunkMetadata {
                chunk: chunk.clone(),
                size: 0,
            };

            for task in tasks {
                match task.await {
                    Ok(Ok(size)) => metadata.size += size,
                    _ => {
                        log::error!("❌ Error downloading chunk {:} Retrying...", chunk);
                        data_manager.download_chunk(chunk.clone());
                        return;
                    }
                }
            }

            *data_manager.storage_size.write().await += metadata.size;

            fs::write(
                data_manager
                    .data_dir
                    .join(hex::encode(chunk.id))
                    .join("metadata"),
                serde_json::to_string(&metadata).unwrap(),
            )
            .unwrap();

            let chunk_ref = ArcDataChunkRef::new(chunk.clone(), data_manager.data_dir);
            data_manager
                .chunks
                .lock()
                .unwrap()
                .insert(chunk.id, chunk_ref);

            // Decrease current downloads by one
            *data_manager.current_downloads.write().await -= 1;

            log::info!("✅ Downloaded chunk {:}", chunk);
            log::info!(
                "📏 current storage size: {:.2} MB",
                *(data_manager.storage_size.read().await) / (1024 * 1024)
            );
        });
    }

    /// Delete chunk task
    /// This task is responsible for deleting a chunk from the data manager.
    /// It will check if the chunk is still in use, and if it is, it will wait for 5 seconds and try again.
    /// If the chunk is not in use, it will delete the chunk folder and remove the chunk from the data manager.
    async fn delete_chunk_task(&self, chunk_id: ChunkId) {
        let counter;
        let chunk;
        {
            let mut chunks = self.chunks.lock().unwrap();
            match chunks.get(&chunk_id) {
                Some(c) => {
                    chunk = c.clone();
                    counter = chunk.get_counter();
                }
                None => {
                    log::info!("❌ Chunk {:?} not found", hex::encode(chunk_id));
                    return;
                }
            }

            if counter == 2 {
                log::info!("❌ Deleted chunk {:?}", hex::encode(chunk_id));
                chunks.remove(&chunk_id);
            }
        }

        if counter == 2 {
            // Get chunk metadata
            let metadata: DataChunkMetadata = serde_json::from_str(
                fs::read_to_string(chunk.path().join("metadata"))
                    .unwrap()
                    .as_str(),
            )
            .unwrap();
            *self.storage_size.write().await -= metadata.size;
            fs::remove_dir_all(chunk.path()).unwrap();
            log::info!(
                "📏 current storage size: {:.2} MB",
                *(self.storage_size.read().await) / (1024 * 1024)
            );
            return;
        }

        if counter > 2 {
            log::info!("⌛️ Chunk {:?} is still in use", hex::encode(chunk_id));
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            self.delete_chunk(chunk_id);
            return;
        }
    }

    async fn worker(
        &self,
        downloaders_rx: &mut mpsc::UnboundedReceiver<DataChunk>,
        deleters_rx: &mut mpsc::UnboundedReceiver<ChunkId>,
        shutdown_rx: &mut mpsc::UnboundedReceiver<()>,
    ) {
        tokio::select! {
            Some(chunk) = downloaders_rx.recv() => self.download_chunk_task(chunk).await,
            Some(chunk_id) = deleters_rx.recv() => self.delete_chunk_task(chunk_id).await,
            _ = shutdown_rx.recv() => {
                log::info!("🔌 Shutting down data manager");
                downloaders_rx.close();
                deleters_rx.close();
            },
        }
    }

    pub async fn stopped(&self) -> bool {
        let state = self.state.read().await;
        *state == DataManagerState::Stopped
    }
}

impl DataManager for MyDataManager {
    fn new(data_dir: PathBuf) -> Self {
        let mut initial_chunks = HashMap::new();
        let mut initial_storage_size: usize = 0;

        if (data_dir.join("lock")).exists() {
            panic!("🔒 Data directory is already in use. If any process is using it, please stop it and remove the lock file.");
        } else {
            fs::create_dir_all(data_dir.clone()).unwrap();
            fs::File::create(data_dir.join("lock")).unwrap();
        }

        // Get all the subdirectories in the data directory
        let chunks = fs::read_dir(data_dir.clone())
            .unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .unwrap();

        for chunk in chunks {
            // Check if metadata file exists
            if !chunk.join("metadata").exists() {
                continue;
            }

            let metadata = fs::read_to_string(chunk.join("metadata")).unwrap();
            let metadata: DataChunkMetadata = serde_json::from_str(&metadata).unwrap();

            let chunk_ref = ArcDataChunkRef::new(metadata.chunk.clone(), data_dir.clone());
            initial_chunks.insert(metadata.chunk.id, chunk_ref);
            initial_storage_size += metadata.size;
        }

        let (downloaders, mut downloaders_rx) = mpsc::unbounded_channel();
        let (deleters, mut deleters_rx) = mpsc::unbounded_channel();
        let (shutdown, mut shutdown_rx) = mpsc::unbounded_channel();

        let dm = MyDataManager {
            data_dir,
            chunks: Arc::new(Mutex::new(initial_chunks)),
            downloaders,
            deleters,
            shutdown,
            state: Arc::new(RwLock::new(DataManagerState::Initialized)),
            task_tracker: TaskTracker::new(),
            storage_size: Arc::new(RwLock::new(initial_storage_size)),
            current_downloads: Arc::new(RwLock::new(0)),
        };

        let dm_clone = dm.clone();

        tokio::spawn(async move {
            signal::ctrl_c().await.unwrap();
            log::info!(" 🛑  Stopping gracefully");
            dm_clone.shutdown();
            let mut state = dm_clone.state.write().await;
            *state = DataManagerState::ShuttingDown;
        });

        let dm_clone = dm.clone();

        tokio::spawn(async move {
            loop {
                {
                    let mut state = dm_clone.state.write().await;
                    if *state == DataManagerState::ShuttingDown
                        && deleters_rx.is_empty()
                        && downloaders_rx.is_empty()
                    {
                        log::info!("🔌 Terminating ongoing tasks");

                        dm_clone.task_tracker.close();
                        dm_clone.task_tracker.wait().await;

                        print!("🛑 Stopped");

                        fs::remove_file(dm_clone.data_dir.join("lock")).unwrap();

                        *state = DataManagerState::Stopped;
                    }
                }

                dm_clone
                    .worker(&mut downloaders_rx, &mut deleters_rx, &mut shutdown_rx)
                    .await;
            }
        });

        dm
    }

    fn download_chunk(&self, chunk: DataChunk) {
        match self.downloaders.send(chunk) {
            Ok(_) => (),
            Err(err) => log::info!("{err}"),
        }
    }

    fn list_chunks(&self) -> Vec<ChunkId> {
        self.chunks.lock().unwrap().keys().cloned().collect()
    }

    fn find_chunk(&self, dataset_id: [u8; 32], block_number: u64) -> Option<impl DataChunkRef> {
        let chunks = self.chunks.lock().unwrap();
        let chunk = chunks.values().find(|chunk| {
            chunk.chunk.dataset_id == dataset_id
                && chunk.chunk.block_range.start <= block_number
                && chunk.chunk.block_range.end >= block_number
        });

        match chunk {
            Some(chunk) => Some(chunk.clone()),
            None => None,
        }
    }

    fn delete_chunk(&self, chunk_id: [u8; 32]) {
        match self.deleters.send(chunk_id) {
            Ok(_) => (),
            Err(err) => log::info!("{err}"),
        }
    }
}
