use std::{collections::HashMap, io::Write, ops::Range, path::PathBuf};

use chrono::Local;
use data_manager::{ChunkId, DataChunk, DataChunkRef, DataManager, MyDataManager};
use log::LevelFilter;
use rand::{thread_rng, Rng};

use env_logger::{self, Target};

/// Simple example of a data manager that downloads and deletes data chunks.
/// The data manager is able to find, download, delete and stop.
///
/// Data chunks are identified by a unique identifier, the `ChunkId`, and
/// contain a dataset identifier, a range of data, and a collection of file
/// names and URLs.
///
/// The `MyDataManager` struct is the implementation of the `DataManager` trait.
/// It contains a `HashMap` of the refrences to the data chunks.
///
/// The `main` function initializes the data manager and creates a loop that
/// simulates the download and deletion of data chunks. The loop generates
/// mock data chunks and downloads them to the data manager. It then randomly
/// deletes data chunks and simulates freeing the data chunks.
#[tokio::main]
async fn main() {
    env_logger::builder()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} [{}] - {}",
                Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.level(),
                record.args()
            )
        })
        .target(Target::Stdout)
        .filter_level(LevelFilter::Info)
        .init();

    let data_manager = MyDataManager::new(PathBuf::from("data"));

    let mut counter = 0;

    let mut data_chunks = HashMap::new();

    loop {
        if data_manager.stopped().await {
            break;
        }

        let mut rng = thread_rng();
        tokio::time::sleep(tokio::time::Duration::from_secs(rng.gen_range(1..10))).await;
        if counter < 100 {
            let (chunk_id, data_chunk) = mock_data_chunk();
            data_chunks.insert(chunk_id, data_chunk.clone());
            data_manager.download_chunk(data_chunk);
            counter += 1;
        }

        if rng.gen_bool(0.5) {
            if !data_chunks.is_empty() {
                let random_chunk_id = data_chunks.keys().collect::<Vec<&ChunkId>>()
                    [rng.gen_range(0..data_chunks.len())];
                let random_chunk = data_chunks.get(random_chunk_id).unwrap().clone();

                if rng.gen_bool(0.7) {
                    let data_manager = data_manager.clone();
                    tokio::spawn(async move {
                        let chunk = data_manager.find_chunk(random_chunk.dataset_id, 0);
                        if chunk.is_some() {
                            let chunk = chunk.unwrap();
                            tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
                            log::info!("⛓️‍💥 Freeing chunk {:?}", chunk.path());
                        }
                    });
                }
                let chunk = data_manager.find_chunk(random_chunk.dataset_id, 0);
                if chunk.is_some() {
                    data_manager.delete_chunk(random_chunk_id.clone());
                    data_chunks.remove(&random_chunk_id.clone());
                }
            }
        }
    }
}

fn mock_data_chunk() -> (ChunkId, DataChunk) {
    let mut files = HashMap::new();

    for i in 1..=4 {
        files.insert(
            "file_".to_owned() + &i.to_string(),
            String::from("https://sabnzbd.org/tests/internetspeed/50MB.bin"),
        );
    }
    let id: ChunkId = rand::random();

    (
        id,
        DataChunk::new(id, rand::random(), Range { start: 0, end: 10 }, files),
    )
}
