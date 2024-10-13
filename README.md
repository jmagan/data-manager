# Data manager

This is a data manager for downloading and managing small chunks of data. It is designed to perform distributed queries on a large dataset, and download only the data that is needed for the query.

## 📝 TO-DO

### 1. Project Setup

- [X] Initialize Rust project and add dependencies (`tokio`, `hex`, etc.).

### 2. Define Data Structures

- [X] Implement `DataChunk`.
- [X] Implement `DataChunkRef` with `DataChunkHandle`.

### 3. Build DataManager

- [X] Create `MyDataManager` struct.
- [X] Implement `DataManager` trait methods:
  - [X] `new`: Initialize from `data_dir` and create lock file.
  - [X] `download_chunk`: Queue chunk for download.
  - [X] `list_chunks`: Return fully downloaded chunks.
  - [X] `find_chunk`: Find chunk for `block_number`.
  - [X] `delete_chunk`: Queue chunk for deletion.
    - [X] Use DataChunkRef for avoiding deletion while the chunk is referenced

### 4. Handle Async Tasks

- [X] Implement background task worker for downloading and deleting chunks.

### 5. File Operations

- [X] Implement async file downloading.
- [X] Implement chunk file deletion.

### 6. Create Lock File

- [X] Implement shutdown procedure
- [X] Implement a lock file mechanism to ensure `data_dir` is used by only one `DataManager`.
- [X] Check for existing lock file on startup and handle conflicts.

### 7. Initialize with downloaded chunks

- [X] Implement `DataChunkMetadata`
- [X] Create and save `DataChunkMetadata`
- [X] Load all chunks in case the folder is not empty

### 8. Set storage limite

- [X] Track size of the data folder
- [X] Check size limit before download 

### 9. Error Handling

- [X] Handle download errors and partial downloads.
- [X] Manage errors in `find_chunk` and deletions.
- [X] Handle channel clousure
- [X] Handle errors in Mutex to avoid poisoning

### 10. Documentation

- [ ] Document structs, traits, and methods.
- [ ] Provide usage examples.

## This solution

The main requirement was to avoid blocking the main thread during file downloads or deletions. To achieve this, I used green threads for lightweight tasks and `spawn_blocking` threads for intensive operations. Communication between the main thread and other threads is handled using channels.

The data manager uses Tokio to manage asynchronous operations and `Mutex` with dedicated threads to handle concurrent access to the data manager's state.

Additionally, the data manager includes a lock file mechanism to ensure that only one instance of the data manager is using the data directory at any given time. It also has a method to check the size of the data directory and stop downloading more data if the total size reaches a certain limit (1 TB).

The data chunks are encapsulated within the `impl DataChunkRef` trait. I created `ArcDataChunkRef`, which tracks the references to each data chunk. This ensures that a data chunk can only be deleted if there are no active references to it. If a reference to a data chunk exists, the data manager will wait for it to be dropped before deleting the data chunk. During the download process, the data manager ensures that only fully downloaded data chunks are included in its state.

The solution also implements a graceful shutdown procedure that waits for all tasks to finish before closing the data manager. I created a data chunk metadata structure to store information about downloaded chunks, saving this metadata in a file within the data directory. This allows the data manager to recover after a graceful shutdown by using the metadata stored in the `data` folder.

I also implemented error handling for download failures, file deletion errors, and potential issues with the `Mutex` to prevent poisoning and ensure smooth execution.


## How to run

To run the project, you need to have Rust installed. You can run the project using the following command:

```bash
cargo run
```

This will run the tests and print the results to the console.

## Demo

The main function simulate random download and deletion operations. Each operation is printed to the console, showing the current state of the data manager. The mock data generate a data chunk with 4 files of 50 MB each. The data manager has a storage limit  of 1 GB (only 5 data chunks), and it will stop downloading data when the limit is reached.

This emojis are used to represent the state of the data chunks:

> <center>
> 📡 - Starting a download. <br>
> 💾 - File downloaded. <br>
> ✅ - Fully downloaded data chunk. <br>
> ❌ - Data chunk deleted. <br>
> 🚫 - Storage exceeded. <br>
> ⌛️ - Waiting for references to drop. <br>
> 📏 - Current state storage size. <br>
> ⛓️‍💥 - Dummy reference dropped. <br>
> 🛑 - Shutdown. <br>
> </center>

<br>

This video shows how the data manager works asynchronusly and handles multiple operations at the same time. The data manager is able to download and delete data chunks while keeping track of the storage size and references to the data chunks. The video also shows the graceful shutdown procedure, where the data manager waits for all tasks to finish before closing.

<br>

https://github.com/user-attachments/assets/3e7a9f46-e51a-4630-9906-e60974197ec5