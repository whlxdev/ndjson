# Ndjson

**Ndjson** is a high-performance .NET 8 library that provides efficient read/write access to **NDJSON** (Newline-Delimited JSON) files, with **indexing support** for fast random access by key. Built with modern .NET performance optimizations including:

- **Memory pooling** with `ArrayPool<byte>` for zero-allocation buffer management
- **Direct streaming** with `Utf8JsonWriter` to eliminate intermediate string allocations
- **Intelligent buffering** with 128KB buffers and automatic overflow handling for objects of any size
- **Async/await patterns** with proper deadlock prevention
- **Parallel processing** with optimized disk seek ordering for batch operations

## Why this library?

Working with large datasets in JSON can quickly become inefficient when loading or deserializing everything into memory. While NDJSON (newline-delimited JSON) is a great format for streaming and append-only logs, **it doesn't support random access natively**.

This library solves that problem by generating and using **index files** that map each object's unique key to its byte offset in the file. This makes it possible to:

- **Retrieve a single object** without reading the entire file
- **Load millions of entries** efficiently in write-once/read-many scenarios
- **Combine structured text** with fast lookup capabilities
- **Process large datasets** with minimal memory footprint

## When to use it

Use **Ndjson** when:

- You're writing **append-only structured logs** or datasets in NDJSON format
- You want to **query or retrieve individual records by ID** without loading the entire file
- You need a **simple, high-performance disk-based store** for temporary or long-term data
- You want to **process data in batches** but still have fast access to individual items
- You're working with **large datasets** (millions of records) that need efficient random access
- You need **async/await support** for non-blocking file operations
- You're working with **objects of varying sizes** including very large objects (>100KB)

## When *not* to use it

Avoid this library if:

- You need **full-text search** or complex filtering — use a database (e.g., SQLite, PostgreSQL, etc)
- Your data **changes frequently** — this is optimized for mostly immutable datasets
- You require **ACID transactions** or complex relational queries
- You need **real-time concurrent writes** from multiple processes
- You need **in-place updates** or deletions of existing records

## Usage

### Writing data

```csharp
var writer = new NdjsonWriter<string, DataStructure>(d => d.Id);

// Synchronous write
writer.Write(data, "data.ndjson"); // Saves data.ndjson and data.index.json

// Asynchronous write (recommended for large datasets)
await writer.WriteAsync(data, "data.ndjson", cancellationToken);
```

### Reading data

#### Single object lookup
```csharp
using var reader = new NdjsonReader<string, DataStructure>(
    dataPath: "data.ndjson",
    indexPath: "data.index.json"
);

// Synchronous read
var entry = reader.ReadByKey("data-1234");

// Asynchronous read (recommended)
var entry = await reader.ReadByKeyAsync("data-1234", cancellationToken);

Console.WriteLine(entry?.Property);
```

#### Batch reads (optimized for performance)
```csharp
using var reader = new NdjsonReader<string, DataStructure>("data.ndjson");

var keys = new[] { "data-1", "data-2", "data-3" };

// Parallel batch read with automatic disk seek optimization
var results = await reader.ReadByKeysAsync(keys, maxParallelism: 4);

foreach (var (key, value) in results)
{
    Console.WriteLine($"{key}: {value.Property}");
}
```

#### Dynamic index building
```csharp
// Build index on-the-fly without separate index file
using var reader = new NdjsonReader<string, DataStructure>(
    dataPath: "data.ndjson",
    indicesBuilder: obj => obj.Id
);

var entry = await reader.ReadByKeyAsync("data-1234");
```

## File Structure

- **`data.ndjson`**: Your serialized objects, one per line, optimized for streaming
- **`data.index.json`**: Separate index file mapping each key to byte offset in the data file
- **Automatic naming**: Index files are automatically named (e.g., `data.index.json` for `data.ndjson`)

## Performance Features

### Memory Efficiency
- **Zero-allocation streaming**: Uses `Utf8JsonWriter` with `ArrayPool<byte>` for buffer management
- **Intelligent buffering**: 128KB buffers with automatic overflow to direct stream writing
- **Memory pooling**: Shared buffer pools reduce garbage collection pressure

### I/O Optimization
- **Batch processing**: Optimized disk seek patterns for multi-key reads
- **Configurable parallelism**: Control concurrent operations based on your system
- **Large object support**: Handles objects of any size without buffer overflow errors

### Async/Sync Compatibility
- **Unified implementation**: Single codebase handles both sync and async operations
- **Deadlock prevention**: Proper async patterns prevent synchronization context issues
- **Cancellation support**: Full `CancellationToken` support throughout the API

## Thread Safety

- **Readers**: Thread-safe for concurrent read operations
- **Writers**: Not thread-safe; use one writer instance per thread
- **File Access**: Uses appropriate file sharing modes for safe concurrent reads
- **Semaphore limiting**: Built-in concurrency control prevents resource exhaustion
