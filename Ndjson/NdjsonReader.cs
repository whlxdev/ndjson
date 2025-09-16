using System.Collections.Concurrent;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Ndjson;

/// <summary>
/// Provides functionality to read objects from an NDJSON file using a precomputed index,
/// allowing fast random access by key, with support for batching and async access.
/// </summary>
/// <typeparam name="TKey">The type of the key used for indexing. Must be non-nullable.</typeparam>
/// <typeparam name="TObject">The type of the object to deserialize.</typeparam>
public class NdjsonReader<TKey, TObject> : IDisposable where TKey : notnull
{
	private readonly int _maxConcurrentReads = Environment.ProcessorCount * 2;

	private readonly string _dataPath;
	private readonly JsonSerializerOptions _jsonOptions;
	private readonly NdjsonIndices<TKey> _indices;
	private readonly SemaphoreSlim _readSemaphore;

	/// <summary>
	/// Gets the collection of all available keys in the loaded index.
	/// </summary>
	public IEnumerable<TKey> Indices => _indices.Keys;

	/// <summary>
	/// Initializes a new instance of the <see cref="NdjsonReader{TKey, TObject}"/> class using indices file.
	/// </summary>
	/// <param name="dataPath">The path to the NDJSON file.</param>
	/// <param name="indexPath">The path to the index file.</param>
	/// <param name="jsonOptions">Optional JSON serializer options. Defaults to system settings.</param>
	public NdjsonReader(string dataPath, string indexPath, JsonSerializerOptions? jsonOptions = null)
		: this(dataPath, jsonOptions, indexPath, null)
	{
	}

	/// <summary>
	/// Initializes a new instance of the <see cref="NdjsonReader{TKey, TObject}"/> class using only the data path.
	/// </summary>
	/// <param name="dataPath">The path to the NDJSON file.</param>
	/// <param name="jsonOptions">Optional JSON serializer options. Defaults to system settings.</param>
	/// <exception cref="FileNotFoundException">Thrown if index file cannot be found.</exception>
	public NdjsonReader(string dataPath, JsonSerializerOptions? jsonOptions = null)
		: this(dataPath, jsonOptions, $"{Path.ChangeExtension(dataPath, null)}.index.json", null)
	{
	}
		
	/// <summary>
	/// Initializes a new instance of the <see cref="NdjsonReader{TKey, TObject}"/> class using index building function.
	/// </summary>
	/// <param name="dataPath">The path to the NDJSON file.</param>
	/// <param name="indicesBuilder">Construct index by object.</param>
	/// <param name="jsonOptions">Optional JSON serializer options. Defaults to system settings.</param>
	public NdjsonReader(string dataPath, Func<TObject, TKey> indicesBuilder, JsonSerializerOptions? jsonOptions = null)
		: this(dataPath, jsonOptions, null, indicesBuilder)
	{
	}
	
	private NdjsonReader(string dataPath, JsonSerializerOptions? jsonOptions, string? indexPath, Func<TObject, TKey>? indicesBuilder)
	{
		if (string.IsNullOrWhiteSpace(dataPath))
		{
			throw new ArgumentException("Data path cannot be null or empty.", nameof(dataPath));
		}

		if (indexPath != null)
		{
			if (string.IsNullOrWhiteSpace(indexPath))
			{
				throw new ArgumentException("Index path cannot be null or empty.", nameof(indexPath));
			}
			
			if (Path.GetFullPath(dataPath).Equals(Path.GetFullPath(indexPath)))
			{
				throw new ArgumentException("Data path and index path cannot be the same file.");
			}
		}

		if (indicesBuilder != null && indexPath != null)
		{
			throw new ArgumentException("Cannot specify both indexPath and indicesBuilder.");
		}

		// Now validate file existence
		if (!File.Exists(dataPath))
		{
			throw new FileNotFoundException("Data file not found.", dataPath);
		}

		if (indexPath != null && !File.Exists(indexPath))
		{
			throw new FileNotFoundException("Index file not found.", indexPath);
		}

		_dataPath = dataPath;
		_readSemaphore = new SemaphoreSlim(_maxConcurrentReads, _maxConcurrentReads);
		
		// Create a copy of JsonSerializerOptions to avoid modifying a read-only instance
		if (jsonOptions == null)
		{
			_jsonOptions = new JsonSerializerOptions
			{
				Encoder = JavaScriptEncoder.Default
			};
		}
		else
		{
			_jsonOptions = new JsonSerializerOptions(jsonOptions)
			{
				Encoder = JavaScriptEncoder.Default
			};
		}
		
		_indices = new NdjsonIndices<TKey>();

		// Load or build indices based on parameters
		if (indexPath != null)
		{
			_indices.Load(indexPath);
		}
		else if (indicesBuilder != null)
		{
			BuildIndicesFromFile(_dataPath, indicesBuilder, _jsonOptions, _indices);
		}
	}

	/// <summary>
	/// Reads and deserializes an object from the NDJSON file using the specified key.
	/// </summary>
	public TObject? ReadByKey(TKey key)
	{
		return ReadByKeyCore(key, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
	}
	
	/// <summary>
	/// Reads multiple objects by a list of keys using parallel processing for optimal performance.
	/// </summary>
	/// <param name="keys">The keys to read.</param>
	/// <param name="maxParallelism">Maximum number of parallel operations. Defaults to processor count.</param>
	/// <returns>Dictionary of successfully read objects keyed by their original keys.</returns>
	public Dictionary<TKey, TObject> ReadByKeys(IEnumerable<TKey> keys, int? maxParallelism = null)
	{
		return ReadByKeysCore(keys, maxParallelism, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
	}

	/// <summary>
	/// Asynchronously reads and deserializes an object from the NDJSON file using the specified key.
	/// </summary>
	public async Task<TObject?> ReadByKeyAsync(TKey key, CancellationToken cancellationToken = default)
	{
		return await ReadByKeyCore(key, isAsync: true, cancellationToken);
	}

	/// <summary>
	/// Asynchronously reads multiple objects by a list of keys using parallel processing for optimal performance.
	/// </summary>
	/// <param name="keys">The keys to read.</param>
	/// <param name="maxParallelism">Maximum number of parallel operations. Defaults to processor count.</param>
	/// <param name="cancellationToken">Cancellation token.</param>
	/// <returns>Dictionary of successfully read objects keyed by their original keys.</returns>
	public async Task<Dictionary<TKey, TObject>> ReadByKeysAsync(IEnumerable<TKey> keys, int? maxParallelism = null, CancellationToken cancellationToken = default)
	{
		return await ReadByKeysCore(keys, maxParallelism, isAsync: true, cancellationToken);
	}
	
	private async Task<TObject?> ReadByKeyCore(TKey key, bool isAsync, CancellationToken cancellationToken)
	{
		if (!_indices.TryGetOffset(key, out var offset))
		{
			return default;
		}

		if (isAsync)
		{
			await _readSemaphore.WaitAsync(cancellationToken);
		}
		else
		{
			_readSemaphore.Wait(cancellationToken);
		}
		
		try
		{
			return await ReadLineAtOffsetCore(offset, isAsync, cancellationToken);
		}
		finally
		{
			_readSemaphore.Release();
		}
	}

	private async Task<Dictionary<TKey, TObject>> ReadByKeysCore(IEnumerable<TKey> keys, int? maxParallelism, bool isAsync, CancellationToken cancellationToken)
	{
		var keysList = keys.ToList();
		if (keysList.Count == 0)
		{
			return new Dictionary<TKey, TObject>();
		}

		var result = new ConcurrentDictionary<TKey, TObject>();

		// Get available entries and order by offset for disk seek optimization
		var available = keysList
			.Select(k => (Key: k, Found: _indices.TryGetOffset(k, out var o), Offset: o))
			.Where(k => k.Found)
			.OrderBy(k => k.Offset) 
			.ToList();

		if (isAsync)
		{
			// Async version using Task.WhenAll with SemaphoreSlim
			await ProcessEntriesAsync(available, maxParallelism ?? Environment.ProcessorCount, result, cancellationToken);
		}
		else
		{
			// Sync version using Parallel.ForEach with built-in parallelism control
			// No need for additional semaphore as Parallel.ForEach handles this internally
			Parallel.ForEach(available, new ParallelOptions 
			{ 
				MaxDegreeOfParallelism = maxParallelism ?? Environment.ProcessorCount,
				CancellationToken = cancellationToken
			}, entry =>
			{
				_readSemaphore.Wait(cancellationToken);
				try
				{
					var obj = ReadLineAtOffsetCore(entry.Offset, isAsync: false, cancellationToken).GetAwaiter().GetResult();
					if (obj != null)
					{
						result[entry.Key] = obj;
					}
				}
				finally
				{
					_readSemaphore.Release();
				}
			});
		}

		return new Dictionary<TKey, TObject>(result);
	}

	private async Task ProcessEntriesAsync(List<(TKey Key, bool Found, long Offset)> available, int maxParallelism, ConcurrentDictionary<TKey, TObject> result, CancellationToken cancellationToken)
	{
		using var semaphore = new SemaphoreSlim(maxParallelism);
		var tasks = available.Select(async entry =>
		{
			await semaphore.WaitAsync(cancellationToken);
			try
			{
				await _readSemaphore.WaitAsync(cancellationToken);
				try
				{
					var obj = await ReadLineAtOffsetCore(entry.Offset, isAsync: true, cancellationToken);
					if (obj != null)
					{
						result[entry.Key] = obj;
					}
				}
				finally
				{
					_readSemaphore.Release();
				}
			}
			finally
			{
				semaphore.Release();
			}
		});

		await Task.WhenAll(tasks);
	}

	private async Task<TObject?> ReadLineAtOffsetCore(long offset, bool isAsync, CancellationToken cancellationToken)
	{
		// Use a separate FileStream to avoid buffering conflicts
		var fileStream = new FileStream(_dataPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: isAsync);
		
		try
		{
			fileStream.Seek(offset, SeekOrigin.Begin);
			
			using var reader = new StreamReader(fileStream, System.Text.Encoding.UTF8, detectEncodingFromByteOrderMarks: false, bufferSize: 1024, leaveOpen: false);
			
			string? line;
			if (isAsync)
			{
				line = await reader.ReadLineAsync(cancellationToken);
			}
			else
			{
				line = reader.ReadLine();
			}
			
			if (string.IsNullOrWhiteSpace(line))
			{
				return default;
			}
			
			return JsonSerializer.Deserialize<TObject>(line, _jsonOptions);
		}
		finally
		{
			if (isAsync)
			{
				await fileStream.DisposeAsync();
			}
			else
			{
				fileStream.Dispose();
			}
		}
	}
	
	public void Dispose()
	{
		_readSemaphore.Dispose();
	}
	
	private static void BuildIndicesFromFile(string dataPath, Func<TObject, TKey> indicesBuilder, 
		JsonSerializerOptions jsonOptions, NdjsonIndices<TKey>? indices = null)
	{
		if (string.IsNullOrWhiteSpace(dataPath))
		{
			throw new ArgumentException("Data path cannot be null or empty.", nameof(dataPath));
		}
		
		if (indicesBuilder == null)
		{
			throw new ArgumentNullException(nameof(indicesBuilder));
		}
		
		if (jsonOptions == null)
		{
			throw new ArgumentNullException(nameof(jsonOptions));
		}
		
		if (!File.Exists(dataPath))
		{
			throw new FileNotFoundException("Data file not found.", dataPath);
		}

		// Use provided indices or create new one
		var targetIndices = indices ?? new NdjsonIndices<TKey>();

		using var indexBuildStream = new FileStream(dataPath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 65536);
		long currentOffset = 0;

		while (true)
		{
			// Store the offset at the START of the line
			var lineStartOffset = currentOffset;
			
			// Read line manually to track exact byte positions
			var lineBytes = new List<byte>();
			int currentByte;
			
			while ((currentByte = indexBuildStream.ReadByte()) != -1)
			{
				currentOffset++; // Track every byte read
				
				if (currentByte == '\n')
				{
					break;
				}
				
				if (currentByte != '\r') // Skip carriage return
				{
					lineBytes.Add((byte)currentByte);
				}
			}
			
			// If no bytes read, we've reached EOF
			if (lineBytes.Count == 0 && currentByte == -1)
			{
				break;
			}

			if (lineBytes.Count <= 0)
			{
				continue;
			}
			
			var line = System.Text.Encoding.UTF8.GetString(lineBytes.ToArray());
			if (string.IsNullOrWhiteSpace(line))
			{
				continue;
			}
			
			var obj = JsonSerializer.Deserialize<TObject>(line, jsonOptions);
			if (obj == null)
			{
				continue;
			}
						
			var key = indicesBuilder(obj);
			targetIndices.Add(key, lineStartOffset);
		}
	}
}
