using System.Buffers;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Ndjson;

/// <summary>
/// Provides functionality to write a collection of objects to a NDJSON file,
/// while generating an index file that maps keys to byte offsets.
/// </summary>
/// <typeparam name="TKey">The type used as key in the index. Must be non-nullable.</typeparam>
/// <typeparam name="TObject">The type of the object to serialize.</typeparam>
public class NdjsonWriter<TKey, TObject> where TKey : notnull
{
	private const int DefaultBufferSize = 128 * 1024; // 128KB buffer for optimal I/O
	
	private readonly Func<TObject, TKey> _keySelector;
	private readonly JsonSerializerOptions _jsonOptions;
	private readonly byte[] _newlineBytes = "\n"u8.ToArray();

	/// <summary>
	/// Initializes a new instance of the <see cref="NdjsonWriter{TKey, TObject}"/> class.
	/// </summary>
	/// <param name="keySelector">Function to extract the key from each object.</param>
	/// <param name="jsonOptions">Optional JSON serializer options. Defaults to non-indented output.</param>
	public NdjsonWriter(Func<TObject, TKey> keySelector, JsonSerializerOptions? jsonOptions = null)
	{
		_keySelector = keySelector;
		_jsonOptions = jsonOptions ?? new JsonSerializerOptions
		{
			WriteIndented = false,
			Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping // Faster encoding for performance
		};
	}

	/// <summary>
	/// Writes the given collection of objects to the specified NDJSON file, and generates an index file.
	/// </summary>
	/// <param name="objects">The objects to serialize.</param>
	/// <param name="dataPath">The path to the NDJSON output file.</param>
	/// <param name="indexPath">The path to the index file.</param>
	/// <exception cref="ArgumentException">Thrown if dataPath and indexPath refer to the same file.</exception>
	public void Write(IEnumerable<TObject> objects, string dataPath, string indexPath)
	{
		WriteCore(objects, dataPath, indexPath, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
	}

	/// <summary>
	/// Writes the given objects to an NDJSON file and generates an index file in the same directory.
	/// </summary>
	/// <param name="objects">The objects to serialize.</param>
	/// <param name="dataPath">The path to the NDJSON file.</param>
	public void Write(IEnumerable<TObject> objects, string dataPath)
	{
		Write(objects, dataPath, Path.ChangeExtension(dataPath, ".index.json"));
	}

	/// <summary>
	/// Asynchronously writes the given collection of objects to the specified NDJSON file, and generates an index file.
	/// </summary>
	/// <param name="objects">The objects to serialize.</param>
	/// <param name="dataPath">The path to the NDJSON output file.</param>
	/// <param name="indexPath">The path to the index file.</param>
	/// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
	/// <exception cref="ArgumentException">Thrown if dataPath and indexPath refer to the same file.</exception>
	public async Task WriteAsync(IEnumerable<TObject> objects, string dataPath, string indexPath, CancellationToken cancellationToken = default)
	{
		await WriteCore(objects, dataPath, indexPath, isAsync: true, cancellationToken);
	}

	/// <summary>
	/// Asynchronously writes the given objects to an NDJSON file and generates an index file in the same directory.
	/// </summary>
	/// <param name="objects">The objects to serialize.</param>
	/// <param name="dataPath">The path to the NDJSON file.</param>
	/// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
	public async Task WriteAsync(IEnumerable<TObject> objects, string dataPath, CancellationToken cancellationToken = default)
	{
		await WriteAsync(objects, dataPath, Path.ChangeExtension(dataPath, ".index.json"), cancellationToken);
	}
	
	private async Task WriteCore(IEnumerable<TObject> objects, string dataPath, string indexPath, bool isAsync, CancellationToken cancellationToken)
	{
		if (Path.GetFullPath(dataPath) == Path.GetFullPath(indexPath))
		{
			throw new ArgumentException("Data path and index path cannot be the same file.");
		}

		var index = new NdjsonIndices<TKey>();
		
		// Use a larger buffer and rent from ArrayPool for better memory management
		var bufferOwner = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);
		try
		{
			var buffer = bufferOwner.AsMemory(0, DefaultBufferSize);

			// Create FileStream with appropriate async flag
			using var fileStream = new FileStream(dataPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: isAsync);
			
			long totalOffset = 0;
			var bufferPosition = 0;
			foreach (var obj in objects)
			{
				cancellationToken.ThrowIfCancellationRequested();
				
				var key = _keySelector(obj);
				index.Add(key, totalOffset);
				
				// Serialize the object first to determine its size
				var jsonBytes = SerializeObject(obj);
				var totalBytesNeeded = jsonBytes.Length + _newlineBytes.Length;
				
				// Check if object + newline fits in remaining buffer space
				if (bufferPosition + totalBytesNeeded <= buffer.Length)
				{
					// Object fits in buffer - write JSON + newline to buffer
					jsonBytes.Span.CopyTo(buffer.Span[bufferPosition..]);
					bufferPosition += jsonBytes.Length;
					
					_newlineBytes.CopyTo(buffer.Span[bufferPosition..]);
					bufferPosition += _newlineBytes.Length;
				}
				else
				{
					// Object doesn't fit in buffer
					// First, flush any existing buffer content
					if (bufferPosition > 0)
					{
						if (isAsync)
						{
							await fileStream.WriteAsync(buffer[..bufferPosition], cancellationToken);
						}
						else
						{
							fileStream.Write(buffer.Span[..bufferPosition]);
						}
						bufferPosition = 0;
					}
					
					// Now write the object + newline directly to stream
					if (isAsync)
					{
						await fileStream.WriteAsync(jsonBytes, cancellationToken);
						await fileStream.WriteAsync(_newlineBytes, cancellationToken);
					}
					else
					{
						fileStream.Write(jsonBytes.Span);
						fileStream.Write(_newlineBytes);
					}
				}
				
				totalOffset += jsonBytes.Length + _newlineBytes.Length;
				
				// Flush buffer when it's getting full (leave some space for next object)
				if (bufferPosition > DefaultBufferSize - 8192) // 8KB safety margin
				{
					if (isAsync)
					{
						await fileStream.WriteAsync(buffer[..bufferPosition], cancellationToken);
					}
					else
					{
						fileStream.Write(buffer.Span[..bufferPosition]);
					}
					bufferPosition = 0;
				}
			}
			
			// Write any remaining data in buffer
			if (bufferPosition > 0)
			{
				if (isAsync)
				{
					await fileStream.WriteAsync(buffer[..bufferPosition], cancellationToken);
				}
				else
				{
					fileStream.Write(buffer.Span[..bufferPosition]);
				}
			}
			
			// Ensure all data is written to disk
			if (isAsync)
			{
				await fileStream.FlushAsync(cancellationToken);
			}
			else
			{
				fileStream.Flush();
			}
		}
		finally
		{
			ArrayPool<byte>.Shared.Return(bufferOwner);
		}
		
		index.Save(indexPath);
	}
	
	private ReadOnlyMemory<byte> SerializeObject(TObject obj)
	{
		var bufferWriter = new ArrayBufferWriter<byte>();
		using var writer = new Utf8JsonWriter(bufferWriter, new JsonWriterOptions 
		{ 
			Indented = false,
			Encoder = _jsonOptions.Encoder
		});
		
		JsonSerializer.Serialize(writer, obj, _jsonOptions);
		writer.Flush();
		
		return bufferWriter.WrittenMemory;
	}
}
