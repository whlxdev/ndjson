using System.Collections.Concurrent;
using System.Text.Json;

namespace Ndjson;

/// <summary>
/// Represents an index that maps keys to byte offsets in an NDJSON file.
/// Used to enable fast lookup of individual records based on a unique key.
/// </summary>
/// <typeparam name="TKey">The type of the key used for indexing. Must be non-nullable.</typeparam>
internal class NdjsonIndices<TKey> where TKey : notnull
{
	private const string KeyPropertyName = "key";
	private const string OffsetPropertyName = "offset";
	private const int LargeFileThreshold = 1000;
	
	private readonly ConcurrentDictionary<TKey, long> _offsets = new();
	
	/// <summary>
	/// Gets the collection of all keys stored in the index.
	/// </summary>
	public IEnumerable<TKey> Keys => _offsets.Keys;
	
	/// <summary>
	/// Adds or updates an offset for the specified key.
	/// </summary>
	/// <param name="key">The unique key associated with an object.</param>
	/// <param name="offset">The byte offset of the object in the NDJSON file.</param>
	public void Add(TKey key, long offset)
	{
		_offsets[key] = offset;
	}
	
	/// <summary>
	/// Removes the offset entry associated with the specified key.
	/// <param name="key">Index entry to remove.</param>
	/// </summary>
	public bool Remove(TKey key)
	{
		return _offsets.Remove(key, out _);
	}

	/// <summary>
	/// Clears all entries in the index.
	/// </summary>
	public void Clear()
	{
		_offsets.Clear();
	}
	
	/// <summary>
	/// Determines whether the index contains the specified key.
	/// <param name="key">The key to check if are contained.</param>
	/// </summary>
	public bool ContainsKey(TKey key)
	{
		return _offsets.ContainsKey(key);
	}

	/// <summary>
	/// Attempts to get the byte offset associated with the specified key.
	/// </summary>
	/// <param name="key">The key to look up.</param>
	/// <param name="offset">The resulting byte offset if found.</param>
	/// <returns>True if the key was found; otherwise, false.</returns>
	public bool TryGetOffset(TKey key, out long offset)
	{
		return _offsets.TryGetValue(key, out offset);
	}
	
	internal void Save(string path)
	{
		using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 65536);
		using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = false }); // Disable indentation for smaller files

		writer.WriteStartArray();
		foreach (var pair in _offsets)
		{
			writer.WriteStartObject();
			writer.WritePropertyName(KeyPropertyName);
			JsonSerializer.Serialize(writer, pair.Key);
			writer.WriteNumber(OffsetPropertyName, pair.Value);
			writer.WriteEndObject();
		}
		writer.WriteEndArray();
	}
	
	internal void Load(string path)
	{
		_offsets.Clear();

		using var stream = File.OpenRead(path);
		using var document = JsonDocument.Parse(stream);

		var elements = document.RootElement.EnumerateArray().ToArray();
		
		// Use parallel processing for large index files
		if (elements.Length > LargeFileThreshold)
		{
			Parallel.ForEach(elements, element =>
			{
				var key = element.GetProperty(KeyPropertyName).Deserialize<TKey>()!;
				var offset = element.GetProperty(OffsetPropertyName).GetInt64();
				_offsets[key] = offset;
			});
		}
		else
		{
			// Sequential processing for smaller files
			foreach (var element in elements)
			{
				var key = element.GetProperty(KeyPropertyName).Deserialize<TKey>()!;
				var offset = element.GetProperty(OffsetPropertyName).GetInt64();
				_offsets[key] = offset;
			}
		}
	}
}