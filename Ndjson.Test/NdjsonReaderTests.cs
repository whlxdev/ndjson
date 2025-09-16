using System.Text.Json;

namespace Ndjson.Test;

public class NdjsonReaderTests : IDisposable
{
    private readonly string _tempDir = TestUtilities.CreateTempDirectory();

    public void Dispose()
    {
        TestUtilities.CleanupTempDirectory(_tempDir);
    }

    private (string dataPath, string indexPath) CreateTestFiles<TKey, TObject>(
        List<TObject> testData, 
        Func<TObject, TKey> keySelector, 
        string baseName) where TKey : notnull
    {
        var writer = new NdjsonWriter<TKey, TObject>(keySelector);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, baseName);
        writer.Write(testData, dataPath, indexPath);
        return (dataPath, indexPath);
    }

    [Fact]
    public void Constructor_WithValidPaths_InitializesCorrectly()
    {
        var testData = TestUtilities.GenerateSimpleTestData(5);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "constructor_test");

        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);

        Assert.NotNull(reader);
        Assert.Equal(testData.Count, reader.Indices.Count());
    }

    [Fact]
    public void Constructor_WithDataPathOnly_LoadsIndexAutomatically()
    {
        var testData = TestUtilities.GenerateSimpleTestData(5);
        var (dataPath, _) = CreateTestFiles(testData, x => x.Id, "auto_index_test");

        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath);

        Assert.NotNull(reader);
        Assert.Equal(testData.Count, reader.Indices.Count());
    }

    [Fact]
    public void Constructor_WithIndexBuilder_BuildsIndexFromData()
    {
        var testData = TestUtilities.GenerateSimpleTestData(8);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var dataPath = Path.Combine(_tempDir, "index_builder.ndjson");
        
        using (var stream = new FileStream(dataPath, FileMode.Create, FileAccess.Write))
        using (var bufferedStream = new BufferedStream(stream))
        {
            var newlineBytes = System.Text.Encoding.UTF8.GetBytes(Environment.NewLine);
            foreach (var item in testData)
            {
                var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(item);
                bufferedStream.Write(jsonBytes);
                bufferedStream.Write(newlineBytes);
            }
        }

        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, x => x.Id);

        Assert.NotNull(reader);
        Assert.Equal(testData.Count, reader.Indices.Count());
        
        var expectedKeys = testData.Select(x => x.Id).ToHashSet();
        var actualKeys = reader.Indices.ToHashSet();
        Assert.Equal(expectedKeys, actualKeys);
    }

    [Fact]
    public void Constructor_WithNullDataPath_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => 
            new NdjsonReader<string, SimpleTestModel>(null!, "index.json"));
    }

    [Fact]
    public void Constructor_WithNonExistentDataFile_ThrowsFileNotFoundException()
    {
        var nonExistentPath = Path.Combine(_tempDir, "nonexistent.ndjson");
        var indexPath = Path.Combine(_tempDir, "index.json");

        Assert.Throws<FileNotFoundException>(() => 
            new NdjsonReader<string, SimpleTestModel>(nonExistentPath, indexPath));
    }

    [Fact]
    public void Constructor_WithSameDataAndIndexPath_ThrowsArgumentException()
    {
        var samePath = Path.Combine(_tempDir, "same.json");

        Assert.Throws<ArgumentException>(() => 
            new NdjsonReader<string, SimpleTestModel>(samePath, samePath));
    }

    [Fact]
    public void ReadByKey_ExistingKey_ReturnsCorrectObject()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(10);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "read_by_key");
        var targetItem = testData[5];

        // Act
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        var result = reader.ReadByKey(targetItem.Id);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(targetItem.Id, result.Id);
        Assert.Equal(targetItem.Name, result.Name);
        Assert.Equal(targetItem.Value, result.Value);
    }

    [Fact]
    public void ReadByKey_NonExistentKey_ReturnsNull()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(5);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "read_nonexistent");

        // Act
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        var result = reader.ReadByKey("nonexistent-key");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task ReadByKeyAsync_ExistingKey_ReturnsCorrectObject()
    {
        // Arrange
        var testData = TestUtilities.GenerateComplexTestData(8);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "read_by_key_async");
        var targetItem = testData[3];

        // Act
        using var reader = new NdjsonReader<string, ComplexTestModel>(dataPath, indexPath);
        var result = await reader.ReadByKeyAsync(targetItem.Id);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(targetItem.Id, result.Id);
        Assert.Equal(targetItem.Metadata.Category, result.Metadata.Category);
        Assert.Equal(targetItem.Tags.Length, result.Tags.Length);
    }

    [Fact]
    public async Task ReadByKeyAsync_WithCancellation_CanBeCancelled()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(5);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "read_cancelled");
        
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => 
            reader.ReadByKeyAsync(testData[0].Id, cts.Token));
    }

    [Fact]
    public void ReadByKeys_MultipleExistingKeys_ReturnsCorrectObjects()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(15);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "read_by_keys");
        var targetKeys = new[] { testData[2].Id, testData[7].Id, testData[12].Id };

        // Act
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        var results = reader.ReadByKeys(targetKeys);

        // Assert
        Assert.Equal(3, results.Count);
        foreach (var key in targetKeys)
        {
            Assert.True(results.ContainsKey(key));
            var original = testData.First(x => x.Id == key);
            var result = results[key];
            Assert.Equal(original.Name, result.Name);
            Assert.Equal(original.Value, result.Value);
        }
    }

    [Fact]
    public void ReadByKeys_MixedExistingAndNonExistentKeys_ReturnsOnlyExisting()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(5);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "read_mixed_keys");
        var keys = new[] { testData[1].Id, "nonexistent-1", testData[3].Id, "nonexistent-2" };

        // Act
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        var results = reader.ReadByKeys(keys);

        // Assert
        Assert.Equal(2, results.Count);
        Assert.True(results.ContainsKey(testData[1].Id));
        Assert.True(results.ContainsKey(testData[3].Id));
        Assert.False(results.ContainsKey("nonexistent-1"));
        Assert.False(results.ContainsKey("nonexistent-2"));
    }

    [Fact]
    public void ReadByKeys_EmptyKeyList_ReturnsEmptyDictionary()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(5);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "read_empty_keys");

        // Act
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        var results = reader.ReadByKeys(new string[0]);

        // Assert
        Assert.Empty(results);
    }

    [Fact]
    public async Task ReadByKeysAsync_MultipleKeys_ReturnsCorrectObjects()
    {
        // Arrange
        var testData = TestUtilities.GenerateNumericKeyTestData(12);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.NumericId, "read_by_keys_async");
        var targetKeys = new[] { testData[1].NumericId, testData[5].NumericId, testData[9].NumericId };

        // Act
        using var reader = new NdjsonReader<int, NumericKeyModel>(dataPath, indexPath);
        var results = await reader.ReadByKeysAsync(targetKeys);

        // Assert
        Assert.Equal(3, results.Count);
        foreach (var key in targetKeys)
        {
            Assert.True(results.ContainsKey(key));
            var original = testData.First(x => x.NumericId == key);
            var result = results[key];
            Assert.Equal(original.Description, result.Description);
        }
    }

    [Fact]
    public void ReadByKeys_WithCustomMaxParallelism_WorksCorrectly()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(20);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "read_custom_parallelism");
        var allKeys = testData.Select(x => x.Id).ToArray();

        // Act
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        var results = reader.ReadByKeys(allKeys, maxParallelism: 2);

        // Assert
        Assert.Equal(testData.Count, results.Count);
        foreach (var original in testData)
        {
            Assert.True(results.ContainsKey(original.Id));
            var result = results[original.Id];
            Assert.Equal(original.Name, result.Name);
        }
    }

    [Fact]
    public void ReadByKey_LargeDataset_PerformsWell()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(1000);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "large_read_performance");
        var randomKey = testData[500].Id;

        // Act
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var result = reader.ReadByKey(randomKey);
        stopwatch.Stop();

        // Assert
        Assert.NotNull(result);
        Assert.True(stopwatch.ElapsedMilliseconds < 100, $"ReadByKey took {stopwatch.ElapsedMilliseconds}ms, expected < 100ms");
    }

    [Fact]
    public void ReadByKeys_LargeDataset_PerformsWell()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(1000);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "large_batch_read_performance");
        var randomKeys = testData.Take(100).Select(x => x.Id).ToArray();

        // Act
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var results = reader.ReadByKeys(randomKeys);
        stopwatch.Stop();

        // Assert
        Assert.Equal(100, results.Count);
        Assert.True(stopwatch.ElapsedMilliseconds < 1000, $"ReadByKeys took {stopwatch.ElapsedMilliseconds}ms, expected < 1000ms");
    }

    [Fact]
    public async Task ReadByKeysAsync_LargeDataset_PerformsWell()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(1000);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "large_async_batch_performance");
        var randomKeys = testData.Take(100).Select(x => x.Id).ToArray();

        // Act
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var results = await reader.ReadByKeysAsync(randomKeys);
        stopwatch.Stop();

        // Assert
        Assert.Equal(100, results.Count);
        Assert.True(stopwatch.ElapsedMilliseconds < 1000, $"ReadByKeysAsync took {stopwatch.ElapsedMilliseconds}ms, expected < 1000ms");
    }

    [Fact]
    public void ReadByKey_ComplexObjects_DeserializesCorrectly()
    {
        // Arrange
        var testData = TestUtilities.GenerateComplexTestData(10);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "complex_objects");
        var targetItem = testData[4];

        // Act
        using var reader = new NdjsonReader<string, ComplexTestModel>(dataPath, indexPath);
        var result = reader.ReadByKey(targetItem.Id);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(targetItem.Id, result.Id);
        Assert.Equal(targetItem.Metadata.Category, result.Metadata.Category);
        Assert.Equal(targetItem.Metadata.Priority, result.Metadata.Priority);
        Assert.Equal(targetItem.Metadata.Active, result.Metadata.Active);
        Assert.Equal(targetItem.Tags.Length, result.Tags.Length);
        for (var i = 0; i < targetItem.Tags.Length; i++)
        {
            Assert.Equal(targetItem.Tags[i], result.Tags[i]);
        }
    }

    [Fact]
    public void ReadByKey_CompositeKeys_WorksCorrectly()
    {
        // Arrange
        var testData = TestUtilities.GenerateCompositeKeyTestData(8);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.CompositeKey, "composite_keys");
        var targetItem = testData[3];

        // Act
        using var reader = new NdjsonReader<string, CompositeKeyModel>(dataPath, indexPath);
        var result = reader.ReadByKey(targetItem.CompositeKey);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(targetItem.Part1, result.Part1);
        Assert.Equal(targetItem.Part2, result.Part2);
        Assert.Equal(targetItem.Data, result.Data);
    }

    [Fact]
    public void ReadByKey_LargeObjects_HandlesCorrectly()
    {
        // Arrange
        var testData = TestUtilities.GenerateLargeTestData(5, textSize: 2000);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "large_objects");
        var targetItem = testData[2];

        // Act
        using var reader = new NdjsonReader<string, LargeTestModel>(dataPath, indexPath);
        var result = reader.ReadByKey(targetItem.Id);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(targetItem.Id, result.Id);
        Assert.Equal(targetItem.LargeText.Length, result.LargeText.Length);
        Assert.Equal(targetItem.Numbers.Length, result.Numbers.Length);
        Assert.Equal(targetItem.Properties.Count, result.Properties.Count);
    }

    [Fact]
    public void ReadByKey_WithCustomJsonOptions_DeserializesCorrectly()
    {
        var testData = TestUtilities.GenerateSimpleTestData(5);
        var customOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
        
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id, customOptions);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "custom_json_read");
        writer.Write(testData, dataPath, indexPath);

        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath, customOptions);
        var result = reader.ReadByKey(testData[2].Id);

        Assert.NotNull(result);
        Assert.Equal(testData[2].Id, result.Id);
        Assert.Equal(testData[2].Name, result.Name);
        Assert.Equal(testData[2].Value, result.Value);
    }

    [Fact]
    public void Indices_ReturnsAllKeys()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(12);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "indices_test");

        // Act
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        var indices = reader.Indices.ToList();

        // Assert
        Assert.Equal(testData.Count, indices.Count);
        var expectedKeys = testData.Select(x => x.Id).ToHashSet();
        var actualKeys = indices.ToHashSet();
        Assert.Equal(expectedKeys, actualKeys);
    }

    [Fact]
    public void ReadByKey_ConcurrentAccess_ThreadSafe()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(100);
        var (dataPath, indexPath) = CreateTestFiles(testData, x => x.Id, "thread_safety");
        var keys = testData.Select(x => x.Id).ToArray();

        // Act
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        var results = new System.Collections.Concurrent.ConcurrentBag<SimpleTestModel>();
        var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        Parallel.ForEach(keys, key =>
        {
            try
            {
                var result = reader.ReadByKey(key);
                if (result != null)
                {
                    results.Add(result);
                }
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
        });

        // Assert
        Assert.Empty(exceptions);
        Assert.Equal(testData.Count, results.Count);
    }

}
