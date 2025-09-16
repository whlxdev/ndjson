using System.Text.Json;
using JsonSerializer = System.Text.Json.JsonSerializer;

namespace Ndjson.Test;

public class IntegrationTests : IDisposable
{
    private readonly string _tempDir = TestUtilities.CreateTempDirectory();

    public void Dispose()
    {
        TestUtilities.CleanupTempDirectory(_tempDir);
    }
    
    [Fact]
    public void CompleteWorkflow_WriteReadSync_WorksEndToEnd()
    {
        var originalData = TestUtilities.GenerateSimpleTestData(50);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "complete_sync");

        writer.Write(originalData, dataPath, indexPath);

        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        
        var firstItem = reader.ReadByKey(originalData[0].Id);
        var lastItem = reader.ReadByKey(originalData[^1].Id);
        var someKeys = originalData.Take(10).Select(x => x.Id).ToArray();
        var batchResults = reader.ReadByKeys(someKeys);
        var allKeys = reader.Indices.ToArray();
        var allResults = reader.ReadByKeys(allKeys);

        Assert.NotNull(firstItem);
        Assert.Equal(originalData[0].Name, firstItem.Name);
        Assert.NotNull(lastItem);
        Assert.Equal(originalData[^1].Value, lastItem.Value);
        Assert.Equal(10, batchResults.Count);
        Assert.Equal(originalData.Count, allResults.Count);
        
        foreach (var original in originalData)
        {
            Assert.True(allResults.ContainsKey(original.Id));
            var retrieved = allResults[original.Id];
            Assert.Equal(original.Id, retrieved.Id);
            Assert.Equal(original.Name, retrieved.Name);
            Assert.Equal(original.Value, retrieved.Value);
        }
    }

    [Fact]
    public async Task CompleteWorkflow_WriteReadAsync_WorksEndToEnd()
    {
        var originalData = TestUtilities.GenerateComplexTestData(30);
        var writer = new NdjsonWriter<string, ComplexTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "complete_async");

        await writer.WriteAsync(originalData, dataPath, indexPath);

        using var reader = new NdjsonReader<string, ComplexTestModel>(dataPath, indexPath);
        
        var firstItem = await reader.ReadByKeyAsync(originalData[0].Id);
        var middleItem = await reader.ReadByKeyAsync(originalData[15].Id);
        var someKeys = originalData.Skip(5).Take(15).Select(x => x.Id).ToArray();
        var batchResults = await reader.ReadByKeysAsync(someKeys);
        var allKeys = reader.Indices.ToArray();
        var allResults = await reader.ReadByKeysAsync(allKeys);

        Assert.NotNull(firstItem);
        Assert.Equal(originalData[0].Metadata.Category, firstItem.Metadata.Category);
        Assert.NotNull(middleItem);
        Assert.Equal(originalData[15].Tags.Length, middleItem.Tags.Length);
        Assert.Equal(15, batchResults.Count);
        Assert.Equal(originalData.Count, allResults.Count);
        
        foreach (var original in originalData)
        {
            Assert.True(allResults.ContainsKey(original.Id));
            var retrieved = allResults[original.Id];
            Assert.Equal(original.Id, retrieved.Id);
            Assert.Equal(original.Metadata.Category, retrieved.Metadata.Category);
            Assert.Equal(original.Metadata.Priority, retrieved.Metadata.Priority);
            Assert.Equal(original.Metadata.Active, retrieved.Metadata.Active);
            Assert.Equal(original.Tags.Length, retrieved.Tags.Length);
        }
    }

    [Fact]
    public void CompleteWorkflow_AutoIndexGeneration_WorksCorrectly()
    {
        // Arrange
        var originalData = TestUtilities.GenerateNumericKeyTestData(25);
        var writer = new NdjsonWriter<int, NumericKeyModel>(x => x.NumericId);
        var dataPath = Path.Combine(_tempDir, "auto_index.ndjson");

        // Act - Write with auto-generated index
        writer.Write(originalData, dataPath);

        // Act - Read with auto-detected index
        using var reader = new NdjsonReader<int, NumericKeyModel>(dataPath);
        
        var allKeys = reader.Indices.ToArray();
        var allResults = reader.ReadByKeys(allKeys);

        // Assert
        Assert.Equal(originalData.Count, allResults.Count);
        
        foreach (var original in originalData)
        {
            Assert.True(allResults.ContainsKey(original.NumericId));
            var retrieved = allResults[original.NumericId];
            Assert.Equal(original.Description, retrieved.Description);
        }
    }

    [Fact]
    public void CompleteWorkflow_RuntimeIndexBuilding_WorksCorrectly()
    {
        // Arrange
        var originalData = TestUtilities.GenerateCompositeKeyTestData(20);
        var writer = new NdjsonWriter<string, CompositeKeyModel>(x => x.CompositeKey);
        var dataPath = Path.Combine(_tempDir, "runtime_index.ndjson");
        
        // Write only the data file (no index)
        using (var stream = new FileStream(dataPath, FileMode.Create, FileAccess.Write))
        using (var bufferedStream = new BufferedStream(stream))
        {
            var newlineBytes = System.Text.Encoding.UTF8.GetBytes(Environment.NewLine);
            foreach (var item in originalData)
            {
                var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(item);
                bufferedStream.Write(jsonBytes);
                bufferedStream.Write(newlineBytes);
            }
        }

        // Act - Read with runtime index building
        using var reader = new NdjsonReader<string, CompositeKeyModel>(dataPath, x => x.CompositeKey);
        
        var allKeys = reader.Indices.ToArray();
        var allResults = reader.ReadByKeys(allKeys);

        // Assert
        Assert.Equal(originalData.Count, allResults.Count);
        
        foreach (var original in originalData)
        {
            Assert.True(allResults.ContainsKey(original.CompositeKey));
            var retrieved = allResults[original.CompositeKey];
            Assert.Equal(original.Part1, retrieved.Part1);
            Assert.Equal(original.Part2, retrieved.Part2);
            Assert.Equal(original.Data, retrieved.Data);
        }
    }
    
    [Fact]
    public void PerformanceIntegration_LargeDataset_MaintainsPerformance()
    {
        var largeDataset = TestUtilities.GenerateSimpleTestData(2000);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "performance_large");

        var writeStopwatch = System.Diagnostics.Stopwatch.StartNew();
        writer.Write(largeDataset, dataPath, indexPath);
        writeStopwatch.Stop();

        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        
        var readStopwatch = System.Diagnostics.Stopwatch.StartNew();
        var randomKeys = largeDataset.Take(100).Select(x => x.Id).ToArray();
        var results = reader.ReadByKeys(randomKeys);
        readStopwatch.Stop();

        Assert.True(writeStopwatch.ElapsedMilliseconds < 10000, 
            $"Write took {writeStopwatch.ElapsedMilliseconds}ms, expected < 10000ms");
        Assert.True(readStopwatch.ElapsedMilliseconds < 2000, 
            $"Read took {readStopwatch.ElapsedMilliseconds}ms, expected < 2000ms");
        
        Assert.Equal(100, results.Count);
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
    }

    [Fact]
    public async Task PerformanceIntegration_LargeDatasetAsync_MaintainsPerformance()
    {
        var largeDataset = TestUtilities.GenerateLargeTestData(500, textSize: 1000);
        var writer = new NdjsonWriter<string, LargeTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "performance_large_async");

        var writeStopwatch = System.Diagnostics.Stopwatch.StartNew();
        await writer.WriteAsync(largeDataset, dataPath, indexPath);
        writeStopwatch.Stop();

        using var reader = new NdjsonReader<string, LargeTestModel>(dataPath, indexPath);
        
        var readStopwatch = System.Diagnostics.Stopwatch.StartNew();
        var randomKeys = largeDataset.Take(50).Select(x => x.Id).ToArray();
        var results = await reader.ReadByKeysAsync(randomKeys);
        readStopwatch.Stop();

        Assert.True(writeStopwatch.ElapsedMilliseconds < 15000, 
            $"WriteAsync took {writeStopwatch.ElapsedMilliseconds}ms, expected < 15000ms");
        Assert.True(readStopwatch.ElapsedMilliseconds < 3000, 
            $"ReadAsync took {readStopwatch.ElapsedMilliseconds}ms, expected < 3000ms");
        
        Assert.Equal(50, results.Count);
        
        var firstResult = results.Values.First();
        Assert.True(firstResult.LargeText.Length > 500);
        Assert.True(firstResult.Numbers.Length > 0);
        Assert.True(firstResult.Properties.Count > 0);
    }

    [Fact]
    public void Integration_CustomJsonOptions_WorksEndToEnd()
    {
        var customOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };
        
        var originalData = TestUtilities.GenerateSimpleTestData(15);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id, customOptions);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "custom_json_integration");

        writer.Write(originalData, dataPath, indexPath);

        var fileContent = File.ReadAllText(dataPath);
        Assert.Contains("\"id\":", fileContent);
        Assert.Contains("\"name\":", fileContent);
        Assert.Contains("\"value\":", fileContent);

        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath, customOptions);
        var allKeys = reader.Indices.ToArray();
        var allResults = reader.ReadByKeys(allKeys);

        Assert.Equal(originalData.Count, allResults.Count);
        
        foreach (var original in originalData)
        {
            Assert.True(allResults.ContainsKey(original.Id));
            var retrieved = allResults[original.Id];
            Assert.Equal(original.Id, retrieved.Id);
            Assert.Equal(original.Name, retrieved.Name);
            Assert.Equal(original.Value, retrieved.Value);
        }
    }

    [Fact]
    public void Integration_CorruptedIndexFile_FallsBackGracefully()
    {
        var originalData = TestUtilities.GenerateSimpleTestData(10);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "corrupted_index");
        
        writer.Write(originalData, dataPath, indexPath);
        File.WriteAllText(indexPath, "{ corrupted json }");

        var exception = Assert.ThrowsAny<System.Text.Json.JsonException>(() => 
            new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath));
        
        Assert.Contains("invalid start of a property name", exception.Message);
        
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, x => x.Id);
        var result = reader.ReadByKey(originalData[0].Id);
        
        Assert.NotNull(result);
        Assert.Equal(originalData[0].Id, result.Id);
    }

    [Fact]
    public void Integration_MissingIndexFile_RuntimeIndexBuilding()
    {
        var originalData = TestUtilities.GenerateSimpleTestData(8);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "missing_index");
        
        writer.Write(originalData, dataPath, indexPath);
        File.Delete(indexPath);

        Assert.Throws<FileNotFoundException>(() => 
            new NdjsonReader<string, SimpleTestModel>(dataPath));
        
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, x => x.Id);
        var allKeys = reader.Indices.ToArray();
        var allResults = reader.ReadByKeys(allKeys);

        Assert.Equal(originalData.Count, allResults.Count);
    }

    [Fact]
    public void Integration_ConcurrentReads_ThreadSafe()
    {
        var originalData = TestUtilities.GenerateSimpleTestData(200);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "concurrent_reads");
        
        writer.Write(originalData, dataPath, indexPath);

        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        var results = new System.Collections.Concurrent.ConcurrentBag<SimpleTestModel>();
        var exceptions = new System.Collections.Concurrent.ConcurrentBag<Exception>();
        
        var keys = originalData.Select(x => x.Id).ToArray();
        
        Parallel.ForEach(keys, new ParallelOptions { MaxDegreeOfParallelism = 8 }, key =>
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

        Assert.Empty(exceptions);
        Assert.Equal(originalData.Count, results.Count);
        
        var resultIds = results.Select(r => r.Id).ToHashSet();
        var originalIds = originalData.Select(o => o.Id).ToHashSet();
        Assert.Equal(originalIds, resultIds);
    }

    [Fact]
    public void Integration_LargeDataset_EfficientMemoryUsage()
    {
        var largeDataset = TestUtilities.GenerateSimpleTestData(5000);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "memory_efficiency");

        var initialMemory = GC.GetTotalMemory(true);
        writer.Write(largeDataset, dataPath, indexPath);
        var afterWriteMemory = GC.GetTotalMemory(true);

        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        var randomKeys = largeDataset.Take(100).Select(x => x.Id).ToArray();
        var results = reader.ReadByKeys(randomKeys);
        var afterReadMemory = GC.GetTotalMemory(true);

        var writeMemoryIncrease = afterWriteMemory - initialMemory;
        var readMemoryIncrease = afterReadMemory - afterWriteMemory;
        
        Assert.True(writeMemoryIncrease < 50_000_000,
            $"Write memory increase: {writeMemoryIncrease:N0} bytes");
        Assert.True(readMemoryIncrease < 10_000_000,
            $"Read memory increase: {readMemoryIncrease:N0} bytes");
        
        Assert.Equal(100, results.Count);
    }

}
