using System.Text.Json;

namespace Ndjson.Test;

public class NdjsonWriterTests : IDisposable
{
    private readonly string _tempDir = TestUtilities.CreateTempDirectory();

    public void Dispose()
    {
        TestUtilities.CleanupTempDirectory(_tempDir);
    }

    [Fact]
    public void Write_SimpleData_CreatesValidFiles()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(10);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "simple");

        // Act
        writer.Write(testData, dataPath, indexPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        
        // Verify data file content
        var lines = File.ReadAllLines(dataPath);
        Assert.Equal(testData.Count, lines.Length);
        
        // Verify each line is valid JSON
        foreach (var line in lines)
        {
            Assert.NotEmpty(line);
            var parsed = JsonSerializer.Deserialize<SimpleTestModel>(line);
            Assert.NotNull(parsed);
        }
    }

    [Fact]
    public async Task WriteAsync_SimpleData_CreatesValidFiles()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(10);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "simple_async");

        // Act
        await writer.WriteAsync(testData, dataPath, indexPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        
        var lines = File.ReadAllLines(dataPath);
        Assert.Equal(testData.Count, lines.Length);
    }

    [Fact]
    public void Write_WithSinglePath_CreatesIndexAutomatically()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(5);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var dataPath = Path.Combine(_tempDir, "auto_index.ndjson");
        var expectedIndexPath = Path.Combine(_tempDir, "auto_index.index.json");

        // Act
        writer.Write(testData, dataPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(expectedIndexPath));
    }

    [Fact]
    public async Task WriteAsync_WithSinglePath_CreatesIndexAutomatically()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(5);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var dataPath = Path.Combine(_tempDir, "auto_index_async.ndjson");
        var expectedIndexPath = Path.Combine(_tempDir, "auto_index_async.index.json");

        // Act
        await writer.WriteAsync(testData, dataPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(expectedIndexPath));
    }

    [Fact]
    public void Write_ComplexData_HandlesNestedObjects()
    {
        // Arrange
        var testData = TestUtilities.GenerateComplexTestData(5);
        var writer = new NdjsonWriter<string, ComplexTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "complex");

        // Act
        writer.Write(testData, dataPath, indexPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        var lines = File.ReadAllLines(dataPath);
        Assert.Equal(testData.Count, lines.Length);
        
        // Verify complex object serialization
        var firstLine = lines[0];
        var parsed = JsonSerializer.Deserialize<ComplexTestModel>(firstLine);
        Assert.NotNull(parsed);
        Assert.NotNull(parsed.Metadata);
        Assert.NotEmpty(parsed.Tags);
    }

    [Fact]
    public void Write_NumericKeys_WorksCorrectly()
    {
        // Arrange
        var testData = TestUtilities.GenerateNumericKeyTestData(8);
        var writer = new NdjsonWriter<int, NumericKeyModel>(x => x.NumericId);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "numeric");

        // Act
        writer.Write(testData, dataPath, indexPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        
        var lines = File.ReadAllLines(dataPath);
        Assert.Equal(testData.Count, lines.Length);
    }

    [Fact]
    public void Write_CompositeKeys_WorksCorrectly()
    {
        // Arrange
        var testData = TestUtilities.GenerateCompositeKeyTestData(6);
        var writer = new NdjsonWriter<string, CompositeKeyModel>(x => x.CompositeKey);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "composite");

        // Act
        writer.Write(testData, dataPath, indexPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        
        var lines = File.ReadAllLines(dataPath);
        Assert.Equal(testData.Count, lines.Length);
    }

    [Fact]
    public void Write_SameDataAndIndexPath_ThrowsArgumentException()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(1);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var samePath = Path.Combine(_tempDir, "same.json");

        // Act & Assert
        var exception = Assert.Throws<ArgumentException>(() => 
            writer.Write(testData, samePath, samePath));
        Assert.Contains("cannot be the same file", exception.Message);
    }

    [Fact]
    public async Task WriteAsync_SameDataAndIndexPath_ThrowsArgumentException()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(1);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var samePath = Path.Combine(_tempDir, "same_async.json");

        // Act & Assert
        var exception = await Assert.ThrowsAsync<ArgumentException>(() => 
            writer.WriteAsync(testData, samePath, samePath));
        Assert.Contains("cannot be the same file", exception.Message);
    }

    [Fact]
    public void Write_EmptyCollection_CreatesEmptyFiles()
    {
        // Arrange
        var testData = new List<SimpleTestModel>();
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "empty");

        // Act
        writer.Write(testData, dataPath, indexPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        Assert.Empty(File.ReadAllLines(dataPath));
    }

    [Fact]
    public void Write_InvalidDirectory_ThrowsDirectoryNotFoundException()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(1);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var invalidPath = Path.Combine(_tempDir, "nonexistent", "file.ndjson");

        // Act & Assert
        Assert.Throws<DirectoryNotFoundException>(() => 
            writer.Write(testData, invalidPath));
    }

    [Fact]
    public void Write_LargeDataset_CompletesInReasonableTime()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(1000);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "large");

        // Act
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        writer.Write(testData, dataPath, indexPath);
        stopwatch.Stop();

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        Assert.True(stopwatch.ElapsedMilliseconds < 5000, $"Write took {stopwatch.ElapsedMilliseconds}ms, expected < 5000ms");
        
        var lines = File.ReadAllLines(dataPath);
        Assert.Equal(testData.Count, lines.Length);
    }

    [Fact]
    public async Task WriteAsync_LargeDataset_CompletesInReasonableTime()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(1000);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "large_async");

        // Act
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await writer.WriteAsync(testData, dataPath, indexPath);
        stopwatch.Stop();

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        Assert.True(stopwatch.ElapsedMilliseconds < 5000, $"WriteAsync took {stopwatch.ElapsedMilliseconds}ms, expected < 5000ms");
    }

    [Fact]
    public void Write_VeryLargeObjects_HandlesCorrectly()
    {
        // Arrange
        var testData = TestUtilities.GenerateLargeTestData(10, textSize: 5000);
        var writer = new NdjsonWriter<string, LargeTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "very_large");

        // Act
        writer.Write(testData, dataPath, indexPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        
        var lines = File.ReadAllLines(dataPath);
        Assert.Equal(testData.Count, lines.Length);
        
        // Verify large objects are serialized correctly
        var firstLine = lines[0];
        var parsed = JsonSerializer.Deserialize<LargeTestModel>(firstLine);
        Assert.NotNull(parsed);
        Assert.True(parsed.LargeText.Length > 4000);
    }

    [Fact]
    public async Task WriteAsync_WithCancellation_CanBeCancelled()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(10000); // Large dataset
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "cancelled");
        
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => 
            writer.WriteAsync(testData, dataPath, indexPath, cts.Token));
    }

    [Fact]
    public void Write_WithCustomJsonOptions_UsesCustomSettings()
    {
        // Arrange
        var testData = TestUtilities.GenerateSimpleTestData(2);
        var customOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id, customOptions);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "custom_json");

        // Act
        writer.Write(testData, dataPath, indexPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        var content = File.ReadAllText(dataPath);
        
        // Should use camelCase property names
        Assert.Contains("\"id\":", content);
        Assert.Contains("\"name\":", content);
        Assert.Contains("\"value\":", content);
    }

    [Fact]
    public void Write_ThenRead_DataRoundTrip()
    {
        // Arrange
        var originalData = TestUtilities.GenerateSimpleTestData(20);
        var writer = new NdjsonWriter<string, SimpleTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "roundtrip");

        // Act - Write
        writer.Write(originalData, dataPath, indexPath);

        // Act - Read back
        using var reader = new NdjsonReader<string, SimpleTestModel>(dataPath, indexPath);
        var readData = new List<SimpleTestModel>();
        
        foreach (var key in reader.Indices)
        {
            var item = reader.ReadByKey(key);
            if (item != null)
            {
                readData.Add(item);
            }
        }

        // Assert
        Assert.Equal(originalData.Count, readData.Count);
        Assert.True(TestUtilities.CollectionsEqual(originalData, readData));
    }

    [Fact]
    public async Task WriteAsync_ThenReadAsync_DataRoundTrip()
    {
        // Arrange
        var originalData = TestUtilities.GenerateComplexTestData(15);
        var writer = new NdjsonWriter<string, ComplexTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "roundtrip_async");

        // Act - Write
        await writer.WriteAsync(originalData, dataPath, indexPath);

        // Act - Read back
        using var reader = new NdjsonReader<string, ComplexTestModel>(dataPath, indexPath);
        var keys = reader.Indices.ToList();
        var readData = await reader.ReadByKeysAsync(keys);

        // Assert
        Assert.Equal(originalData.Count, readData.Count);
        foreach (var original in originalData)
        {
            Assert.True(readData.ContainsKey(original.Id));
            var read = readData[original.Id];
            Assert.Equal(original.Id, read.Id);
            Assert.Equal(original.Metadata.Category, read.Metadata.Category);
        }
    }

    [Fact]
    public void Write_ObjectsLargerThanBuffer_HandlesCorrectly()
    {
        // Arrange - Create objects larger than the 128KB buffer
        var testData = TestUtilities.GenerateLargeTestData(3, textSize: 150_000); // 150KB text per object
        var writer = new NdjsonWriter<string, LargeTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "buffer_overflow");

        // Act
        writer.Write(testData, dataPath, indexPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        
        var lines = File.ReadAllLines(dataPath);
        Assert.Equal(testData.Count, lines.Length);
        
        // Verify each large object is serialized correctly
        for (var i = 0; i < lines.Length; i++)
        {
            var parsed = JsonSerializer.Deserialize<LargeTestModel>(lines[i]);
            Assert.NotNull(parsed);
            Assert.Equal(testData[i].Id, parsed.Id);
            Assert.True(parsed.LargeText.Length >= 150_000, $"Object {i} text length: {parsed.LargeText.Length}");
        }
    }

    [Fact]
    public async Task WriteAsync_ObjectsLargerThanBuffer_HandlesCorrectly()
    {
        // Arrange - Create objects larger than the 128KB buffer
        var testData = TestUtilities.GenerateLargeTestData(3, textSize: 200_000); // 200KB text per object
        var writer = new NdjsonWriter<string, LargeTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "buffer_overflow_async");

        // Act
        await writer.WriteAsync(testData, dataPath, indexPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        
        var lines = File.ReadAllLines(dataPath);
        Assert.Equal(testData.Count, lines.Length);
        
        // Verify each large object is serialized correctly
        for (var i = 0; i < lines.Length; i++)
        {
            var parsed = JsonSerializer.Deserialize<LargeTestModel>(lines[i]);
            Assert.NotNull(parsed);
            Assert.Equal(testData[i].Id, parsed.Id);
            Assert.True(parsed.LargeText.Length >= 200_000, $"Object {i} text length: {parsed.LargeText.Length}");
        }
    }

    [Fact]
    public void Write_MixedSmallAndLargeObjects_HandlesCorrectly()
    {
        // Arrange - Mix small and large objects to test buffer management
        var smallObjects = TestUtilities.GenerateSimpleTestData(5);
        var largeObjects = TestUtilities.GenerateLargeTestData(2, textSize: 160_000); // 160KB each
        
        // Create a mixed dataset
        var mixedData = new List<object>();
        mixedData.AddRange(smallObjects.Cast<object>());
        mixedData.AddRange(largeObjects.Cast<object>());
        
        // Use a custom model that can handle both types
        var testData = mixedData.Select((obj, index) => new MixedTestModel
        {
            Id = $"mixed-{index:D4}",
            Data = obj switch
            {
                SimpleTestModel simple => JsonSerializer.Serialize(simple),
                LargeTestModel large => JsonSerializer.Serialize(large),
                _ => throw new InvalidOperationException("Unknown object type")
            }
        }).ToList();

        var writer = new NdjsonWriter<string, MixedTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "mixed_objects");

        // Act
        writer.Write(testData, dataPath, indexPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        
        var lines = File.ReadAllLines(dataPath);
        Assert.Equal(testData.Count, lines.Length);
        
        // Verify all objects are serialized correctly
        for (var i = 0; i < lines.Length; i++)
        {
            var parsed = JsonSerializer.Deserialize<MixedTestModel>(lines[i]);
            Assert.NotNull(parsed);
            Assert.Equal(testData[i].Id, parsed.Id);
            Assert.NotEmpty(parsed.Data);
        }
    }

    [Fact]
    public void Write_ObjectAtBufferSizeLimit_HandlesCorrectly()
    {
        // Arrange - Create an object that's exactly at the buffer size limit (128KB)
        var testData = TestUtilities.GenerateLargeTestData(1, textSize: 128_000); // Exactly 128KB
        var writer = new NdjsonWriter<string, LargeTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "buffer_limit");

        // Act
        writer.Write(testData, dataPath, indexPath);

        // Assert
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        
        var lines = File.ReadAllLines(dataPath);
        Assert.Single(lines);
        
        var parsed = JsonSerializer.Deserialize<LargeTestModel>(lines[0]);
        Assert.NotNull(parsed);
        Assert.Equal(testData[0].Id, parsed.Id);
        Assert.True(parsed.LargeText.Length >= 128_000);
    }

    [Fact]
    public void Write_VeryLargeObjectsWithCancellation_HandlesCorrectly()
    {
        // Arrange - Create very large objects to test cancellation with large data
        var testData = TestUtilities.GenerateLargeTestData(2, textSize: 300_000); // 300KB each
        var writer = new NdjsonWriter<string, LargeTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "very_large_cancellation");

        using var cts = new CancellationTokenSource();
        
        // Act & Assert - Should not throw for sync version (no cancellation support)
        writer.Write(testData, dataPath, indexPath);
        
        Assert.True(File.Exists(dataPath));
        Assert.True(File.Exists(indexPath));
        
        var lines = File.ReadAllLines(dataPath);
        Assert.Equal(testData.Count, lines.Length);
    }

    [Fact]
    public async Task WriteAsync_VeryLargeObjectsWithCancellation_CanBeCancelled()
    {
        // Arrange - Create very large objects to test cancellation
        var testData = TestUtilities.GenerateLargeTestData(10, textSize: 500_000); // 500KB each, large dataset
        var writer = new NdjsonWriter<string, LargeTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "large_cancelled");
        
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Cancel immediately

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => 
            writer.WriteAsync(testData, dataPath, indexPath, cts.Token));
    }

    [Fact]
    public void Write_DebugLargeObjectSerialization_ShowsActualOutput()
    {
        // Arrange - Create a simple test with one small and one large object
        var testData = new List<LargeTestModel>
        {
            TestUtilities.GenerateLargeTestData(1, textSize: 1000)[0], // Small object
            TestUtilities.GenerateLargeTestData(1, textSize: 160_000)[0] // Large object (160KB)
        };
        
        var writer = new NdjsonWriter<string, LargeTestModel>(x => x.Id);
        var (dataPath, indexPath) = TestUtilities.GetTestFilePaths(_tempDir, "debug_large");

        // Act
        writer.Write(testData, dataPath, indexPath);

        // Assert - Read the raw file content to see what's actually written
        var lines = File.ReadAllLines(dataPath);
        
        // Verify we have exactly 2 lines
        Assert.Equal(2, lines.Length);
        
        // Verify each line is valid JSON
        for (var i = 0; i < lines.Length; i++)
        {
            var parsed = JsonSerializer.Deserialize<LargeTestModel>(lines[i]);
            Assert.NotNull(parsed);
            Assert.Equal(testData[i].Id, parsed.Id);
        }
    }

    // Helper model for mixed object testing
    private class MixedTestModel
    {
        public string Id { get; set; } = string.Empty;
        public string Data { get; set; } = string.Empty;
    }

}
