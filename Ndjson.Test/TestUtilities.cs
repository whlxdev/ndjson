using System.Text;

namespace Ndjson.Test;

internal static class TestUtilities
{
    public static string CreateTempDirectory()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "NdjsonTest_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(tempDir);
        return tempDir;
    }

    public static void CleanupTempDirectory(string tempDir)
    {
        if (Directory.Exists(tempDir))
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    public static List<SimpleTestModel> GenerateSimpleTestData(int count)
    {
        var data = new List<SimpleTestModel>();
        for (var i = 0; i < count; i++)
        {
            data.Add(new SimpleTestModel(
                Id: $"test-{i:D4}",
                Name: $"Test Item {i}",
                Value: i * 10
            ));
        }
        return data;
    }

    public static List<ComplexTestModel> GenerateComplexTestData(int count)
    {
        var categories = new[] { "Category A", "Category B", "Category C" };
        var data = new List<ComplexTestModel>();
        
        for (var i = 0; i < count; i++)
        {
            data.Add(new ComplexTestModel(
                Id: $"complex-{i:D4}",
                Metadata: new TestMetadata(
                    Category: categories[i % categories.Length],
                    Priority: (i % 5) + 1,
                    Active: i % 2 == 0
                ),
                Tags: new[] { $"tag{i % 3}", $"tag{(i + 1) % 3}" },
                Timestamp: DateTime.UtcNow.AddMinutes(-i)
            ));
        }
        return data;
    }

    public static List<NumericKeyModel> GenerateNumericKeyTestData(int count)
    {
        var data = new List<NumericKeyModel>();
        for (var i = 0; i < count; i++)
        {
            data.Add(new NumericKeyModel(
                NumericId: i + 1000,
                Description: $"Numeric item {i}"
            ));
        }
        return data;
    }

    public static List<CompositeKeyModel> GenerateCompositeKeyTestData(int count)
    {
        var parts = new[] { "alpha", "beta", "gamma", "delta" };
        var data = new List<CompositeKeyModel>();
        
        for (var i = 0; i < count; i++)
        {
            data.Add(new CompositeKeyModel(
                Part1: parts[i % parts.Length],
                Part2: i,
                Data: $"Composite data {i}"
            ));
        }
        return data;
    }

    public static List<LargeTestModel> GenerateLargeTestData(int count, int textSize = 1000)
    {
        var data = new List<LargeTestModel>();
        var random = new Random(42);
        
        for (var i = 0; i < count; i++)
        {
            var largeText = GenerateRandomText(textSize, random);
            var numbers = Enumerable.Range(0, 50).Select(_ => random.Next(1000)).ToArray();
            var properties = new Dictionary<string, string>();
            
            for (var j = 0; j < 10; j++)
            {
                properties[$"prop{j}"] = $"value{j}_{i}";
            }
            
            data.Add(new LargeTestModel(
                Id: $"large-{i:D4}",
                LargeText: largeText,
                Numbers: numbers,
                Properties: properties
            ));
        }
        return data;
    }

    private static string GenerateRandomText(int size, Random random)
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";
        var result = new StringBuilder(size);
        
        for (var i = 0; i < size; i++)
        {
            result.Append(chars[random.Next(chars.Length)]);
        }
        
        return result.ToString();
    }

    public static bool CollectionsEqual<T>(IEnumerable<T> expected, IEnumerable<T> actual)
    {
        var expectedList = expected.ToList();
        var actualList = actual.ToList();
        
        if (expectedList.Count != actualList.Count)
        {
            return false;
        }
            
        return expectedList.All(actualList.Contains) && actualList.All(expectedList.Contains);
    }

    public static (string dataPath, string indexPath) GetTestFilePaths(string tempDir, string baseName)
    {
        var dataPath = Path.Combine(tempDir, $"{baseName}.ndjson");
        var indexPath = Path.Combine(tempDir, $"{baseName}.index.json");
        return (dataPath, indexPath);
    }
}
