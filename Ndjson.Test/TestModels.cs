using System.Text.Json.Serialization;

namespace Ndjson.Test;

public record SimpleTestModel(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("value")] int Value
);

public record ComplexTestModel(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("metadata")] TestMetadata Metadata,
    [property: JsonPropertyName("tags")] string[] Tags,
    [property: JsonPropertyName("timestamp")] DateTime Timestamp
);

public record TestMetadata(
    [property: JsonPropertyName("category")] string Category,
    [property: JsonPropertyName("priority")] int Priority,
    [property: JsonPropertyName("active")] bool Active
);

public record NumericKeyModel(
    [property: JsonPropertyName("numericId")] int NumericId,
    [property: JsonPropertyName("description")] string Description
);

public record CompositeKeyModel(
    [property: JsonPropertyName("part1")] string Part1,
    [property: JsonPropertyName("part2")] int Part2,
    [property: JsonPropertyName("data")] string Data
)
{
    public string CompositeKey => $"{Part1}_{Part2}";
}

public record LargeTestModel(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("largeText")] string LargeText,
    [property: JsonPropertyName("numbers")] int[] Numbers,
    [property: JsonPropertyName("properties")] Dictionary<string, string> Properties
);
