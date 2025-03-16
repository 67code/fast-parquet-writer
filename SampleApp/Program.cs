using FastParquetWriter.WriteToParquet;
using Parquet.Schema;

var parquetWriter = new WriteToParquet();

await parquetWriter.InitializeAsync("sample.parquet", new ParquetSchema(new DataField<int>("Id")));
await parquetWriter.WriteRowGroupAsync(new List<dynamic> { new { Id = 1 } }, new ParquetSchema(new DataField<int>("Id")));
await parquetWriter.CloseFileAsync();

Console.WriteLine("Parquet file written successfully!");
