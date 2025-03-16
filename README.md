# FastParquetWriter

**FastParquetWriter** is a high-performance .NET library designed for efficient writing of Parquet files. It enables developers to write large datasets in the columnar Parquet format quickly and with minimal resource consumption. The library leverages asynchronous operations for optimal performance and supports parallel data writing to enhance throughput.

## Features

- **High Performance**: Optimized for fast writing of Parquet files.
- **Asynchronous and Parallel**: Utilizes asynchronous I/O and parallel column writing for maximum throughput.
- **Schema Support**: Accepts a customizable Parquet schema for flexibility.
- **Compression**: Built-in Snappy compression for reduced file size and faster data processing.
- **Easy to Use**: Simple API for writing data to Parquet files with minimal configuration.

## Installation

Clone the repository or download the source files to use **FastParquetWriter** in your project.

```bash
git clone https://github.com/your-username/fast-parquet-writer.git
cd fast-parquet-writer
```

## Usage

### Writing Data to Parquet

The main class for writing data is `WriteToParquet`. Here’s an example of how to use it:

### Example

1. **Define your data type**: Create a class to represent your data.

```csharp
public class MyData
{
    public int Id { get; set; }
    public string Name { get; set; }
}
```


2.**Prepare the schema**: Define the schema corresponding to your data type.
```csharp
var schema = new ParquetSchema(
    new DataField<int>("Id"),
    new DataField<string>("Name")
);
```

3.**Write the data**: Use the WriteToParquet class to write data to a Parquet file.
```csharp
var data = new List<MyData>
{
    new MyData { Id = 1, Name = "Alice" },
    new MyData { Id = 2, Name = "Bob" },
};

var parquetWriter = new WriteToParquet();
await parquetWriter.InitializeAsync("data.parquet", schema);
await parquetWriter.WriteRowGroupAsync(data, schema);
await parquetWriter.CloseFileAsync();
```
## Explanation:
->InitializeAsync initializes the writer with the file path and schema.
->WriteRowGroupAsync writes the actual data to the Parquet file.
->CloseFileAsync properly closes the file and releases resources.
->Performance
This library has been optimized for performance. It uses Snappy compression to reduce the file size and supports writing data in parallel for faster processing. It also minimizes memory consumption by processing data in chunks.

## Viewing Parquet Files:
you can view the output Parquet files using online viewers such as [ Konbert Parquet Viewer](https://konbert.com/viewer/parquet). Simply upload your .parquet file, and you will be able to inspect the data and its structure.
## Contributing
Contributions are welcome! Feel free to fork the repository, submit issues, or create pull requests to enhance the library.

### Steps to contribute:
Fork the repository.
Create a new branch for your feature or bugfix.
Make your changes and commit them with clear messages.
Submit a pull request.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments
Parquet.Net: This library uses the Parquet.Net library for Parquet file handling.
Thanks to the open-source community for their valuable contributions and feedback!


