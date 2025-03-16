using System.Collections.Concurrent;
using System.Linq.Expressions;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace FastParquetWriter.WriteToParquet;

/// <summary>
/// Class for writing Parquet files.
/// </summary>
public class WriteToParquet : IDisposable
{
    private FileStream? _fileStream;
    private ParquetWriter? _parquetWriter;
    private static readonly ConcurrentDictionary<string, Delegate> _propertyAccessorsCache = new();

    // Use proper async exception handling
    /// <summary>
    /// Initializes a new instance of the <see cref="WriteToParquet"/> class.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <param name="schema">The schema.</param>
    /// <exception cref="ArgumentException">Thrown when the file path is empty.</exception>
    /// <exception cref="ArgumentNullException">Thrown when the schema is not provided.</exception>
    /// <exception cref="IOException">Thrown when failed to open the parquet file.</exception>
    public async Task InitializeAsync(string filePath, ParquetSchema schema)
    {
        if (string.IsNullOrEmpty(filePath)) throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        if (schema == null) throw new ArgumentNullException(nameof(schema));

        try
        {
            _fileStream = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None);
            _parquetWriter = await ParquetWriter.CreateAsync(schema, _fileStream);
            _parquetWriter.CompressionMethod = CompressionMethod.Snappy;
        }
        catch (Exception ex)
        {
            // Clean up in case of failure
            Dispose();
            throw new IOException($"Failed to initialize Parquet writer. Details: {ex.Message}", ex);
        }
    }

    // Handle exceptions in writing row groups
    /// <summary>
    /// Writes the row group.
    /// </summary>
    /// <typeparam name="T">Type of the data.</typeparam>
    /// <param name="data">The data to write.</param>
    /// <param name="schema">The schema.</param>
    /// <exception cref="ArgumentException">Thrown when the data is null or empty.</exception>
    /// <exception cref="ArgumentNullException">Thrown when the schema is not provided.</exception>
    /// <exception cref="IOException">Thrown when failed to write the row group.</exception>
    public async Task WriteRowGroupAsync<T>(List<T> data, ParquetSchema schema)
    {
        if (data == null || data.Count == 0) throw new ArgumentException("Data cannot be null or empty.", nameof(data));
        if (schema == null) throw new ArgumentNullException(nameof(schema));

        var dataFields = schema.Fields.OfType<DataField>().ToList();

        try
        {
            // Precompute values for all fields, reducing per-row computation
            var columnValues = dataFields.Select(df => PrecomputeColumnValues(data, df)).ToArray();

            using (var groupWriter = _parquetWriter!.CreateRowGroup())
            {
                var writeTasks = new List<Task>();
                for (int i = 0; i < dataFields.Count; i++)
                {
                    var dataColumn = new DataColumn(dataFields[i], columnValues[i]);
                    writeTasks.Add(groupWriter.WriteColumnAsync(dataColumn));
                }

                // Write columns in parallel
                await Task.WhenAll(writeTasks);
            }
        }
        catch (Exception ex)
        {
            throw new IOException($"Failed to write row group. Details: {ex.Message}", ex);
        }
    }

    // Safely precompute column values with error handling
    /// <summary>
    /// Precomputes the column values.
    /// </summary>
    /// <typeparam name="T">The type of the data.</typeparam>
    /// <param name="data">The data to precompute.</param>
    /// <param name="dataField">The data field.</param>
    /// <returns>An array of precomputed values.</returns>
    /// <exception cref="ArgumentNullException">Thrown when the data field is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when failed to precompute column values.</exception>
    private Array PrecomputeColumnValues<T>(List<T> data, DataField dataField)
    {
        if (dataField == null) throw new ArgumentNullException(nameof(dataField));

        var propertyAccessor = GetPropertyAccessor<T>(dataField.Name);
        var values = Array.CreateInstance(dataField.ClrType, data.Count);

        try
        {
            Parallel.For(0, data.Count, i =>
            {
                values.SetValue(propertyAccessor(data[i]), i);
            });
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to precompute column values for field {dataField.Name}.", ex);
        }

        return values;
    }

    // Property accessor caching and exception handling
    /// <summary>
    /// Gets the property accessor.
    /// </summary>
    /// <typeparam name="T">The type of the data.</typeparam>
    /// <param name="propertyName">The property name.</param>
    /// <exception cref="ArgumentException">Thrown when the property name is null or empty.</exception>
    /// <exception cref="InvalidOperationException">Thrown when failed to create property accessor.</exception>
    private static Func<T, object> GetPropertyAccessor<T>(string propertyName)
    {
        if (string.IsNullOrEmpty(propertyName)) throw new ArgumentException("Property name cannot be null or empty.", nameof(propertyName));

        var key = $"{typeof(T).FullName}.{propertyName}";
        return (Func<T, object>)_propertyAccessorsCache.GetOrAdd(key, _ =>
        {
            try
            {
                var parameter = Expression.Parameter(typeof(T), "x");
                var property = Expression.Property(parameter, propertyName);
                var convert = Expression.Convert(property, typeof(object));
                var lambda = Expression.Lambda<Func<T, object>>(convert, parameter);
                return lambda.Compile();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to create property accessor for {propertyName}.", ex);
            }
        });
    }

    // Properly handle closing of resources
    /// <summary>
    /// Closes the file.
    /// </summary>
    /// <exception cref="IOException">Thrown when failed to close the file and release resources.</exception>
    public async Task CloseFileAsync()
    {
        try
        {
            if (_parquetWriter != null)
            {
                _parquetWriter.Dispose();
                _parquetWriter = null;
            }

            if (_fileStream != null)
            {
                await _fileStream.DisposeAsync();
                _fileStream = null;
            }
        }
        catch (Exception ex)
        {
            throw new IOException("Failed to close file and release resources.", ex);
        }
    }

    // IDisposable implementation with proper resource handling
    /// <summary>
    /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when an error occurred while disposing the WriteToParquet object.</exception>
    public void Dispose()
    {

        CloseFileAsync().GetAwaiter().GetResult();


    }
}
