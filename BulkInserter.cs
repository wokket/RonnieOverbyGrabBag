/* 
 * Written by Ronnie Overby
 * and part of the Ronnie Overby Grab Bag: https://github.com/ronnieoverby/RonnieOverbyGrabBag
 */

// Wokket 2020-01-24: Updated to async/await
// Wokket 2020-01-29: Updated to non-nullable reference types, fixed a bunch of fxcop warnings.

#nullable enable

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace netcore_31_template
{
    public class BulkInsertEventArgs<T> : EventArgs
    {

        public IEnumerable<T> Items { get; }
        public DataTable DataTable { get; }

        public BulkInsertEventArgs(T[] items, DataTable dataTable)
        {
            DataTable = dataTable;
            Items = items.ToArray();
        }
    }

    /// <summary>
    /// Performs buffered bulk inserts into a sql server table using objects instead of DataRows. :)
    /// </summary>
    public sealed class BulkInserter<T> : IDisposable where T : class
    {

        public List<string> RemoveColumns { get; }


        public event EventHandler<BulkInsertEventArgs<T>>? PreBulkInsert;
        public void OnPreBulkInsert(BulkInsertEventArgs<T> e)
        {
            PreBulkInsert?.Invoke(this, e);
        }

        public event EventHandler<BulkInsertEventArgs<T>>? PostBulkInsert;
        public void OnPostBulkInsert(BulkInsertEventArgs<T> e)
        {
            PostBulkInsert?.Invoke(this, e);
        }

        private const int DefaultBufferSize = 2000;
        private readonly SqlConnection _connection;

        public int BufferSize { get; }
        public int InsertedCount { get; private set; }
        public TimeSpan? BulkCopyTimeout { get; set; }

        private readonly Lazy<Dictionary<string, Func<T, object>>> _props =
            new Lazy<Dictionary<string, Func<T, object>>>(GetPropertyInformation);

        private readonly Lazy<DataTable> _dt;

        private readonly bool _constructedSqlBulkCopy;
        private readonly SqlBulkCopy _sbc;
        private readonly List<T> _queue = new List<T>();
        private readonly SqlTransaction? _tran;

        /// <param name="connection">SqlConnection to use for retrieving the schema of sqlBulkCopy.DestinationTableName</param>
        /// <param name="sqlBulkCopy">SqlBulkCopy to use for bulk insert.</param>
        /// <param name="bufferSize">Number of rows to bulk insert at a time. The default is 2000.</param>
        public BulkInserter(SqlConnection connection, SqlBulkCopy sqlBulkCopy, int bufferSize = DefaultBufferSize)
        {
            _connection = connection;
            _sbc = sqlBulkCopy;
            _dt = new Lazy<DataTable>(CreateDataTable);
            BufferSize = bufferSize;
            RemoveColumns = new List<string>();
        }

        /// <param name="connection">SqlConnection to use for retrieving the schema of sqlBulkCopy.DestinationTableName and for bulk insert.</param>
        /// <param name="tableName">The name of the table that rows will be inserted into.</param>
        /// <param name="bufferSize">Number of rows to bulk insert at a time. The default is 2000.</param>
        /// <param name="copyOptions">Options for SqlBulkCopy.</param>
        /// <param name="sqlTransaction">SqlTransaction for SqlBulkCopy</param>
        public BulkInserter(SqlConnection connection,
                            string tableName,
                            int bufferSize = DefaultBufferSize,
                            SqlBulkCopyOptions copyOptions = SqlBulkCopyOptions.Default,
                            SqlTransaction? sqlTransaction = null)
            : this(connection, new SqlBulkCopy(connection, copyOptions, sqlTransaction) { DestinationTableName = tableName }, bufferSize)
        {
            _tran = sqlTransaction;
            _constructedSqlBulkCopy = true; //signal that we're responsible for cleaning up the mess we've made
        }

        /// <summary>
        /// Performs buffered bulk insert of enumerable items.
        /// </summary>
        /// <param name="items">The items to be inserted.</param>
        public async Task InsertAsync(IEnumerable<T> items)
        {
            if (items == null) throw new ArgumentNullException(nameof(items));

            // get columns that have a matching property
            var cols = _dt.Value.Columns.Cast<DataColumn>()
                .Where(x => _props.Value.ContainsKey(x.ColumnName))
                .Select(x => new { Column = x, Getter = _props.Value[x.ColumnName] })
                .Where(x => x.Getter != null)
                .ToArray();

            foreach (var buffer in Buffer(items, BufferSize).Select(x => x.ToArray()))
            {
                foreach (var item in buffer)
                {
                    var row = _dt.Value.NewRow();

                    foreach (var col in cols)
                    {
                        row[col.Column] = col.Getter(item) ?? DBNull.Value;
                    }

                    _dt.Value.Rows.Add(row);
                }

                var bulkInsertEventArgs = new BulkInsertEventArgs<T>(buffer, _dt.Value);
                OnPreBulkInsert(bulkInsertEventArgs);

                if (BulkCopyTimeout.HasValue)
                {
                    _sbc.BulkCopyTimeout = (int)BulkCopyTimeout.Value.TotalSeconds;
                }

                await _sbc.WriteToServerAsync(_dt.Value).ConfigureAwait(false);

                OnPostBulkInsert(bulkInsertEventArgs);

                InsertedCount += _dt.Value.Rows.Count;
                _dt.Value.Clear();
            }
        }

        private static IEnumerable<IEnumerable<T>> Buffer(IEnumerable<T> source, int bufferSize)
        {
            using var enumerator = source.GetEnumerator();
            while (enumerator.MoveNext())
            {
                yield return YieldBufferElements(enumerator, bufferSize - 1);
            }
        }

        private static IEnumerable<T> YieldBufferElements(IEnumerator<T> source, int bufferSize)
        {
            yield return source.Current;
            for (var i = 0; i < bufferSize && source.MoveNext(); i++)
            {
                yield return source.Current;
            }
        }

        /// <summary>
        /// Queues a single item for bulk insert. When the queue count reaches the buffer size, bulk insert will happen.
        /// Call Flush() to manually bulk insert the currently queued items.
        /// </summary>
        /// <param name="item">The item to be inserted.</param>
        public Task InsertAsync(T item)
        {
            if (item == null) throw new ArgumentNullException(nameof(item));

            _queue.Add(item);

            if (_queue.Count == BufferSize)
            {
                return FlushAsync();
            }
            else
            {
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Bulk inserts the currently queued items.
        /// </summary>
        public async Task FlushAsync()
        {
            await InsertAsync(_queue).ConfigureAwait(false);
            _queue.Clear();
        }

        /// <summary>
        /// Sets the InsertedCount property to zero.
        /// </summary>
        public void ResetInsertedCount()
        {
            InsertedCount = 0;
        }

        private static Dictionary<string, Func<T, object>> GetPropertyInformation()
        {
            return typeof(T).GetProperties().ToDictionary(x => x.Name, CreatePropertyGetter);
        }

        private static Func<T, object> CreatePropertyGetter(PropertyInfo propertyInfo)
        {
            if (typeof(T) != propertyInfo.DeclaringType)
                throw new ArgumentException(
                    $"Mismatched types between that requested ({typeof(T).Name}), and that returned by the property ({propertyInfo.DeclaringType?.Name}).",
                    propertyInfo.Name);

            var instance = Expression.Parameter(propertyInfo.DeclaringType, "i");
            var property = Expression.Property(instance, propertyInfo);
            var convert = Expression.TypeAs(property, typeof(object));
            return (Func<T, object>)Expression.Lambda(convert, instance).Compile();
        }

        /// <summary>
        /// Creates and returns a datatable representing the target columns we need to insert data into.
        /// </summary>
        /// <remarks>This does NOT contains columns for anything in the `RemoveColumns` collection.</remarks>
        /// <returns></returns>
        private DataTable CreateDataTable()
        {
            var dt = new DataTable();

            // get the target shape of the table
            using var cmd = _connection.CreateCommand();
            cmd.Transaction = _tran;
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
            cmd.CommandText = $"select top 0 * from {_sbc.DestinationTableName}";
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

            using var reader = cmd.ExecuteReader();
            dt.Load(reader);


            foreach (var col in RemoveColumns)
            {
                dt.Columns.Remove(col);
            }

            return dt;
        }

        public void Dispose()
        {
            if (_constructedSqlBulkCopy)
                using (_sbc) _sbc.Close();
        }
    }
}
