using Duplicati.Library.Main.Database;
using System.Data;

namespace sqlite_bench
{
    public class SQLiteBenchmark : IDisposable
    {
        // The SQLite connection
        protected IDbConnection con;

        protected IDbTransaction? transaction;

        protected long last_id = -1;

        public SQLiteBenchmark(Backends backend)
        {
            var data_source = "testdb.sqlite";

            switch (backend)
            {
                case Backends.DuplicatiSQLite:
                    con = Duplicati.Library.SQLiteHelper.SQLiteLoader.LoadConnection();
                    con.Close();
                    con.ConnectionString = $"Data Source={data_source}";
                    con.Open();
                    break;
                //case Backends.MicrosoftSQLite:
                //    con = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={data_source}");
                //    con.Open();
                //    break;
                //case Backends.SystemSQLite:
                //    con = new System.Data.SQLite.SQLiteConnection($"Data Source={data_source}");
                //    con.Open();
                //    break;
                //case Backends.Dictionary:
                //    con = new Dictionary<string, string>();
                //    break;
                default:
                    throw new NotImplementedException();
            }
        }

        protected IDbCommand CreateCommand(string query)
        {
            var cmd = con.CreateCommand();
            cmd.CommandText = query;
            var parameters = query.Split()
                .Where(s => s.Contains('@'))
                .Select(s => new string([.. s.Where(c => char.IsLetterOrDigit(c))]));

            foreach (var param in parameters)
            {
                var parameter = cmd.CreateParameter();
                parameter.ParameterName = param;
                cmd.Parameters.Add(parameter);
            }

            cmd.Prepare();
            return cmd;
        }

        protected void CreateTables(string[] queries)
        {
            using var cmd = con.CreateCommand();
            using var transaction = con.BeginTransaction();
            foreach (var query in queries)
            {
                cmd.CommandText = query;
                cmd.ExecuteNonQuery(transaction);
            }
            transaction.Commit();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool _)
        {
            try
            {
                transaction?.Commit();
            }
            catch (InvalidOperationException)
            {
                // Ignore, transaction already committed
            }
            catch (System.Data.SQLite.SQLiteException)
            {
                // Ignore, transaction already committed
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            transaction?.Dispose();
            con.Close();
            con.Dispose();
        }

        protected void DropRows()
        {
            using IDbCommand cmd = con.CreateCommand();
            cmd.CommandText = SQLQeuriesOriginal.DropAllRows;
            cmd.AddNamedParameter("id", last_id);
            cmd.ExecuteNonQuery();
        }

        protected long GetLastRowId()
        {
            using var cmd = con.CreateCommand();
            cmd.CommandText = SQLQeuriesOriginal.LastRowId;
            return cmd.ExecuteScalarInt64();
        }
    }
}