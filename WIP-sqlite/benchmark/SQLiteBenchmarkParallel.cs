using Duplicati.Library.Main.Database;
using System.Data;

namespace sqlite_bench
{
    public class SQLiteBenchmarkParallel : IDisposable
    {
        // The SQLite connections
        protected List<IDbConnection> cons = [];
        protected List<IDbTransaction?> transactions = [];

        protected long last_id = -1;

        public SQLiteBenchmarkParallel(Backends backend, int count)
        {
            var data_source = "testdb.sqlite";

            IDbConnection con;

            for (int i = 0; i < count; i++)
            {
                switch (backend)
                {
                    case Backends.DuplicatiSQLite:
                        con = Duplicati.Library.SQLiteHelper.SQLiteLoader.LoadConnection();
                        con.Close();
                        con.ConnectionString = $"Data Source={data_source};cache=shared";
                        con.Open();
                        cons.Add(con);
                        break;
                    case Backends.MicrosoftSQLite:
                        con = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={data_source}");
                        con.Open();
                        cons.Add(con);
                        break;
                    case Backends.SystemSQLite:
                        con = new System.Data.SQLite.SQLiteConnection($"Data Source={data_source}");
                        con.Open();
                        cons.Add(con);
                        break;
                    default:
                        throw new NotImplementedException();
                }

                transactions.Add(null);
            }
        }

        protected IDbCommand CreateCommand(IDbConnection con, string query)
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

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool _)
        {
            for (int i = 0; i < cons.Count; i++)
            {
                var transaction = transactions[i];
                var con = cons[i];

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
        }

        protected void DropRows()
        {
            using IDbCommand cmd = cons[0].CreateCommand();
            cmd.CommandText = SQLQeuriesOriginal.DropAllRows;
            cmd.AddNamedParameter("id", last_id);
            cmd.ExecuteNonQuery();
        }

        protected long GetLastRowId()
        {
            using var cmd = cons[0].CreateCommand();
            cmd.CommandText = SQLQeuriesOriginal.LastRowId;
            return cmd.ExecuteScalarInt64();
        }

        protected void RunNonQueries(IDbConnection con, string[] queries, bool use_transaction)
        {
            using var cmd = con.CreateCommand();
            using var transaction = use_transaction ? con.BeginTransaction() : null;
            foreach (var query in queries)
            {
                cmd.CommandText = query;
                cmd.ExecuteNonQuery(transaction);
            }
            transaction?.Commit();
        }
    }
}