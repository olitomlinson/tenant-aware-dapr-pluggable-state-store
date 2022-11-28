using Npgsql;

namespace Helpers
{
    public interface IPgsqlFactory
    {
        Pgsql Create(string schema, string table, NpgsqlConnection connection, NpgsqlTransaction transaction, ILogger logger);
    }
    public class PgsqlFactory : IPgsqlFactory
    {
        public Pgsql Create(string schema, string table, NpgsqlConnection connection, NpgsqlTransaction transaction, ILogger logger)
        {
            return new Pgsql(schema, table, connection, transaction, logger);
        }
    }
}
