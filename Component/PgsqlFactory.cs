namespace Helpers
{
    public interface IPgsqlFactory {
        Pgsql Create(string schema, string table, string connectionString, ILogger logger);
    }
    public class PgsqlFactory : IPgsqlFactory
    {
        public Pgsql Create(string schema, string table, string connectionString, ILogger logger)
        {
            return new Pgsql(schema, table, connectionString, logger);
        }
    }
}
