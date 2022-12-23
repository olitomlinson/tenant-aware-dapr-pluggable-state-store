using Npgsql;

namespace Helpers
{
    public class Pgsql
    {
        private readonly string _SafeSchema;
        private readonly string _SafeTable;
        private readonly string _schema;
        private readonly string _table;
        private ILogger _logger;
        private readonly NpgsqlConnection _connection;

        public Pgsql(string schema, string table, NpgsqlConnection connection, ILogger logger)
        {
            if (string.IsNullOrEmpty(schema))
                throw new ArgumentException("'schema' is not set");
            _SafeSchema = Safe(schema);
            _schema = schema;

            if (string.IsNullOrEmpty(table))
                throw new ArgumentException("'table' is not set");
            _SafeTable = Safe(table);
            _table = table;

            _logger = logger;

            _connection = connection;
        }

        public async Task CreateSchemaIfNotExistsAsync(NpgsqlTransaction transaction = null)
        {
            var sql = 
                @$"CREATE SCHEMA IF NOT EXISTS {_SafeSchema} 
                AUTHORIZATION myusername;
                CREATE EXTENSION IF NOT EXISTS ""uuid-ossp"";";
            
            _logger.LogDebug($"CreateSchemaAsync - {sql}");
            
            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            await cmd.ExecuteNonQueryAsync();

            _logger.LogDebug($"Schema Created : [{_SafeSchema}]");
        }
        
        public async Task CreateTableIfNotExistsAsync(NpgsqlTransaction transaction = null)
        {
            var sql = 
                @$"CREATE TABLE IF NOT EXISTS {SchemaAndTable} 
                ( 
                    key text NOT NULL PRIMARY KEY COLLATE pg_catalog.""default"" 
                    ,value text
                    ,etag text
                    ,insertdate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                    ,updatedate TIMESTAMP WITH TIME ZONE NULL
                ) 
                TABLESPACE pg_default; 
                ALTER TABLE IF EXISTS {SchemaAndTable} OWNER to myusername;";

            _logger.LogDebug($"CreateTableAsync - {sql}");

            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            await cmd.ExecuteNonQueryAsync();

            _logger.LogDebug($"Table Created : [{SchemaAndTable}]");
        }


        private string Safe(string input)
        {
            // Postgres needs any object starting with a non-alpha to be wrapped in double-qoutes
            return $"\"{input}\"";
        }

        public string SchemaAndTable 
        { 
            get {
                return $"{_SafeSchema}.{_SafeTable}";
            }
        }

        public async Task<Tuple<string,string>> GetAsync(string key, NpgsqlTransaction transaction = null)
        {
            string value = "";
            string etag = "";
            string sql = 
                @$"SELECT 
                    key
                    ,value
                    ,etag
                    
                FROM {SchemaAndTable} 
                WHERE 
                    key = (@key)";

            _logger.LogInformation($"GetAsync:  key: [{key}], value: [{value}], sql: [{sql}]");

            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            {
                cmd.Parameters.AddWithValue("key", key);
                await using (var reader = await cmd.ExecuteReaderAsync())
                while (await reader.ReadAsync())
                {
                    value = reader.GetString(1);
                    etag = reader.GetString(2);
                    _logger.LogDebug("key: {0}, value: {1}, etag : {2}", reader.GetString(0), value, etag);
                    return new Tuple<string,string>(value, etag);
                }
            }
            return new Tuple<string,string>(null,null);
        }

        public async Task UpsertAsync(string key, string value, string etag, NpgsqlTransaction transaction = null)
        {
    
            // this is an optimisation, which I will probably remove when I eventually support First-Write-Wins.
            if (string.IsNullOrEmpty(etag))
            {
                await CreateSchemaIfNotExistsAsync(transaction); 
                await CreateTableIfNotExistsAsync(transaction); 
            }

            await InsertOrUpdateAsync(key, value, etag, transaction);
        }

        public async Task InsertOrUpdateAsync(string key, string value, string etag, NpgsqlTransaction transaction = null)
        {
      
            var query = @$"INSERT INTO {SchemaAndTable} 
            (
                key
                ,value
                ,etag
            ) 
            VALUES 
            (
                @1 
                ,@2
                ,uuid_generate_v4()::text
            )
            ON CONFLICT (key)
            DO
            UPDATE SET 
                value = @2
                ,updatedate = NOW()
                ,etag = uuid_generate_v4()::text
            WHERE {SchemaAndTable}.etag = @3
            ;";
            

            _logger.LogDebug($"InsertOrUpdateAsync : key: [{key}], value: [{value}], sql: [{query}]");

            await using (var cmd = new NpgsqlCommand(query, _connection, transaction))
            {
                cmd.Parameters.AddWithValue("1", NpgsqlTypes.NpgsqlDbType.Text, key);
                cmd.Parameters.AddWithValue("2", NpgsqlTypes.NpgsqlDbType.Jsonb, value);
                cmd.Parameters.AddWithValue("3", NpgsqlTypes.NpgsqlDbType.Text, etag);
                var rowsAffected = await cmd.ExecuteNonQueryAsync();

                if (!string.IsNullOrEmpty(etag) && rowsAffected == 0)
                    throw new Exception("Etag mismatch");
            }
            _logger.LogDebug($"Row inserted/updated");
        }

        public async Task DeleteRowAsync(string key, string etag, NpgsqlTransaction transaction = null)
        {
            // TODO this is vulenerable to sql-injection as-is, need to try converting to a proc because
            // you can't use parameters in code blocks like below.
            var sql = @$"
            DO $$
            BEGIN 
                IF EXISTS
                    ( SELECT 1
                    FROM   information_schema.tables 
                    WHERE  table_schema = '{_schema}'
                    AND    table_name = '{_table}'
                    )
                THEN
                    DELETE FROM {SchemaAndTable}
                    WHERE 
                        key = '{key}'
                        AND
                        etag = '{etag}';
                END IF;
            END
            $$;";

            _logger.LogDebug($"DeleteRowAsync: key: [{key}], etag: [{etag}], sql: [{sql}]");

            await using (var cmd = new NpgsqlCommand(sql, _connection, transaction))
            {
                var rowsDeleted = await cmd.ExecuteNonQueryAsync();
                if (rowsDeleted == 0 && !string.IsNullOrEmpty(etag))
                    throw new Exception("Etag mismatch");
            }
        }
    }
}


public static class PostgresExtensions{
    public static bool TableDoesNotExist(this PostgresException ex){
        return (ex.SqlState == "42P01");
    }
    public static bool AnyErrorExcludingTableDoesNotExist(this PostgresException ex){
        return !TableDoesNotExist(ex);
    }
    public static bool SchemaDoesNotExist(this PostgresException ex){
        return (ex.SqlState == "42P06");
    }
     public static bool AnyErrorExcludingSchemaDoesNotExist(this PostgresException ex){
        return (!SchemaDoesNotExist(ex));
    }
}
