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
        private NpgsqlTransaction _transaction;

        public Pgsql(string schema, string table, NpgsqlConnection connection, NpgsqlTransaction transaction, ILogger logger)
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

            _transaction = transaction;
        }

        // public async Task<NpgsqlTransaction> BeginTransactionAsync()
        // {
        //     _transaction = await _connection.BeginTransactionAsync();
        //     return _transaction;
        // }

        public async Task CreateSchemaIfNotExistsAsync()
        {
            var sql = 
                @$"CREATE SCHEMA IF NOT EXISTS {_SafeSchema} 
                AUTHORIZATION myusername;";
            
            _logger.LogDebug($"CreateSchemaAsync - {sql}");
            
            await using (var cmd = new NpgsqlCommand(sql, _connection, _transaction))
            await cmd.ExecuteNonQueryAsync();

            _logger.LogDebug($"Schema Created : [{_SafeSchema}]");
        }
        
        public async Task CreateTableIfNotExistsAsync()
        {
            var sql = 
                @$"CREATE TABLE IF NOT EXISTS {SchemaAndTable} 
                ( 
                    key text NOT NULL PRIMARY KEY COLLATE pg_catalog.""default"" 
                    ,value jsonb
                    ,insertdate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                    ,updatedate TIMESTAMP WITH TIME ZONE NULL
                ) 
                TABLESPACE pg_default; 
                ALTER TABLE IF EXISTS {SchemaAndTable} OWNER to myusername;";

            _logger.LogDebug($"CreateTableAsync - {sql}");

            await using (var cmd = new NpgsqlCommand(sql, _connection, _transaction))
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

        public async Task<string> GetAsync(string key)
        {
            string value = "";
            string sql = 
                @$"SELECT 
                    key
                    ,value 
                FROM {SchemaAndTable} 
                WHERE 
                    key = (@key)";

            await using (var cmd = new NpgsqlCommand(sql, _connection, _transaction))
            {
                cmd.Parameters.AddWithValue("key", key);
                await using (var reader = await cmd.ExecuteReaderAsync())
                while (await reader.ReadAsync())
                {
                    value = reader.GetString(1);
                    _logger.LogDebug("key: {0}, value: {1}", reader.GetString(0), value);
                    return value;
                }
            }
            return null;
        }

        public async Task UpsertAsync(string key, string value, string etag)
        {
            bool done = false;
            int attempt = 0;
            while(!done)
            {
                /* 
                why the loop?! - It's just a super naive way to create the schemas and tables if they don't exist
                */
                try
                {
                    attempt += 1;
                    if (string.IsNullOrEmpty(etag))
                    {
                        await CreateSchemaIfNotExistsAsync(); 
                        await CreateTableIfNotExistsAsync(); 
                        await InsertOrUpdateAsync(key,value, attempt);
                    }
                    else
                    {
                        throw new NotImplementedException();
                        /* TODO : Need to implement Etag handling but I can't get my head around the 
                           c# equivalent of the XID data type
                           https://github.com/dapr/components-contrib/blob/d3662118105a1d8926f0d7b598c8b19cd9dc1ccf/state/postgresql/postgresdbaccess.go#L158

                           TODO : await databaseHelper.UpdateAsync(item.Key, strValue, item.Etag, attempt);
                        */
                    }
                    done = true;
                }
                catch(PostgresException ex) when (ex.TableDoesNotExist() || ex.SchemaDoesNotExist())
                {
                    try 
                    { 
                        await CreateSchemaIfNotExistsAsync(); 
                    }
                    catch(PostgresException ex1) when (ex.AnyErrorExcludingSchemaDoesNotExist())
                    {
                        _logger.LogError(ex1, $"SCHEMA CREATE exception : sqlState = {ex1.SqlState}");
                        throw ex;
                    }
    
                    try
                    { 
                        await CreateTableIfNotExistsAsync(); 
                    }
                    catch(PostgresException ex2) when (ex2.AnyErrorExcludingTableDoesNotExist())
                    {
                        _logger.LogError(ex2, $"TABLE CREATE exception : sqlState = {ex2.SqlState}");
                        throw ex;
                    }
                }

                if (attempt == 3) done = true;
            }
        }

        public async Task InsertOrUpdateAsync(string key, string value, int attempt)
        {
            var sql = 
                @$"INSERT INTO {SchemaAndTable} 
                (
                    key
                    ,value
                ) 
                VALUES 
                (
                    @1 
                    ,@2
                )
                ON CONFLICT (key)
                DO
                UPDATE SET 
                    value = @2
                    ,updatedate = NOW()
                ;";

            _logger.LogDebug($"InsertOrUpdateAsync : key: [{key}], value: [{value}], sql: [{sql}]");

            await using (var cmd = new NpgsqlCommand(sql, _connection, _transaction))
            {
                cmd.Parameters.AddWithValue("1", NpgsqlTypes.NpgsqlDbType.Text, key);
                cmd.Parameters.AddWithValue("2", NpgsqlTypes.NpgsqlDbType.Jsonb, value);
                _logger.LogDebug($"INSERT attempt {attempt}");
                await cmd.ExecuteNonQueryAsync();
            }
            _logger.LogDebug($"Row inserted/updated");
        }

        public async Task DeleteRowAsync(string key)
        {
            // TODO this is vulenerable to sql-injection as-is, and needs converting to a proc because you can't use parameters in code blocks.

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
                    WHERE key = '{key}';
                END IF;
            END
            $$;";

            await using (var cmd = new NpgsqlCommand(sql, _connection, _transaction))
            await cmd.ExecuteNonQueryAsync();

            _logger.LogDebug($"key deleted : [{key}]");
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
