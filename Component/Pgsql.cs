using Npgsql;

namespace Helpers
{
    public class Pgsql{
        private readonly string _schema;
        private readonly string _table;
        private readonly string _connectionString;
        private ILogger _logger;
        public Pgsql(string schema, string table, string connectionString, ILogger logger){
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentException("'connectionString' is not set");
            _connectionString = connectionString;

            if (string.IsNullOrEmpty(schema))
                throw new ArgumentException("'schema' is not set");
            _schema = Safe(schema);

            if (string.IsNullOrEmpty(table))
                throw new ArgumentException("'table' is not set");
            _table = Safe(table);
            
            _logger = logger;
        }

        public async Task CreateSchemaAsync(){
            var sql = 
                @$"CREATE SCHEMA {_schema} 
                AUTHORIZATION myusername;";
            
            _logger.LogDebug($"CreateSchemaAsync - {sql}");
            
            await using var dataSource = NpgsqlDataSource.Create(_connectionString);
            await using (var cmd = dataSource.CreateCommand(sql)) 
            await cmd.ExecuteNonQueryAsync();

            _logger.LogDebug($"Schema Created : [{_schema}]");
        }
        
        public async Task CreateTableAsync(){
            var sql = 
                @$"CREATE TABLE {SchemaAndTable} 
                ( 
                    key text NOT NULL PRIMARY KEY COLLATE pg_catalog.""default"" 
                    ,value jsonb
                    ,insertdate TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                    ,updatedate TIMESTAMP WITH TIME ZONE NULL
                ) 
                TABLESPACE pg_default; 
                ALTER TABLE IF EXISTS {SchemaAndTable} OWNER to myusername;";

            _logger.LogDebug($"CreateTableAsync - {sql}");

            await using var dataSource = NpgsqlDataSource.Create(_connectionString);
            await using (var cmd = dataSource.CreateCommand(sql)) 
            await cmd.ExecuteNonQueryAsync();

            _logger.LogDebug($"Table Created : [{SchemaAndTable}]");
        }


        private string Safe(string input){
            // Postgres needs any object starting with a non-alpha to be wrapped in double-qoutes
            return $"\"{input}\"";
        }

        public string SchemaAndTable { 
            get {
                return $"{_schema}.{_table}";
            }
        }

        public async Task<string> GetAsync(string key){

            string value = "";
            string sql = 
                @$"SELECT 
                    key
                    ,value 
                FROM {SchemaAndTable} 
                WHERE 
                    key = (@key)";

            await using var dataSource = NpgsqlDataSource.Create(_connectionString);
            await using (var cmd = dataSource.CreateCommand(sql)) {
                cmd.Parameters.AddWithValue("key", key);
                await using (var reader = await cmd.ExecuteReaderAsync())
                while (await reader.ReadAsync()){
                    value = reader.GetString(1);
                    _logger.LogDebug("key: {0}, value: {1}", reader.GetString(0), value);
                    return value;
                }
            }
            return null;
        }

        public async Task InsertOrUpdateAsync(string key, string value, int attempt){
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

            await using var dataSource = NpgsqlDataSource.Create(_connectionString);
            await using (var cmd = dataSource.CreateCommand(sql)){
                cmd.Parameters.AddWithValue("1", NpgsqlTypes.NpgsqlDbType.Text, key);
                cmd.Parameters.AddWithValue("2", NpgsqlTypes.NpgsqlDbType.Jsonb, value);
                _logger.LogDebug($"INSERT attempt {attempt}");
                await cmd.ExecuteNonQueryAsync();
            }
            _logger.LogDebug($"Row inserted/updated");
        }

        public async Task DeleteRowAsync(string key){
            var sql = 
                @$"DELETE FROM {SchemaAndTable}
	            WHERE key = @1;";

            await using var dataSource = NpgsqlDataSource.Create(_connectionString);
            await using (var cmd = dataSource.CreateCommand(sql)){
                cmd.Parameters.AddWithValue("1", NpgsqlTypes.NpgsqlDbType.Text, key);
                await cmd.ExecuteNonQueryAsync();
            }

            _logger.LogDebug($"key deleted : [{key}]");
        }
    }
}