using Dapr.Client.Autogen.Grpc.v1;
using Google.Protobuf.Collections;
using Npgsql;

namespace Helpers
{
    public class StateStoreInitHelper
    {
        private const string TABLE_KEYWORD = "table";
        private const string SCHEMA_KEYWORD = "schema";
        private const string TENANT_KEYWORD = "tenant";
        private const string CONNECTION_STRING_KEYWORD = "connectionString";
        private const string DEFAULT_TABLE_NAME = "state";
        private const string DEFAULT_SCHEMA_NAME = "public";
        private IPgsqlFactory _pgsqlFactory;
        public Func<MapField<string, string>, NpgsqlConnection,ILogger, Pgsql>? TenantAwareDatabaseFactory { get; private set; }

        private string _connectionString;
        
        public StateStoreInitHelper(IPgsqlFactory pgsqlFactory){
            _pgsqlFactory = pgsqlFactory;
            TenantAwareDatabaseFactory = (_,_,_) => { throw new InvalidOperationException("Call 'InitAsync' first"); };
        }

        public async Task<(Func<MapField<string,string>, Pgsql>, NpgsqlConnection)> GetDbFactory(ILogger logger)
        {
            var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();
            Func<MapField<string,string>,Pgsql> factory = null;

            factory = (metadata) => {

                return TenantAwareDatabaseFactory(metadata, connection, logger);
            };   
            return (factory, connection);
        }

        public async Task InitAsync(MetadataRequest componentMetadata){
            
            (var isTenantAware, var tenantTarget) = IsTenantAware(componentMetadata.Properties);        
            
            _connectionString = GetConnectionString(componentMetadata.Properties);

            var defaultSchema = GetDefaultSchemaName(componentMetadata.Properties);

            string defaultTable = GetDefaultTableName(componentMetadata.Properties);  

            TenantAwareDatabaseFactory = 
                (operationMetadata, connection, logger) => {
                    /* 
                        Why is this a func? 
                        Schema and Table are not known until a state operation is requested, 
                        as we rely on a combination on the component metadata and operation metadata,
                    */

                    if (!isTenantAware)
                        return _pgsqlFactory.Create(
                            defaultSchema, 
                            defaultTable, 
                            connection, 
                            logger);
                    
                    var tenantId = GetTenantIdFromMetadata(operationMetadata);

                    
                    switch(tenantTarget){
                        case SCHEMA_KEYWORD :
                            return _pgsqlFactory.Create(
                                schema:             $"{tenantId}-{defaultSchema}", 
                                table:              defaultTable, 
                                connection, 
                                logger); 
                        case TABLE_KEYWORD : 
                            return _pgsqlFactory.Create(
                                schema:             defaultSchema, 
                                table:              $"{tenantId}-{defaultTable}",
                                connection, 
                                logger);
                        default:
                            throw new Exception("Couldn't instanciate the correct tenant-aware Pgsql wrapper");
                    }
                };
        }

        private (bool,string) IsTenantAware(MapField<string,string> properties){
            bool isTenantAware = (properties.TryGetValue(TENANT_KEYWORD, out string tenantTarget));
            if (isTenantAware && !(new string[]{ SCHEMA_KEYWORD, TABLE_KEYWORD }.Contains(tenantTarget)))
                throw new Exception($"Unsupported 'tenant' property value of '{tenantTarget}'. Use 'schema' or 'table' instead");
            
            return (isTenantAware, tenantTarget);
        }

        private string GetDefaultSchemaName(MapField<string,string> properties){
            if (!properties.TryGetValue(SCHEMA_KEYWORD, out string defaultSchema))
                defaultSchema = DEFAULT_SCHEMA_NAME;
            return defaultSchema;
        }

        private string GetDefaultTableName(MapField<string,string> properties){
           if (!properties.TryGetValue(TABLE_KEYWORD, out string defaultTable))
                defaultTable = DEFAULT_TABLE_NAME;
            return defaultTable;
        }

        private string GetConnectionString(MapField<string,string> properties){
            if (!properties.TryGetValue(CONNECTION_STRING_KEYWORD, out string connectionString))
                throw new ArgumentException($"Mandatory component metadata property '{CONNECTION_STRING_KEYWORD}' is not set");
            return connectionString;
        }

        private string GetTenantIdFromMetadata(MapField<string, string> operationMetadata){
            operationMetadata.TryGetValue("tenantId", out string tenantId);   
            if (String.IsNullOrEmpty(tenantId))
                throw new ArgumentException("'metadata.tenantId' value is not specified");
            return tenantId;
        }
    }
}
