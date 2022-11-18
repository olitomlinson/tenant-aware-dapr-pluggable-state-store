// ------------------------------------------------------------------------
// Copyright 2022 The Dapr Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ------------------------------------------------------------------------

// Uncomment to import Dapr proto components namespace.
using Dapr.Client.Autogen.Grpc.v1;
using Dapr.Proto.Components.V1;
using Grpc.Core;
using Helpers;
using Npgsql;

namespace DaprComponents.Services;

public class StateStoreService : StateStore.StateStoreBase{
    private readonly ILogger<StateStoreService> _logger;

    private StateStoreInitHelper _stateStoreInitHelper;
    public StateStoreService(ILogger<StateStoreService> logger, StateStoreInitHelper stateStoreInitHelper){
        _logger = logger;
        _logger.LogInformation("ctor");
        _stateStoreInitHelper = stateStoreInitHelper;
    }

    public override async Task<SetResponse> Set(SetRequest request, ServerCallContext context){
        _logger.LogDebug("set");
        throw new Exception("'Set' is not implemented");
        // TODO 
    }

    public override async Task<PingResponse> Ping(PingRequest request, ServerCallContext context){
        _logger.LogDebug("ping");
        // TODO 
        return new PingResponse();
    }

    public override async Task<FeaturesResponse> Features(FeaturesRequest request, ServerCallContext context){
        _logger.LogDebug("features");
        var response = new FeaturesResponse();
        response.Features.Add("TRANSACTIONAL");
        // TODO : Support Etags eventually
        return response;
    }

    public override async Task<InitResponse> Init(InitRequest request, ServerCallContext context){
        _logger.LogInformation("init");

        await _stateStoreInitHelper.InitAsync(request.Metadata);

        return new InitResponse();
    }

    public override async Task<GetResponse> Get(GetRequest request, ServerCallContext context){
        _logger.LogInformation("get");

        var db = _stateStoreInitHelper?.TenantAwareDatabaseHelper?.Invoke(request.Metadata);
        string value = "";
        bool notFound = false;

        try {
            value = await db.GetAsync(request.Key);
            if (value == null)
                notFound = true;
        } 
        catch(PostgresException ex) when (ex.TableDoesNotExist()){
            notFound = true;
        }

        if (notFound)
            return new GetResponse();

        return new GetResponse(){ 
            Data = Google.Protobuf.ByteString.CopyFromUtf8(value)
        };
    }

    public override async Task<BulkSetResponse> BulkSet(BulkSetRequest request, ServerCallContext context){
        _logger.LogInformation($"bulkset - {request.Items.Count} items");
        foreach(var item in request.Items){
            var db = _stateStoreInitHelper?.TenantAwareDatabaseHelper?.Invoke(item.Metadata);

            // TODO : Need to implement 'something' here with regards to 'isBinary',
            // but I do not know what this is trying to achieve. See existing pgSQL built-in component 
            // https://github.com/dapr/components-contrib/blob/d3662118105a1d8926f0d7b598c8b19cd9dc1ccf/state/postgresql/postgresdbaccess.go#L135
            var strValue = item.Value.ToString(System.Text.Encoding.UTF8);      

            bool done = false;
            int attempt = 0;
            while(!done){
                /* 
                why the loop?! - It's just a super naive way to create the schemas and tables if they don't exist
                */
                try{
                    attempt += 1;
                    if (string.IsNullOrEmpty(item.Etag?.Value ?? String.Empty))
                        await db.InsertOrUpdateAsync(item.Key, strValue, attempt);
                    else{
                        throw new NotImplementedException();
                        /* TODO : Need to implement Etag handling but I can't get my head around the 
                           c# equivalent of the XID data type
                           https://github.com/dapr/components-contrib/blob/d3662118105a1d8926f0d7b598c8b19cd9dc1ccf/state/postgresql/postgresdbaccess.go#L158

                           TODO : await databaseHelper.UpdateAsync(item.Key, strValue, item.Etag, attempt);
                        */
                    }
                    done = true;
                }
                catch(PostgresException ex) when (ex.TableDoesNotExist() || ex.SchemaDoesNotExist()){
                    try { 
                        await db.CreateSchemaAsync(); 
                    }
                    catch(PostgresException ex1) when (ex.AnyErrorExcludingSchemaDoesNotExist()) {
                        _logger.LogError(ex1, $"SCHEMA CREATE exception : sqlState = {ex1.SqlState}");
                    }
    
                    try { 
                        await db.CreateTableAsync(); 
                    }
                    catch(PostgresException ex2) when (ex2.AnyErrorExcludingTableDoesNotExist()){
                        _logger.LogError(ex2, $"TABLE CREATE exception : sqlState = {ex2.SqlState}");
                    }
                }

                if (attempt == 3) done = true;
            }
        }
        
        return new BulkSetResponse();
    }


    public override async Task<DeleteResponse> Delete(DeleteRequest request, ServerCallContext context){
        _logger.LogInformation("delete");
        var db = _stateStoreInitHelper?.TenantAwareDatabaseHelper?.Invoke(request.Metadata);
        await db.DeleteRowAsync(request.Key);
        return new DeleteResponse();

        // TODO : what should happen when deleting something that doesnt exist?
        // TODO : Support Etag on delete
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
