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

public class StateStoreService : StateStore.StateStoreBase
{
    private readonly ILogger<StateStoreService> _logger;
    private StateStoreInitHelper _stateStoreInitHelper;
    public StateStoreService(ILogger<StateStoreService> logger, StateStoreInitHelper stateStoreInitHelper)
    {
        _logger = logger;
        _stateStoreInitHelper = stateStoreInitHelper;
    }

    public override async Task<SetResponse> Set(SetRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Set");
        throw new Exception("'Set' is not implemented");
    }

    public override async Task<PingResponse> Ping(PingRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Ping");
        return new PingResponse();
    }

    public override async Task<FeaturesResponse> Features(FeaturesRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Features");
        var response = new FeaturesResponse();
        response.Features.Add("TRANSACTIONAL");
        response.Features.Add("ETAG");
        return response;
    }

    public override async Task<InitResponse> Init(InitRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Init");

        await _stateStoreInitHelper.InitAsync(request.Metadata);

        return new InitResponse();
    }

    public override async Task<GetResponse> Get(GetRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Get");

        (var dbfactory, var conn) = await _stateStoreInitHelper.GetDbFactory(_logger);
        using (conn)
        {
            string value = "";
            string etag = "";
            bool notFound = false;

            try 
            {
                (value, etag) = await dbfactory(request.Metadata).GetAsync(request.Key);
                if (value == null)
                    notFound = true;
            } 
            catch(PostgresException ex) when (ex.TableDoesNotExist())
            {
                notFound = true;
            }

            if (notFound)
            {
                _logger.LogDebug($"Object not found with key : [{request.Key}]");
                return new GetResponse();
            }

            return new GetResponse(){ 
                Data = Google.Protobuf.ByteString.CopyFromUtf8(value),
                Etag = new Etag() {
                    Value = etag
                }};       
        }
    }

    public override async Task<BulkSetResponse> BulkSet(BulkSetRequest request, ServerCallContext context)
    {
        _logger.LogInformation($"BulkSet - {request.Items.Count} items");
                
        (var dbfactory, var conn) = await _stateStoreInitHelper.GetDbFactory(_logger);
        using (conn)
        {
            NpgsqlTransaction tran = null;
            try
            {
                foreach(var item in request.Items)
                {
                    //var db = _stateStoreInitHelper?.TenantAwareDatabaseFactory?.Invoke(item.Metadata, conn, null);

                    // TODO : Need to implement 'something' here with regards to 'isBinary',
                    // but I do not know what this is trying to achieve. See existing pgSQL built-in component 
                    // https://github.com/dapr/components-contrib/blob/d3662118105a1d8926f0d7b598c8b19cd9dc1ccf/state/postgresql/postgresdbaccess.go#L135
                    var strValue = item.Value.ToString(System.Text.Encoding.UTF8);      
                    tran = await conn.BeginTransactionAsync();
                    await dbfactory(item.Metadata).UpsertAsync(item.Key, strValue, item.Etag?.Value ?? String.Empty, tran);   
                    await tran.CommitAsync();         
                }
            }
            catch(Exception ex)
            {
                await tran.RollbackAsync();

                if (ex.Message == "Etag mismatch")
                    _logger.LogInformation("Etag mismatch");
                else
                    _logger.LogError(ex, "State object could not be inserted/updated");
                throw ex;
            }
        }   
        return new BulkSetResponse();
    }

    public override async Task<BulkGetResponse> BulkGet(BulkGetRequest request, ServerCallContext context)
    {
        _logger.LogInformation("BulkGet");
        var response = new BulkGetResponse();

        (var dbfactory, var conn) = await _stateStoreInitHelper.GetDbFactory(_logger);
        using (conn)
        {
            string value = "";
            string etag = "";
            bool notFound = false;

            foreach(var item in request.Items)
            {
                try 
                {
                    (value, etag) = await dbfactory(item.Metadata).GetAsync(item.Key);
                    if (value == null)
                        notFound = true;
                } 
                catch(PostgresException ex) when (ex.TableDoesNotExist())
                {
                    notFound = true;
                }

                if (notFound){
                    _logger.LogDebug($"Object not found with key : [{item.Key}]");
                    continue;
                }

                response.Items.Add( 
                    new BulkStateItem(){
                        Key = item.Key,
                        Data = Google.Protobuf.ByteString.CopyFromUtf8(value),
                        Etag = new Etag() {
                            Value = etag
                        }
                    }
                );
            }      
        }

        response.Got = true;

        return response;
    }


    public override async Task<DeleteResponse> Delete(DeleteRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Delete");
        
        (var dbfactory, var conn) = await _stateStoreInitHelper.GetDbFactory(_logger);
        using (conn)
        {
            var tran = await conn.BeginTransactionAsync();
            try 
            {
                await dbfactory(request.Metadata).DeleteRowAsync(request.Key, request.Etag?.Value ?? String.Empty, tran);
            }
            catch(Exception ex)
            {   
                await tran.RollbackAsync();

                if (ex.Message == "Etag mismatch")
                    _logger.LogInformation("Etag mismatch");
                else
                    _logger.LogError(ex, "State object could not be deleted");
                throw ex;
            }
            await tran.CommitAsync();
        }
        return new DeleteResponse();
    }
}