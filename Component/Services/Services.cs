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
        _logger.LogInformation("ctor");
        _stateStoreInitHelper = stateStoreInitHelper;
    }

    public override async Task<SetResponse> Set(SetRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Set");
        throw new Exception("'Set' is not implemented");
        // TODO 
    }

    public override async Task<PingResponse> Ping(PingRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Ping");
        // TODO 
        return new PingResponse();
    }

    public override async Task<FeaturesResponse> Features(FeaturesRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Features");
        var response = new FeaturesResponse();
        response.Features.Add("TRANSACTIONAL");
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

        (var dbfactory, var conn, _) = await _stateStoreInitHelper.GetDbFactory(_logger);
        using (conn)
        {
            string value = "";
            bool notFound = false;

            try 
            {
                value = await dbfactory(request.Metadata).GetAsync(request.Key);
                if (value == null)
                    notFound = true;
            } 
            catch(PostgresException ex) when (ex.TableDoesNotExist())
            {
                notFound = true;
            }

            if (notFound)
                return new GetResponse();

            return new GetResponse(){ Data = Google.Protobuf.ByteString.CopyFromUtf8(value)};       
        }
    }

    public override async Task<BulkSetResponse> BulkSet(BulkSetRequest request, ServerCallContext context)
    {
        _logger.LogInformation($"BulkSet - {request.Items.Count} items");
                
        (var dbfactory, var conn, _) = await _stateStoreInitHelper.GetDbFactory(_logger);
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
                    
                    
                    
                    // this only works by accident because of postgres implicit transactions, need to find a way to pass down the new tran to make it explict
                    tran = await conn.BeginTransactionAsync();
                    await dbfactory(item.Metadata).UpsertAsync(item.Key, strValue, item.Etag?.Value ?? String.Empty);   
                    await tran.CommitAsync();         
                }
            }
            catch
            {
                await tran.RollbackAsync();
            }
        }   
        return new BulkSetResponse();
    }


    public override async Task<DeleteResponse> Delete(DeleteRequest request, ServerCallContext context)
    {
        _logger.LogInformation("Delete");
        
        (var dbfactory, var conn, var tran) = await _stateStoreInitHelper.GetDbFactory(_logger, true);
        using (conn)
        {
            try 
            {
                await dbfactory(request.Metadata).DeleteRowAsync(request.Key);
            }
            catch
            {
                await tran.RollbackAsync();
            }
            await tran.CommitAsync();
        }
        return new DeleteResponse();
    }
}