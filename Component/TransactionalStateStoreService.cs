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
using Dapr.Proto.Components.V1;
using Grpc.Core;

namespace Helpers;

public class TransactionalStateStoreService : TransactionalStateStore.TransactionalStateStoreBase
{
    private readonly ILogger<TransactionalStateStoreService> _logger;
    private readonly StateStoreInitHelper _initHelper;

    public TransactionalStateStoreService(ILogger<TransactionalStateStoreService> logger, StateStoreInitHelper initHelper)
    {
        
        _logger = logger;
        _logger.LogInformation("transact-ctor");
        _initHelper = initHelper;
    }

    public override async Task<TransactionalStateResponse> Transact(TransactionalStateRequest request, ServerCallContext context)
    {
        var db = _initHelper.TenantAwareDatabaseHelper?.Invoke(request.Metadata);

        foreach(var op in request.Operations)
        {
            switch (op.RequestCase)
            {
                case TransactionalStateOperation.RequestOneofCase.Set : 
                {
                    _logger.LogInformation("transact - set");
                    // TODO : Need to implement 'something' here with regards to 'isBinary',
                    // but I do not know what this is trying to achieve. See existing pgSQL built-in component 
                    // https://github.com/dapr/components-contrib/blob/d3662118105a1d8926f0d7b598c8b19cd9dc1ccf/state/postgresql/postgresdbaccess.go#L135
                    var strValue = op.Set.Value.ToString(System.Text.Encoding.UTF8);

                    await db.UpsertAsync(op.Set.Key, strValue, op.Set.Etag?.Value ?? String.Empty); 
                    continue;
                }
                case TransactionalStateOperation.RequestOneofCase.Delete :
                {
                    _logger.LogInformation("transact - del");
                    
                    await db.DeleteRowAsync(op.Delete.Key);
                    continue;
                }
                case TransactionalStateOperation.RequestOneofCase.None : 
                    throw new Exception("transact - operation Not Set");
            }
        }
        return new TransactionalStateResponse();
    }
}
