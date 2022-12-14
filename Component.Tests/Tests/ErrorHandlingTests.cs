using Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using Google.Protobuf.Collections;
using Dapr.Client.Autogen.Grpc.v1;
using Npgsql;

namespace Tests;

[TestClass]
public class ErrorHandlingTests
{
    [TestMethod]
    [ExpectedException(typeof(InvalidOperationException),
    "Call 'InitAsync' firs")]
    public async Task MustCallInitBeforeUsingTheDatabaseHelper()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(pgsqlFactory);

        var operationMetadata = new MapField<string,string>();
        
        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null, null);
        Assert.Fail();
    }

    [TestMethod]
    [ExpectedException(typeof(ArgumentException),
    "Mandatory component metadata property 'connectionString' is not set")]
    public async Task ConnectionStringIsNotSpecified()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(pgsqlFactory);

        var componentMetadata = new MetadataRequest();
        await h.InitAsync(componentMetadata);
    }

    [TestMethod]
    [ExpectedException(typeof(ArgumentException),
    "'metadata.tenantId' value is not specified")]
    public async Task RequestFailsWhenNoTenantIdIsSpecified()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(pgsqlFactory);

        var componentMetadata = new MetadataRequest();
        componentMetadata.Properties.Add("connectionString", "some-c-string");
        componentMetadata.Properties.Add("tenant", "schema");
        await h.InitAsync(componentMetadata);

        var operationMetadata = new MapField<string, string>();
        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null, null);
    }
}