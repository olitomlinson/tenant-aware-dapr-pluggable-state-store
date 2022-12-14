using Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using Google.Protobuf.Collections;
using Dapr.Client.Autogen.Grpc.v1;
using Npgsql;

namespace Tests;

[TestClass]
public class HappyPathTests
{
    [TestMethod]
    public async Task DefaultSchemaAndNameAreAppliedWhenNotUsingTenancy()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(pgsqlFactory);

        var componentMetadata = new MetadataRequest();
        componentMetadata.Properties.Add("connectionString", "some-c-string");
        await h.InitAsync(componentMetadata);

        var operationMetadata = new MapField<string, string>();
        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null, null);

        pgsqlFactory.Received().Create("public", "state", null, Arg.Any<ILogger>());
    }

    [TestMethod]
    public async Task TenantIdIsPrefixedToDefaultSchemaName()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(pgsqlFactory);

        var componentMetadata = new MetadataRequest();
        componentMetadata.Properties.Add("connectionString", "some-c-string");
        componentMetadata.Properties.Add("tenant", "schema");
        await h.InitAsync(componentMetadata);

        var operationMetadata = new MapField<string, string>();
        operationMetadata.Add("tenantId", "123");
        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null, null);

        pgsqlFactory.Received().Create("123-public", "state", null, Arg.Any<ILogger>());
    }

    [TestMethod]
    public async Task TenantIdIsPrefixedToCustomSchemaName()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(pgsqlFactory);

        var componentMetadata = new MetadataRequest();
        componentMetadata.Properties.Add("connectionString", "some-c-string");
        componentMetadata.Properties.Add("tenant", "schema");
        componentMetadata.Properties.Add("schema", "custom");
        await h.InitAsync(componentMetadata);

        var operationMetadata = new MapField<string, string>();
        operationMetadata.Add("tenantId", "123");
        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null, null);

        pgsqlFactory.Received().Create("123-custom", "state", null, Arg.Any<ILogger>());
    }

    [TestMethod]
    public async Task TenantIdIsPrefixedToDefaultTableName()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(pgsqlFactory);

        var componentMetadata = new MetadataRequest();
        componentMetadata.Properties.Add("connectionString", "some-c-string");
        componentMetadata.Properties.Add("tenant", "table");
        await h.InitAsync(componentMetadata);

        var operationMetadata = new MapField<string, string>();
        operationMetadata.Add("tenantId", "123");
        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null, null);

        pgsqlFactory.Received().Create("public", "123-state", null, Arg.Any<ILogger>());
    }

    [TestMethod]
    public async Task TenantIdIsPrefixedToCustomTableName()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(pgsqlFactory);

        var componentMetadata = new MetadataRequest();
        componentMetadata.Properties.Add("connectionString", "some-c-string");
        componentMetadata.Properties.Add("tenant", "table");
        componentMetadata.Properties.Add("table", "custom");
        await h.InitAsync(componentMetadata);

        var operationMetadata = new MapField<string, string>();
        operationMetadata.Add("tenantId", "123");
        h.TenantAwareDatabaseFactory?.Invoke(operationMetadata, null, null);

        pgsqlFactory.Received().Create("public", "123-custom", null, Arg.Any<ILogger>());
    }
}