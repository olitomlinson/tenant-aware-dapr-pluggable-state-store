using Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using Google.Protobuf.Collections;
using Dapr.Client.Autogen.Grpc.v1;

namespace Tests;

[TestClass]
public class HappyPathTests
{
    [TestMethod]
    public async Task DefaultSchemaAndNameAreAppliedWhenNotUsingTenancy()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(Substitute.For<ILogger<StateStoreInitHelper>>(), pgsqlFactory);

        var componentMetadata = new MetadataRequest();
        componentMetadata.Properties.Add("connectionString", "some-c-string");
        await h.InitAsync(componentMetadata);

        var operationMetadata = new MapField<string, string>();
        h.TenantAwareDatabaseHelper?.Invoke(operationMetadata);

        pgsqlFactory.Received().Create("public", "state", Arg.Any<string>(), Arg.Any<ILogger>());
    }

    [TestMethod]
    public async Task TenantIdIsPrefixedToDefaultSchemaName()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(Substitute.For<ILogger<StateStoreInitHelper>>(), pgsqlFactory);

        var componentMetadata = new MetadataRequest();
        componentMetadata.Properties.Add("connectionString", "some-c-string");
        componentMetadata.Properties.Add("tenant", "schema");
        await h.InitAsync(componentMetadata);

        var operationMetadata = new MapField<string, string>();
        operationMetadata.Add("tenantId", "123");
        h.TenantAwareDatabaseHelper?.Invoke(operationMetadata);

        pgsqlFactory.Received().Create("123-public", "state", Arg.Any<string>(), Arg.Any<ILogger>());
    }

    [TestMethod]
    public async Task TenantIdIsPrefixedToCustomSchemaName()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(Substitute.For<ILogger<StateStoreInitHelper>>(), pgsqlFactory);

        var componentMetadata = new MetadataRequest();
        componentMetadata.Properties.Add("connectionString", "some-c-string");
        componentMetadata.Properties.Add("tenant", "schema");
        componentMetadata.Properties.Add("schema", "custom");
        await h.InitAsync(componentMetadata);

        var operationMetadata = new MapField<string, string>();
        operationMetadata.Add("tenantId", "123");
        h.TenantAwareDatabaseHelper?.Invoke(operationMetadata);

        pgsqlFactory.Received().Create("123-custom", "state", Arg.Any<string>(), Arg.Any<ILogger>());
    }

    [TestMethod]
    public async Task TenantIdIsPrefixedToDefaultTableName()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(Substitute.For<ILogger<StateStoreInitHelper>>(), pgsqlFactory);

        var componentMetadata = new MetadataRequest();
        componentMetadata.Properties.Add("connectionString", "some-c-string");
        componentMetadata.Properties.Add("tenant", "table");
        await h.InitAsync(componentMetadata);

        var operationMetadata = new MapField<string, string>();
        operationMetadata.Add("tenantId", "123");
        h.TenantAwareDatabaseHelper?.Invoke(operationMetadata);

        pgsqlFactory.Received().Create("public", "123-state", Arg.Any<string>(), Arg.Any<ILogger>());
    }

    [TestMethod]
    public async Task TenantIdIsPrefixedToCustomTableName()
    {
        var pgsqlFactory = Substitute.For<IPgsqlFactory>();
        var h = new StateStoreInitHelper(Substitute.For<ILogger<StateStoreInitHelper>>(), pgsqlFactory);

        var componentMetadata = new MetadataRequest();
        componentMetadata.Properties.Add("connectionString", "some-c-string");
        componentMetadata.Properties.Add("tenant", "table");
        componentMetadata.Properties.Add("table", "custom");
        await h.InitAsync(componentMetadata);

        var operationMetadata = new MapField<string, string>();
        operationMetadata.Add("tenantId", "123");
        h.TenantAwareDatabaseHelper?.Invoke(operationMetadata);

        pgsqlFactory.Received().Create("public", "123-custom", Arg.Any<string>(), Arg.Any<ILogger>());
    }
}