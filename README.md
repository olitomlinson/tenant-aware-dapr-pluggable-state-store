A limited, proof of concept, demonstrating a customised Dapr pluggable state store, written in c# aspet .net 6.0, backed by PostgreSQL. 

Customised to support tenant-aware behaviours, such as 'Schema-per-Tenant' and 'Table-per-Tenant''

### Working capabilities

- Standard state store behaviors (CRUD)

### Customisations

- Tenant-aware operations ('Schema-per-Tenant', 'Table-per-Tenant'). See below instructions to run 'Schema-per-Tenant' example.

This enables a client to specify a `tenantId` as part of the `metadata` on each State Store operation, which will dynamically prefix the `Schema`, or `Table` with the given `tenantId`, allowing the logical separation of data in a multi-tenant environment.

### Todo

- Etag support (blocked by https://github.com/dapr/dapr/issues/5520)
- Transactional State Store support
- Properly utilise JSONP in `value` db field

## Run the pluggable component in Dapr stand-alone mode

### Prerequisite

- Obtain an instance of a postgresdb (With a user named 'myusername' with sufficient permissions to create schemas and tables in the db)
- Install Dapr CLI and ensure https://docs.dapr.io/getting-started/install-dapr-selfhost/ Dapr works.
- Pull this repo and open Terminal in the `Component` folder

#### Instructions (Schema-per-Tenant)

1. `dotnet run Component.csproj`

2. create a new `component.yaml` for the pluggable component and place it the default directory where Dapr discovers your components on your machine. Replace with your connection string to your postgresql db instance.

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: prod-mystore
spec:
  type: state.my-component
  version: v1
  metadata:
  - name: connectionString
    value: "<REPLACE-WITH-YOUR-CONNECTION-STRING>"
  - name: tenant
    value: schema
```
3. `dapr run --app-id myapp --dapr-http-port 3500`

4. Persist a value against a key

`POST http://localhost:3500/v1.0/state/prod-mystore`

```json
[{
	"key": "1",
	"value": { 
		"name" : "Dave Mustaine" 
	},
	"metadata": { 
		"tenantId" : "123" 
	}
},{
	"key": "2",
	"value": { 
		"name" : "Kirk Hammett" 
	},
	"metadata": { 
		"tenantId" : "123"
	}
}]
```
5. Observe a new Scehma in your posgresql database has been created called `"123-public"`. Observe the persisted Key Value persisted in the `"state"` Table

<img width="702" alt="image" src="https://user-images.githubusercontent.com/4224880/202821328-95b9f1d6-49a3-431d-bd48-d673178a1f8f.png">

```
