# sf-nada

This is the sf-nada monorepo

The sf-nada app is an integration between salesforce and the NADA data product solution (see https://docs.knada.io/)

Its default behaviour is to run a work session each morning (~02:00) where it fetches all data records from salesforce
last modified yesterday and post them to corresponding bigquery tables of the NADA solution.

The behaviour can be changed with config in [dev-gcp.yaml](.nais/team-dialog/dev-gcp.yaml) and [prod-gcp.yaml](.nais/team-dialog/prod-gcp.yaml)

The query and mapping for each data product is set up in the map definition files: [dev.json](src/main/resources/mapdef/dev.json)
and [prod.json](src/main/resources/mapdef/prod.json)

Each push to this repository will trigger a deployment for a namespace instance to either dev-gcp or prod-gcp, defined in [main.yml](.github/workflows/main.yml)

You can examine the current state of the app and perform an initial bulk job to transfer all data at these ingresses (naisdevice required):
```
<Configured ingress>/internal/gui
```
For example team-dialog instance:

Dev: https://sf-nada-dialog.intern.dev.nav.no/internal/gui

Prod: https://sf-nada-dialog.intern.nav.no/internal/gui


### Config

You will see the current active config on the examine-ingresses above. They are set by env-variables in [dev-gcp.yaml](.nais/team-dialog/dev-gcp.yaml) and [prod-gcp.yaml](.nais/team-dialog/prod-gcp.yaml)
#### POST_TO_BIGQUERY
Default is true. Whether you will actually send the fetched data to bigquery or not.
#### EXCLUDE_TABLES
Default is ''. A comma seperated list of tables to ignore when fetching data. Used for instance to ignore existing products when performing a data dump on a new one.

### Map definition

The query and mapping for each data product is setup in the map definition files [dev.json](src/main/resources/mapdef/team-dialog/dev.json)
and [prod.json](src/main/resources/mapdef/team-dialog/prod.json).
They are well formed json-objects defined as:
```
{
    "<dataset>": { 
        "<table>": {
            "query": "<query to run in salesforce to fetch all records for product>" 
            "schema": {
                "<field name in salesforce>": {
                    "name": "<column name in bigquery>",
                    "type": "<type in bigquery - one of STRING, INTEGER, DATETIME, DATE or BOOLEAN>"
                 },
                ...
            }
        }
    }
}
```
Note that you can map nested fields in Salesforce. I.e "LiveChatButton.MasterLabel" is a legal field name.

### Gui-ingesses

You can use the gui ingresses (team-dialog examples: https://sf-nada-dialog.dev.intern.nav.no/internal/gui, https://sf-nada-dialog.intern.nav.no/internal/gui) to examine the state of the app in dev and prod. Here you can expand each table
to verify how the current deployed map definition file are being parsed by the app versus metadata from BigQuery. You can also run the query for that table (returning a total count) to see if the fetch from salesforce
goes through successfully, and perform a bulk transfer that in a first step prepares the data in Salesforce and in a second step transfers the data to BigQuery.
If POST_TO_BIGQUERY is false or a table you are looking at is listed in EXCLUDE_TABLES you only get to simulate the transfer of data to BigQuery - no data will be sent.