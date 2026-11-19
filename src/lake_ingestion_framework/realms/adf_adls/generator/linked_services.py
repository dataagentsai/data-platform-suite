"""ADF LinkedService builders — parameterised form.

Each LinkedService is built **once per source-type** (or per
type+auth-method where ADF's ``authenticationType`` field forces a
split). Connection details — server, database, username, KV secret
name — are declared as ADF parameters and resolved at activity time
by the Per-Source Driver pipeline, which Lookups
``meta.v_source_config`` and forwards values down through the
dataset reference.

Why per-type instead of per-source?

- A customer with two Postgres databases gets ONE ``LS_POSTGRES``
  artifact, not two. Adding a third Postgres source becomes a
  metadata-only change.
- ADF resolves LS parameters per activity invocation, so two
  concurrent Copy Activities can target different servers via the
  same LS — no contention.

Why split SFTP / REST per auth method?

- ADF's ``authenticationType`` is **not** parameterisable
  (it's an enum field validated at LS-definition time). Basic and
  SSH-Key SFTP need different LSs. Same for REST Basic vs
  Anonymous (bearer).

Per CLAUDE.md ("Secrets via Key Vault only"): the password /
private-key fields always resolve through ``LS_KEY_VAULT``. The KV
``secretName`` is itself a parameter — the LS stores the *reference*
to the secret name, not the secret value, and never sees plaintext.
"""

from __future__ import annotations

from lake_ingestion_framework.realms.adf_adls.generator import naming


def _kv_secret_param_ref(secret_name_param: str) -> dict[str, object]:
    """Reference to a Key-Vault-stored secret whose name is an LS parameter.

    The secret *name* itself is supplied at activity time via the LS
    parameter — that's the parameterisation hop. The secret *value* is
    resolved by ADF inside Key Vault and never touches the JSON.
    """
    return {
        "type": "AzureKeyVaultSecret",
        "store": {
            "referenceName": naming.linked_service_key_vault(),
            "type": "LinkedServiceReference",
        },
        "secretName": {
            "value": f"@{{linkedService().{secret_name_param}}}",
            "type": "Expression",
        },
    }


def build_azure_sql_db_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for Azure SQL DB sources (MSI auth).

    Parameters: ``p_server``, ``p_database``. The connection string
    is templated with these. MSI auth — no credentials in JSON.
    """
    name = naming.linked_service_azure_sql_db()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_server": {"type": "String"},
                "p_database": {"type": "String"},
            },
            "type": "AzureSqlDatabase",
            "typeProperties": {
                "connectionString": (
                    "Server=tcp:@{linkedService().p_server},1433;"
                    "Database=@{linkedService().p_database};"
                    "Encrypt=true;TrustServerCertificate=false;Connection Timeout=30;"
                ),
                "authenticationType": "ManagedIdentity",
            },
        },
    }


def build_adls_gen2_linked_service(*, storage_account: str) -> dict[str, object]:
    """LinkedService for the env's ADLS Gen2 storage account.

    Single-instance per env — storage account is fixed at deploy time
    (it's a customer infrastructure choice, not a per-source dimension).
    Uses MSI for the runtime ADF identity.
    """
    name = naming.linked_service_adls_gen2()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "type": "AzureBlobFS",
            "typeProperties": {
                "url": f"https://{storage_account}.dfs.core.windows.net",
            },
        },
    }


def build_key_vault_linked_service(*, key_vault_uri: str) -> dict[str, object]:
    """LinkedService for the env's Key Vault.

    Single-instance per env. Other LinkedServices reference this one
    when they need to pull a secret at runtime.
    """
    name = naming.linked_service_key_vault()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "type": "AzureKeyVault",
            "typeProperties": {
                "baseUrl": key_vault_uri,
            },
        },
    }


def build_postgres_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for Postgres sources (basic auth).

    Parameters: ``p_server``, ``p_database``, ``p_user``,
    ``p_password_secret``. Port defaults to 5432; customers needing a
    non-default port can edit the LS JSON post-deploy or wire ``p_port``
    in a future revision. Uses ADF's modern ``PostgreSqlV2`` connector.
    """
    name = naming.linked_service_postgres()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_server": {"type": "String"},
                "p_database": {"type": "String"},
                "p_user": {"type": "String"},
                "p_password_secret": {"type": "String"},
            },
            "type": "PostgreSqlV2",
            "typeProperties": {
                "server": {
                    "value": "@{linkedService().p_server}",
                    "type": "Expression",
                },
                "port": 5432,
                "database": {
                    "value": "@{linkedService().p_database}",
                    "type": "Expression",
                },
                "username": {
                    "value": "@{linkedService().p_user}",
                    "type": "Expression",
                },
                "password": _kv_secret_param_ref("p_password_secret"),
                # 3 = require (server enforces TLS, no cert verification).
                "sslMode": 3,
                "encoding": "UTF8",
            },
        },
    }


def build_mysql_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for MySQL sources (basic auth).

    Parameters: ``p_server``, ``p_database``, ``p_user``,
    ``p_password_secret``. Port defaults to 3306. Works with both
    MySQL and MariaDB.
    """
    name = naming.linked_service_mysql()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_server": {"type": "String"},
                "p_database": {"type": "String"},
                "p_user": {"type": "String"},
                "p_password_secret": {"type": "String"},
            },
            "type": "MySql",
            "typeProperties": {
                "server": {
                    "value": "@{linkedService().p_server}",
                    "type": "Expression",
                },
                "port": 3306,
                "database": {
                    "value": "@{linkedService().p_database}",
                    "type": "Expression",
                },
                "username": {
                    "value": "@{linkedService().p_user}",
                    "type": "Expression",
                },
                "password": _kv_secret_param_ref("p_password_secret"),
                # 1 = preferred (use TLS if the server offers it).
                "sslMode": 1,
            },
        },
    }


def build_oracle_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for Oracle sources (basic auth).

    Parameters: ``p_server``, ``p_service_name``, ``p_user``,
    ``p_password_secret``. Port defaults to 1521. Uses Oracle service
    name (not SID); customers on legacy SIDs edit the LS JSON
    post-deploy.
    """
    name = naming.linked_service_oracle()
    connection_string = (
        "Host=@{linkedService().p_server};Port=1521;"
        "ServiceName=@{linkedService().p_service_name};"
    )
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_server": {"type": "String"},
                "p_service_name": {"type": "String"},
                "p_user": {"type": "String"},
                "p_password_secret": {"type": "String"},
            },
            "type": "Oracle",
            "typeProperties": {
                "connectionString": connection_string,
                "authenticationType": "Basic",
                "username": {
                    "value": "@{linkedService().p_user}",
                    "type": "Expression",
                },
                "password": _kv_secret_param_ref("p_password_secret"),
            },
        },
    }


def build_sql_server_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for SQL Server sources (SQL-auth).

    Parameters: ``p_server``, ``p_database``, ``p_user``,
    ``p_password_secret``. Covers on-prem SQL Server, SQL Server in
    Azure VMs, and Managed Instance with public endpoint.

    On-prem boxes need a Self-Hosted Integration Runtime; customers
    bind their SHIR via ``connectVia`` post-deploy. The default
    AutoResolveIntegrationRuntime works for cloud-reachable servers.

    Encrypt+TrustServerCertificate=True is standard for SQL Server
    boxes that don't have a public CA cert; tighten via JSON edit if
    your server has a proper cert.
    """
    name = naming.linked_service_sql_server()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_server": {"type": "String"},
                "p_database": {"type": "String"},
                "p_user": {"type": "String"},
                "p_password_secret": {"type": "String"},
            },
            "type": "SqlServer",
            "typeProperties": {
                "connectionString": (
                    "Data Source=@{linkedService().p_server};"
                    "Initial Catalog=@{linkedService().p_database};"
                    "Integrated Security=False;"
                    "Encrypt=True;TrustServerCertificate=True;"
                    "Connection Timeout=30;"
                ),
                "userName": {
                    "value": "@{linkedService().p_user}",
                    "type": "Expression",
                },
                "password": _kv_secret_param_ref("p_password_secret"),
            },
        },
    }


def build_snowflake_basic_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for Snowflake sources using user/password.

    Parameters: ``p_account``, ``p_database``, ``p_warehouse``, ``p_user``,
    ``p_password_secret``, ``p_role`` (optional — defaults to user's
    default role when empty). Uses ADF's ``SnowflakeV2`` connector
    (the modern replacement for the legacy ``Snowflake``).

    Snowflake's recommended path is key-pair (build_snowflake_key_pair_*);
    basic stays for legacy accounts that still permit password auth.
    """
    name = naming.linked_service_snowflake_basic()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_account": {"type": "String"},
                "p_database": {"type": "String"},
                "p_warehouse": {"type": "String"},
                "p_user": {"type": "String"},
                "p_password_secret": {"type": "String"},
                "p_role": {"type": "String", "defaultValue": ""},
            },
            "type": "SnowflakeV2",
            "typeProperties": {
                "accountIdentifier": {
                    "value": "@{linkedService().p_account}",
                    "type": "Expression",
                },
                "database": {
                    "value": "@{linkedService().p_database}",
                    "type": "Expression",
                },
                "warehouse": {
                    "value": "@{linkedService().p_warehouse}",
                    "type": "Expression",
                },
                "role": {
                    "value": "@{linkedService().p_role}",
                    "type": "Expression",
                },
                "authenticationType": "Basic",
                "user": {
                    "value": "@{linkedService().p_user}",
                    "type": "Expression",
                },
                "password": _kv_secret_param_ref("p_password_secret"),
            },
        },
    }


def build_snowflake_key_pair_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for Snowflake using key-pair auth.

    Parameters: ``p_account``, ``p_database``, ``p_warehouse``, ``p_user``,
    ``p_private_key_secret`` (KV holds PEM-encoded RSA private key),
    ``p_role`` (optional). Snowflake's recommended path now that
    password auth is being phased out — pair with
    ``ALTER USER ... SET RSA_PUBLIC_KEY = '...'`` on the Snowflake
    side, after generating the key pair locally.
    """
    name = naming.linked_service_snowflake_key_pair()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_account": {"type": "String"},
                "p_database": {"type": "String"},
                "p_warehouse": {"type": "String"},
                "p_user": {"type": "String"},
                "p_private_key_secret": {"type": "String"},
                "p_role": {"type": "String", "defaultValue": ""},
            },
            "type": "SnowflakeV2",
            "typeProperties": {
                "accountIdentifier": {
                    "value": "@{linkedService().p_account}",
                    "type": "Expression",
                },
                "database": {
                    "value": "@{linkedService().p_database}",
                    "type": "Expression",
                },
                "warehouse": {
                    "value": "@{linkedService().p_warehouse}",
                    "type": "Expression",
                },
                "role": {
                    "value": "@{linkedService().p_role}",
                    "type": "Expression",
                },
                "authenticationType": "KeyPair",
                "user": {
                    "value": "@{linkedService().p_user}",
                    "type": "Expression",
                },
                "privateKey": _kv_secret_param_ref("p_private_key_secret"),
            },
        },
    }


def build_bigquery_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for BigQuery sources (service-account auth).

    Parameters: ``p_project_id``, ``p_key_file_secret``. Uses ADF's
    ``GoogleBigQueryV2`` connector. The KV secret holds the full
    service-account JSON key file content; ADF dereferences it as
    ``keyFileContent`` at activity time.

    Customers create the service account in GCP IAM with
    ``BigQuery Data Viewer`` + ``BigQuery Job User`` (or narrower)
    on the source datasets, download the key file as JSON, and store
    the JSON content in Key Vault under the secret name referenced
    by the source's ``connection_secret_name``.
    """
    name = naming.linked_service_bigquery()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_project_id": {"type": "String"},
                "p_key_file_secret": {"type": "String"},
            },
            "type": "GoogleBigQueryV2",
            "typeProperties": {
                "projectId": {
                    "value": "@{linkedService().p_project_id}",
                    "type": "Expression",
                },
                "authenticationType": "ServiceAuthentication",
                "keyFileContent": _kv_secret_param_ref("p_key_file_secret"),
            },
        },
    }


def build_servicenow_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for ServiceNow sources (basic auth).

    Parameters: ``p_environment_url`` (the instance URL — e.g.
    ``https://acme.service-now.com``), ``p_user``,
    ``p_password_secret``. Uses ADF's legacy ``ServiceNow`` connector
    with HTTP Basic auth — the simplest path for orgs running a
    service-account user with REST API access on the target instance.

    Customer side (one-time setup):
    1. Create a ServiceNow user with the ``rest_api_explorer`` /
       ``snc_platform_rest_api_access`` role (or equivalent) — read
       access on the tables you intend to ingest.
    2. Store the password in Key Vault under the secret name
       referenced by source_config.connection_secret_name.
    3. Set source_config.environment_url to the instance URL
       (``https://<instance>.service-now.com``).

    OAuth 2.0 (via the newer ``ServiceNowV2`` connector) lands in
    Phase B for orgs that mandate OAuth instead of password auth.
    The ADF property is named ``endpoint``; the framework's
    ``p_environment_url`` parameter maps to it (Salesforce uses the
    same registry key but maps to ``environmentUrl``).
    """
    name = naming.linked_service_servicenow()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_environment_url": {"type": "String"},
                "p_user": {"type": "String"},
                "p_password_secret": {"type": "String"},
            },
            "type": "ServiceNow",
            "typeProperties": {
                "endpoint": {
                    "value": "@{linkedService().p_environment_url}",
                    "type": "Expression",
                },
                "authenticationType": "Basic",
                "username": {
                    "value": "@{linkedService().p_user}",
                    "type": "Expression",
                },
                "password": _kv_secret_param_ref("p_password_secret"),
                # Default TLS posture — ServiceNow instances ship with
                # public CA certs.
                "useEncryptedEndpoints": True,
                "useHostVerification": True,
                "usePeerVerification": True,
            },
        },
    }


def build_sap_hana_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for SAP HANA sources (basic auth).

    Parameters: ``p_server``, ``p_user``, ``p_password_secret``. Server
    accepts the ``host:port`` form (port defaults to 30015 for HANA
    standard SQL when omitted; HANA Cloud typically uses 443 with TLS).
    Customers running on-prem HANA bind a Self-Hosted Integration
    Runtime via ``connectVia`` post-deploy; the default
    AutoResolveIntegrationRuntime works for HANA Cloud and HANA boxes
    reachable from the ADF Azure IR.

    HANA has no per-connection ``database`` concept (schemas are
    referenced inline in the SELECT), so the LS doesn't carry a
    ``p_database`` parameter — distinct from Postgres / MySQL / SQL
    Server LSs.
    """
    name = naming.linked_service_sap_hana()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_server": {"type": "String"},
                "p_user": {"type": "String"},
                "p_password_secret": {"type": "String"},
            },
            "type": "SapHana",
            "typeProperties": {
                "server": {
                    "value": "@{linkedService().p_server}",
                    "type": "Expression",
                },
                "authenticationType": "Basic",
                "userName": {
                    "value": "@{linkedService().p_user}",
                    "type": "Expression",
                },
                "password": _kv_secret_param_ref("p_password_secret"),
            },
        },
    }


def build_salesforce_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for Salesforce sources (OAuth 2.0).

    Parameters: ``p_environment_url``, ``p_client_id``,
    ``p_client_secret_secret``. Uses ADF's ``SalesforceV2`` connector
    with the ``OAuth2ClientCredentials`` grant — Salesforce's
    recommended path now that username + password is being deprecated
    for new orgs.

    Customer side (one-time setup):
    1. Salesforce Setup → App Manager → New Connected App with
       ``OAuth Settings`` enabled. Add ``OAuth 2.0 Client Credentials``
       to the grant types.
    2. Approve the Connected App; capture Consumer Key (= clientId)
       and Consumer Secret.
    3. Store the Consumer Secret in Key Vault under the secret name
       referenced by source_config.connection_secret_name.
    4. Set source_config.environment_url to the org's My Domain URL
       (https://my-org.my.salesforce.com).

    JWT bearer flow (for orgs that require it) lands in Phase B.
    """
    name = naming.linked_service_salesforce()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_environment_url": {"type": "String"},
                "p_client_id": {"type": "String"},
                "p_client_secret_secret": {"type": "String"},
            },
            "type": "SalesforceV2",
            "typeProperties": {
                "environmentUrl": {
                    "value": "@{linkedService().p_environment_url}",
                    "type": "Expression",
                },
                "authenticationType": "OAuth2ClientCredentials",
                "clientId": {
                    "value": "@{linkedService().p_client_id}",
                    "type": "Expression",
                },
                "clientSecret": _kv_secret_param_ref("p_client_secret_secret"),
                "apiVersion": "v59.0",
            },
        },
    }


def build_azure_blob_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for Azure Blob sources (MSI auth).

    Parameter: ``p_storage_account``. Uses ADF's ``AzureBlobStorage``
    connector. Implicit MSI auth — no credentials in JSON; the ADF
    MSI must be granted ``Storage Blob Data Reader`` on the storage
    account or specific containers out-of-band.

    Customers needing SAS / account-key auth (cross-tenant or partner
    drops) edit the LS post-deploy or wait for the SAS LS variant
    (Phase B).
    """
    name = naming.linked_service_azure_blob()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_storage_account": {"type": "String"},
            },
            "type": "AzureBlobStorage",
            "typeProperties": {
                "serviceEndpoint": {
                    "value": (
                        "@{concat('https://', linkedService().p_storage_account, "
                        "'.blob.core.windows.net/')}"
                    ),
                    "type": "Expression",
                },
                "accountKind": "StorageV2",
            },
        },
    }


def build_s3_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for AWS S3 sources (access-key auth).

    Parameters: ``p_access_key_id``, ``p_secret_access_key_secret``.
    The KV secret holds the AWS secret access key (paired with the
    access_key_id supplied via source_config).

    Customers create an IAM user with ``AmazonS3ReadOnlyAccess`` on the
    target buckets, generate the access-key pair, and store the secret
    access key in Key Vault under the name referenced by the source's
    ``connection_secret_name``. Cross-account access via STS / role
    assumption is a future enhancement.
    """
    name = naming.linked_service_s3()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_access_key_id": {"type": "String"},
                "p_secret_access_key_secret": {"type": "String"},
            },
            "type": "AmazonS3",
            "typeProperties": {
                "authenticationType": "AccessKey",
                "accessKeyId": {
                    "value": "@{linkedService().p_access_key_id}",
                    "type": "Expression",
                },
                "secretAccessKey": _kv_secret_param_ref("p_secret_access_key_secret"),
            },
        },
    }


def build_synapse_sql_pool_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for Synapse Dedicated SQL Pool (MSI auth).

    Parameters: ``p_server``, ``p_database``. Uses ADF's ``AzureSqlDW``
    connector — distinct from ``AzureSqlDatabase`` (Azure SQL DB) and
    ``SqlServer``. Connection string format mirrors the Azure SQL DB
    LS; the ADF connector handles dedicated-pool semantics under the
    hood.

    MSI-only in Phase A. The Synapse workspace must grant the ADF MSI
    a database-level user with ``db_datareader``:
        CREATE USER [my-adf] FROM EXTERNAL PROVIDER;
        ALTER ROLE db_datareader ADD MEMBER [my-adf];
    """
    name = naming.linked_service_synapse_sql_pool()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_server": {"type": "String"},
                "p_database": {"type": "String"},
            },
            "type": "AzureSqlDW",
            "typeProperties": {
                "connectionString": (
                    "Server=tcp:@{linkedService().p_server},1433;"
                    "Database=@{linkedService().p_database};"
                    "Encrypt=true;TrustServerCertificate=false;Connection Timeout=30;"
                ),
                "authenticationType": "ManagedIdentity",
            },
        },
    }


def build_metadata_db_linked_service(*, server: str, database: str) -> dict[str, object]:
    """LinkedService for the env's metadata Azure SQL DB.

    Single-instance per env — deploy-time fixed. Used by driver pipelines
    for ``Lookup`` activities. MSI auth with ``db_datareader`` granted to
    the ADF MSI on the metadata DB out-of-band.
    """
    name = naming.linked_service_metadata_db()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "type": "AzureSqlDatabase",
            "typeProperties": {
                "connectionString": (
                    f"Server=tcp:{server},1433;Database={database};"
                    "Encrypt=true;TrustServerCertificate=false;Connection Timeout=30;"
                ),
                "authenticationType": "ManagedIdentity",
            },
        },
    }


def build_sftp_basic_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for SFTP sources using password auth.

    Parameters: ``p_server``, ``p_user``, ``p_password_secret``.
    Port defaults to 22. ``skipHostKeyValidation`` is False — customers
    needing first-time host-key acceptance set this through the LS JSON
    post-deploy.
    """
    name = naming.linked_service_sftp_basic()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_server": {"type": "String"},
                "p_user": {"type": "String"},
                "p_password_secret": {"type": "String"},
            },
            "type": "Sftp",
            "typeProperties": {
                "host": {
                    "value": "@{linkedService().p_server}",
                    "type": "Expression",
                },
                "port": 22,
                "skipHostKeyValidation": False,
                "authenticationType": "Basic",
                "userName": {
                    "value": "@{linkedService().p_user}",
                    "type": "Expression",
                },
                "password": _kv_secret_param_ref("p_password_secret"),
            },
        },
    }


def build_sftp_ssh_key_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for SFTP sources using SSH-key auth.

    Parameters: ``p_server``, ``p_user``, ``p_private_key_secret``
    (KV secret name holding the PEM-encoded private key).
    """
    name = naming.linked_service_sftp_ssh_key()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_server": {"type": "String"},
                "p_user": {"type": "String"},
                "p_private_key_secret": {"type": "String"},
            },
            "type": "Sftp",
            "typeProperties": {
                "host": {
                    "value": "@{linkedService().p_server}",
                    "type": "Expression",
                },
                "port": 22,
                "skipHostKeyValidation": False,
                "authenticationType": "SshPublicKey",
                "userName": {
                    "value": "@{linkedService().p_user}",
                    "type": "Expression",
                },
                "privateKeyContent": _kv_secret_param_ref("p_private_key_secret"),
            },
        },
    }


def build_rest_api_anonymous_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for REST API sources with anonymous / bearer auth.

    Parameters: ``p_base_url``. Bearer tokens are injected as
    ``additionalHeaders`` at activity time (the dataset / Copy Activity
    handles the Authorization header) — the LS itself stays Anonymous.
    """
    name = naming.linked_service_rest_api_anonymous()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_base_url": {"type": "String"},
            },
            "type": "RestService",
            "typeProperties": {
                "url": {
                    "value": "@{linkedService().p_base_url}",
                    "type": "Expression",
                },
                "enableServerCertificateValidation": True,
                "authenticationType": "Anonymous",
            },
        },
    }


def build_rest_api_basic_linked_service() -> dict[str, object]:
    """Parameterised LinkedService for REST API sources using HTTP basic auth.

    Parameters: ``p_base_url``, ``p_user``, ``p_password_secret``.
    """
    name = naming.linked_service_rest_api_basic()
    return {
        "name": name,
        "properties": {
            "annotations": [],
            "parameters": {
                "p_base_url": {"type": "String"},
                "p_user": {"type": "String"},
                "p_password_secret": {"type": "String"},
            },
            "type": "RestService",
            "typeProperties": {
                "url": {
                    "value": "@{linkedService().p_base_url}",
                    "type": "Expression",
                },
                "enableServerCertificateValidation": True,
                "authenticationType": "Basic",
                "userName": {
                    "value": "@{linkedService().p_user}",
                    "type": "Expression",
                },
                "password": _kv_secret_param_ref("p_password_secret"),
            },
        },
    }


__all__ = [
    "build_adls_gen2_linked_service",
    "build_azure_blob_linked_service",
    "build_azure_sql_db_linked_service",
    "build_bigquery_linked_service",
    "build_key_vault_linked_service",
    "build_metadata_db_linked_service",
    "build_mysql_linked_service",
    "build_oracle_linked_service",
    "build_postgres_linked_service",
    "build_rest_api_anonymous_linked_service",
    "build_rest_api_basic_linked_service",
    "build_s3_linked_service",
    "build_salesforce_linked_service",
    "build_sap_hana_linked_service",
    "build_servicenow_linked_service",
    "build_sftp_basic_linked_service",
    "build_sftp_ssh_key_linked_service",
    "build_snowflake_basic_linked_service",
    "build_snowflake_key_pair_linked_service",
    "build_sql_server_linked_service",
    "build_synapse_sql_pool_linked_service",
]
