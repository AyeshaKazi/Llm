import os
import json
import snowflake.connector
from snowflake.snowpark import Session

connection_parameters = json.load(open("./snowflake_connection.json"))


def connection() -> snowflake.connector.SnowflakeConnection:
    creds = {
        "authenticator": "username_password_mfa",
        "account": "",
        "user": "",
        "password": "",
        "role": "accountadmin",
        "warehouse": "compute_wh",
        "database": "aw_spcs_db",
        "schema": "public",
        "client_session_keep_alive": "True",
    }
    return snowflake.connector.connect(**creds)


def session() -> Session:
    return Session.builder.configs(connection_parameters).create()
