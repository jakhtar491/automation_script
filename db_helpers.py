import psycopg2

from config import (
    DB_USER,
    DATABASE,
    PASSWORD,
    HOST,
    PORT,
)

CREATE_INT = \
"""
CREATE TABLE IF NOT EXISTS {schema}.{table}
(
    {pk}  BIGINT   ENCODE az64
)
DISTSTYLE EVEN
;
ALTER TABLE {schema}.{table} owner to {user};
"""

CREATE_STR = \
"""
CREATE TABLE IF NOT EXISTS {schema}.{table}
(
    {pk}  VARCHAR(65535)   ENCODE lzo
)
DISTSTYLE EVEN
;
ALTER TABLE {schema}.{table} owner to {user};
"""

TRUNCATE = "TRUNCATE TABLE {schema}.{table};"

DROP = "DROP TABLE {schema}.{table};"

COPY = \
"""
COPY {schema}.{table}
({column})
FROM '{s3_location}'
IAM_ROLE '{iam_role}'
GZIP
REMOVEQUOTES
DELIMITER ','
IGNOREHEADER AS 1
ESCAPE;
"""


CREATE_WITH_KEYS = \
"""
CREATE TABLE {schema}.{table}
(
{columns}
)
{distyle}
{distkeys}
{compound_sort_keys}
{interleaved_sort_keys}
;
"""

INSERT_CAST = \
"""
INSERT INTO {destination_schema}.{destination_table}
(
{destination_columns}
)
SELECT 
{source_columns}
FROM {source_schema}.{source_table};
"""


TABLE_DESC = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='{schema}' AND table_name='{table}';"

GET_TABLE_INFO = """SELECT tablename, "column", encoding, "column" || ' ' || UPPER(type) || ' ENCODE ' as dtype FROM pg_table_def WHERE schemaname = 'dev';"""


USER = DB_USER


def create_connection():
    return psycopg2.connect(
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )


def create_table(connection, schema, table, user, command, commit=True):
    cursor = connection.cusrsor()
    variables = {
        'schema': schema,
        'table': table,
        'user': user,
    }

    command = CREATE.format(**variables)

    cursor.execute(command)

    if commit:
        connection.commit()


def truncate_table(connection, schema, table, commit=True):
    pass


def drop_table(connection, cursor, schema, table, commit=True):
    command = DROP.format(schema=schema, table=table)
    cursor.execute(command)
    if commit:
        connection.commit()

