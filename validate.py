import uuid
import sys
import boto3
import subprocess

from datetime import datetime

import pandas as pd
import numpy as np
import multiprocessing

from tables import TABLES
from config import IAM_ROLE, SCHEMA, ERROR_S3_BUCKET, SUCCESS_S3_BUCKET
from utils import Pool, dateparse , get_size , resolve_s3_location
from db_helpers import (
    create_connection,
    CREATE_INT,
    CREATE_STR,
    USER,
    DROP,
    COPY,
    TABLE_DESC,
)

S3_CLIENT = boto3.client('s3', 'us-west-2')

NA_VALUES = ["", "-1.#QNAN", "-NaN'", "-nan'", "1.#IND", "1.#QNAN", "NULL", "NaN", "nan", "null"]

class Table(object):
    def __init__(self, schema, name, pk_s3, pk_db, pk_data_type, files, columns):
        self.schema = schema
        self.name = name
        self.pk_s3 = pk_s3
        self.pk_db = pk_db
        self.pk_data_type = pk_data_type
        self.files = files
        self.columns = columns

    def process(self):
        if not DEBUG:
            pool = multiprocessing.Pool(multiprocessing.cpu_count())
            pool.map(self.process_file, self.files)
        else:
            for file in self.files:
                self.process_file(file)

        bucket_name, file_key = resolve_s3_location(file)
        id = uuid.uuid4()
        temp_dir = '/data/' 
        output_path = "s3://" + bucket_name + "/chucked_files"

        file_name_arr = file_key.split(".")
        subprocess.call(['sh' , 'split.sh' , file , output_path, temp_dir , file_name_arr[0] , str(id)])

    def regular_flow(self, file):
        run = str(uuid.uuid4()).replace('-', '_')
        file_name = file.split('/')[-1].split('.')[0].lower()
        test_db_table_name = file_name + '_' + run
        errors = []

        connection = create_connection()
        cursor = connection.cursor()
        with connection:
            self.create_database_table(connection, cursor, test_db_table_name)
            self.copy_data(
                connection=connection,
                cursor=cursor,
                s3_location=file,
                table=test_db_table_name,
                column=self.pk_db
            )
            sql = f'SELECT * FROM {self.schema}.{self.name} WHERE {self.pk_db} IN (SELECT {self.pk_db} FROM {self.schema}.{test_db_table_name}) ORDER BY {self.pk_db};'
            df_db = pd.read_sql_query(sql=sql, con=connection)
            date_columns = self.get_dtypes(cursor)
            # dtypes_db = {key.lower(): value for key, dtypes,value in dtypes.items()}
            # df_db = df_db.astype(dtypes_db)
            self.drop_table(connection, cursor, test_db_table_name)

        df_db.fillna('', inplace=True)
        print(date_columns)
        print(dtypes)

        df_s3 = pd.read_csv(file, parse_dates=date_columns, float_precision='round_trip', quotechar='"', escapechar='\\', date_parser=dateparse, na_values=NA_VALUES, keep_default_na=False)
        df_s3 = df_s3.sort_values(by=[self.pk_s3])

        try:
            df_s3.fillna('', inplace=True)
            df_s3 = df_s3.applymap(str)
        except (ValueError, TypeError):
            # Can't fill empty string on Int columns, so first fill NaN, then convert to string and replace 'nan' with ''
            df_s3.fillna(np.nan, inplace=True)
            df_s3 = df_s3.applymap(str)
            df_s3.replace('nan', '', inplace=True)

        df_db = df_db.applymap(str)

        df_s3.columns = map(str.lower, df_s3.columns)
        df_s3 = df_s3[df_db.columns.tolist()]
        df_s3_header = list(df_s3)
        # import ipdb;ipdb.set_trace()

        if df_db.shape != df_s3.shape:
            errors.append(f'Shape mismatch on file {file}')
            print(f' DB shape {df_db.shape} is different from S3 shape {df_s3.shape} on file {file}' )

        elif df_s3.equals(df_db):
                print ('Both comparable datasets are the same')
        else:  # Looping through datasets to find and list differences
            for i in range (0, len(df_s3.columns)):
                df_s3_value=df_s3[list(df_s3)[i]].tolist()
                df_db_value=df_db[list(df_db)[i]].tolist()
                for j in range (0,len(df_s3_value)):
                    if df_s3_value[j] != df_db_value[j]:
                        errors.append([df_s3_header[i], df_s3_value[j], df_db_value[j], df_s3.loc[j, self.pk_db]])
        '''             
        else:
            for idx in range(0, len(df_db)):
                result = df_s3.iloc[idx] == df_db.iloc[idx]
                if not result.all():
                    if IPDB:
                        import ipdb;ipdb.set_trace()
                    errors.append(f'Mismatch at PK {df_s3.iloc[idx][self.pk_db]}, {list(result.items())}')
        '''
        if errors:
            print(f'Validation failed on file {file}')
            s3_error_loc = ERROR_S3_BUCKET.format(table=self.name, table_db=file_name, time=str(datetime.now()).replace(' ', '_'))
            print(f'Writing errors to S3 to location {s3_error_loc}')
            columns = ['Col Name', 'S3 Val', 'DB Val', self.pk_db]
            df = pd.DataFrame(errors, columns=columns)
            df.to_csv(s3_error_loc, index=False)
        else:
            s3_success_loc = SUCCESS_S3_BUCKET.format(table=self.name, table_db=file_name, time=str(datetime.now()).replace(' ', '_'))
            print(f'Validation passed on file {file}')
            df = pd.DataFrame([f'Validation passed on file {file}'])
            df.to_csv(s3_success_loc, index=False)

    def get_dtypes(self, cursor):

        column_lookup = {column.lower(): column for column in self.columns}
        dtypes = {}
        date_columns = []

        data_type_map = {
            'bigint': pd.Int64Dtype(),
            'boolean': np.dtype(str),
            'character varying': np.dtype(str),
            'date': np.dtype(str),
            'double precision': np.dtype('float64'),
            'integer': pd.Int64Dtype(),
            'timestamp without time zone': np.dtype(str),
        }
        command = TABLE_DESC.format(table=self.name.lower(), schema=self.schema.lower())

        cursor.execute(command)
        result = cursor.fetchall()

        for column, data_type in result:
            if column in {'created_time', 'modified_time'}:
                continue
            if data_type == 'timestamp without time zone':
                date_columns.append(column_lookup[column])
            dtypes[column_lookup[column]] = data_type_map[data_type]

        return dtypes, date_columns

    def copy_data(self, connection, cursor, s3_location, table, column):
        command = COPY.format(
            schema=self.schema,
            s3_location=s3_location,
            table=table,
            column=column,
            iam_role=IAM_ROLE
        )
        cursor.execute(command)
        connection.commit()

    def create_database_table(self, connection, cursor, test_db_table_name):
        if self.pk_data_type == 'int':
            command = CREATE_INT.format(schema=self.schema, table=test_db_table_name, user=USER, pk=self.pk_db)
        else:
            command = CREATE_STR.format(schema=self.schema, table=test_db_table_name, user=USER, pk=self.pk_db)
        cursor.execute(command)
        connection.commit()

    def drop_table(self, connection, cursor, table):
        command = DROP.format(schema=self.schema, table=table)
        cursor.execute(command)
        connection.commit()

    def process_file(self, file):
        print(f"Processing file {file}")
        self.regular_flow(file)


def process_tables(table):
    table.process()


def main(schema):
    pool = Pool(multiprocessing.cpu_count())

    tables = [
        Table(
            schema=schema,
            name=table,
            pk_s3=TABLES[table]['pk_s3'],
            pk_db=TABLES[table]['pk_db'],
            pk_data_type=TABLES[table]['pk_data_type'],
            files=TABLES[table]['files'],
            columns=TABLES[table]['columns'],
        ) for table in TABLES.keys()
    ]
    if not DEBUG:
        with pool:
            pool.map(process_tables, tables)
    else:
        for table in tables:
            table.process()


if __name__ == '__main__':
    if '--debug' in sys.argv:
        DEBUG = True
    else:
        DEBUG = False

    if '--ipdb' in sys.argv:
        IPDB = True
    else:
        IPDB = False
    main(SCHEMA)
