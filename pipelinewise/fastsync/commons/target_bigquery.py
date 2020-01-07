from typing import List

from google.cloud import bigquery
from google.cloud.bigquery.job import SourceFormat
from google.cloud.bigquery import Dataset, WriteDisposition
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery import LoadJobConfig
from google.api_core import exceptions

import time
import os

from . import utils

class FastSyncTargetBigquery:
    EXTRACTED_AT_COLUMN = '_sdc_extracted_at'
    BATCHED_AT_COLUMN = '_sdc_batched_at'
    DELETED_AT_COLUMN = '_sdc_deleted_at'

    def __init__(self, connection_config, transformation_config=None):
        self.connection_config = connection_config
        self.transformation_config = transformation_config

    def open_connection(self):
        project_id = self.connection_config['project_id']
        return bigquery.Client(project=project_id)

    def query(self, query, params=[]):
        def to_query_parameter(value):
            if isinstance(value, int):
                value_type = "INT64"
            elif isinstance(value, float):
                value_type = "NUMERIC"
            #TODO: repeated float here and in target
            elif isinstance(value, float):
                value_type = "FLOAT64"
            elif isinstance(value, bool):
                value_type = "BOOL"
            else:
                value_type = "STRING"
            return bigquery.ScalarQueryParameter(None, value_type, value)

        job_config = bigquery.QueryJobConfig()
        query_params = [to_query_parameter(p) for p in params]
        job_config.query_parameters = query_params

        queries = []
        if type(query) is list:
            queries.extend(query)
        else:
            queries = [query]

        client = self.open_connection()
        utils.log("TARGET_BIGQUERY - Running query: {}".format(query))
        query_job = client.query(';\n'.join(queries), job_config=job_config)
        query_job.result()

        return query_job


    def create_schema(self, schema_name):
        temp_schema = self.connection_config.get('temp_schema', schema_name)
        schema_rows = 0

        for schema in set([schema_name, temp_schema]):
            schema_rows = self.query(
                'SELECT LOWER(schema_name) schema_name FROM INFORMATION_SCHEMA.SCHEMATA WHERE LOWER(schema_name) = ?',
                (schema.lower(),)
            )

            if schema_rows.result().total_rows == 0:
                utils.log("Schema '{}' does not exist. Creating...".format(schema))
                client = self.open_connection()
                dataset = client.create_dataset(schema)

    def drop_table(self, target_schema, table_name, is_temporary=False):
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        sql = "DROP TABLE IF EXISTS {}.{}".format(target_schema, target_table)
        self.query(sql)

    def create_table(self, target_schema: str, table_name: str, columns: List[str],
                     is_temporary: bool = False, sort_columns = False):

        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        # skip the EXTRACTED, BATCHED and DELETED columns in case they exist because they gonna be added later
        columns = [c.replace('current', '_current') for c in columns if not (c.startswith(self.EXTRACTED_AT_COLUMN) or
                                              c.startswith(self.BATCHED_AT_COLUMN) or
                                              c.startswith(self.DELETED_AT_COLUMN))]

        columns += [f'{self.EXTRACTED_AT_COLUMN} TIMESTAMP',
                    f'{self.BATCHED_AT_COLUMN} TIMESTAMP',
                    f'{self.DELETED_AT_COLUMN} TIMESTAMP'
                    ]

        sql = f"""CREATE OR REPLACE TABLE {target_schema}.{target_table} (
        {','.join(columns)})
        """

        self.query(sql)

    def copy_to_table(self, file_name, target_schema, table_name, is_temporary):
        utils.log("BIGQUERY - Loading {} into Bigquery...".format(file_name))
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')

        client = self.open_connection()
        dataset_ref = client.dataset(target_schema)
        table_ref = dataset_ref.table(target_table)
        table_schema = client.get_table(table_ref).schema
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = table_schema
        job_config.write_disposition = 'WRITE_TRUNCATE'
        with open(file_name, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()
        utils.log(job.errors)

    # grant_... functions are common functions called by utils.py: grant_privilege function
    # "to_group" is not used here but exists for compatibility reasons with other database types
    # "to_group" is for databases that can grant to users and groups separately like Amazon Redshift
    def grant_select_on_table(self, target_schema, table_name, role, is_temporary, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            table_dict = utils.tablename_to_dict(table_name)
            target_table = table_dict.get('table_name') if not is_temporary else table_dict.get('temp_table_name')
            sql = "GRANT SELECT ON {}.{} TO ROLE {}".format(target_schema, target_table, role)
            self.query(sql)

    def grant_usage_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = "GRANT USAGE ON SCHEMA {} TO ROLE {}".format(target_schema, role)
            self.query(sql)

    def grant_select_on_schema(self, target_schema, role, to_group=False):
        # Grant role is not mandatory parameter, do nothing if not specified
        if role:
            sql = "GRANT SELECT ON ALL TABLES IN SCHEMA {} TO ROLE {}".format(target_schema, role)
            self.query(sql)

    def obfuscate_columns(self, target_schema, table_name):
        utils.log("BIGQUERY - Applying obfuscation rules")
        table_dict = utils.tablename_to_dict(table_name)
        temp_table = table_dict.get('temp_table_name')
        transformations = self.transformation_config.get('transformations', [])
        trans_cols = []

        # Find obfuscation rule for the current table
        for t in transformations:
            # Input table_name is formatted as {{schema}}.{{table}}
            # Stream name in taps transformation.json is formatted as {{schema}}-{{table}}
            #
            # We need to convert to the same format to find the transformation
            # has that has to be applied
            tap_stream_name_by_table_name = "{}-{}".format(table_dict['schema_name'], table_dict['table_name']) \
                if table_dict['schema_name'] is not None else table_dict['table_name']

            if t.get('tap_stream_name') == tap_stream_name_by_table_name:
                # use safe field id in case the column to transform is has a name of a reserved word
                # fallback to field_id if the safe id doesn't exist
                column = t.get('safe_field_id', t.get('field_id'))
                transform_type = t.get('type')
                if transform_type == 'SET-NULL':
                    trans_cols.append("{} = NULL".format(column))
                elif transform_type == 'HASH':
                    trans_cols.append("{} = SHA2({}, 256)".format(column, column))
                elif 'HASH-SKIP-FIRST' in transform_type:
                    skip_first_n = transform_type[-1]
                    trans_cols.append(
                        "{} = CONCAT(SUBSTRING({}, 1, {}), SHA2(SUBSTRING({}, {} + 1), 256))".format(column, column,
                                                                                                     skip_first_n,
                                                                                                     column,
                                                                                                     skip_first_n))
                elif transform_type == 'MASK-DATE':
                    trans_cols.append("{} = TO_CHAR({}::DATE,'YYYY-01-01')::DATE".format(column, column))
                elif transform_type == 'MASK-NUMBER':
                    trans_cols.append("{} = 0".format(column))

        # Generate and run UPDATE if at least one obfuscation rule found
        if len(trans_cols) > 0:
            sql = "UPDATE {}.{} SET {}".format(target_schema, temp_table, ','.join(trans_cols))
            self.query(sql)

    def swap_tables(self, schema, table_name):
        project_id = self.connection_config['project_id']
        table_dict = utils.tablename_to_dict(table_name)
        target_table = table_dict.get('table_name')
        temp_table = table_dict.get('temp_table_name')

        # Swap tables and drop the temp tamp
        table_id = '{}.{}.{}'.format(project_id, schema, target_table)
        temp_table_id = '{}.{}.{}'.format(project_id, schema, temp_table)

        # we cant swap tables in bigquery, so we copy the temp into the table
        # then delete the temp table
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        client = self.open_connection()
        replace_job = client.copy_table(temp_table_id, table_id, job_config=job_config)
        replace_job.result()

        # delete the temp table
        delete_job = client.delete_table(temp_table_id)
        delete_job.result()
