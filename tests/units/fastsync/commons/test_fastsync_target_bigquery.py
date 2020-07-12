from pipelinewise.fastsync.commons.target_bigquery import FastSyncTargetBigquery

class BigqueryResultsMock(list):
    def __init__(self):
        self.total_rows = 0

class BigqueryJobMock():
    def result(self):
        return BigqueryResultsMock()

class BigqueryClientMock():
    """
    Mocked Bigquery Client class
    """
    def __init__(self):
        self.commands = []

    def create_dataset(self, dataset):
        """Create new dataset mock function"""
        self.commands.append(
            {"command": "create_dataset",
             "dataset": dataset}
        )
        return BigqueryJobMock()

    def load_table_from_file(self, file_obj, destination, job_config):
        """Load table from file mock function"""
        self.commands.append(
            {"command": "load_table_from_file",
             "file_obj": file_obj,
             "destination": destination,
             "job_config": job_config}
        )
        return BigqueryJobMock()

    def copy_table(self, sources, destination, job_config):
        """copy table from mock function"""
        self.commands.append(
            {"command": "copy_table",
             "sources": sources,
             "destination": destination,
             "job_config": job_config}
        )
        return BigqueryJobMock()

    def delete_table(self, table):
        """delete table mock function"""
        self.commands.append(
            {"command": "delete_table",
             "table": table}
        )

    def query(self, query):
        return BigqueryJobMock()

class FastSyncTargetBigqueryMock(FastSyncTargetBigquery):
    """
    Mocked FastSyncTargetBigquery class
    """
    def __init__(self, connection_config, transformation_config=None):
        super().__init__(connection_config, transformation_config)

        self.executed_queries = []
        self.client = BigqueryClientMock()

    def open_connection(self):
        return self.client

    def query(self, query, params=None):
        """Mock running of a query"""
        queries = []
        if type(query) is list:
            queries.extend(query)
        else:
            queries = [query]
        self.executed_queries += queries
        client = self.open_connection()
        query_job = client.query(';\n'.join(queries))
        return query_job


# pylint: disable=attribute-defined-outside-init
class TestFastSyncTargetBigquery:
    """
    Unit tests for fastsync target bigquery
    """
    def setup_method(self):
        """Initialise test FastSyncTargetPostgres object"""
        self.bigquery = FastSyncTargetBigqueryMock(connection_config={'project_id': 'dummy-project'},
                                                     transformation_config={})

    def test_create_schema(self):
        """Validate if create schema queries generated correctly"""
        self.bigquery.create_schema('new_schema')
        assert self.bigquery.client.commands == [{"command": "create_dataset", "dataset": "new_schema"}]


    def test_drop_table(self):
        """Validate if drop table queries generated correctly"""
        self.bigquery.drop_table('test_schema', 'test_table')
        self.bigquery.drop_table('test_schema', 'test_table', is_temporary=True)
        self.bigquery.drop_table('test_schema', 'UPPERCASE_TABLE')
        self.bigquery.drop_table('test_schema', 'UPPERCASE_TABLE', is_temporary=True)
        self.bigquery.drop_table('test_schema', 'test table with space')
        self.bigquery.drop_table('test_schema', 'test table with space', is_temporary=True)
        assert self.bigquery.executed_queries == [
            'DROP TABLE IF EXISTS test_schema.`test_table`',
            'DROP TABLE IF EXISTS test_schema.`test_table_temp`',
            'DROP TABLE IF EXISTS test_schema.`uppercase_table`',
            'DROP TABLE IF EXISTS test_schema.`uppercase_table_temp`',
            'DROP TABLE IF EXISTS test_schema.`test table with space`',
            'DROP TABLE IF EXISTS test_schema.`test table with space_temp`']

    def test_create_table(self):
        """Validate if create table queries generated correctly"""
        # Create table with standard table and column names
        self.bigquery.executed_queries = []
        self.bigquery.create_table(target_schema='test_schema',
                                    table_name='test_table',
                                    columns=['`id` INTEGER',
                                             '`txt` STRING'])
        assert self.bigquery.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema.`test_table` ('
            '`id` integer,`txt` string,'
            '_sdc_extracted_at TIMESTAMP,'
            '_sdc_batched_at TIMESTAMP,'
            '_sdc_deleted_at TIMESTAMP']

        # Create table with reserved words in table and column names
        self.bigquery.executed_queries = []
        self.bigquery.create_table(target_schema='test_schema',
                                    table_name='order',
                                    columns=['`id` INTEGER',
                                             '`txt` STRING',
                                             '`select` STRING'])
        assert self.bigquery.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema.`order` ('
            '`id` integer,`txt` string,`select` string,'
            '_sdc_extracted_at TIMESTAMP,'
            '_sdc_batched_at TIMESTAMP,'
            '_sdc_deleted_at TIMESTAMP']

        # Create table with mixed lower and uppercase and space characters
        self.bigquery.executed_queries = []
        self.bigquery.create_table(target_schema='test_schema',
                                    table_name='TABLE with SPACE',
                                    columns=['`ID` INTEGER',
                                             '`COLUMN WITH SPACE` STRING'])
        assert self.bigquery.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema.`table with space` ('
            '`id` integer,`column with space` string,'
            '_sdc_extracted_at TIMESTAMP,'
            '_sdc_batched_at TIMESTAMP,'
            '_sdc_deleted_at TIMESTAMP']

        # Create table with no primary key
        self.bigquery.executed_queries = []
        self.bigquery.create_table(target_schema='test_schema',
                                    table_name='test_table_no_pk',
                                    columns=['`ID` INTEGER',
                                             '`TXT` STRING'])
        assert self.bigquery.executed_queries == [
            'CREATE OR REPLACE TABLE test_schema.`test_table_no_pk` ('
            '`id` integer,`txt` string,'
            '_sdc_extracted_at TIMESTAMP,'
            '_sdc_batched_at TIMESTAMP,'
            '_sdc_deleted_at TIMESTAMP']

    def test_copy_to_table(self):
        """Validate if COPY command generated correctly"""
        # COPY table with standard table and column names
        self.bigquery.executed_queries = []
        self.bigquery.copy_to_table(filepath='filename.csv.gz',
                                     target_schema='test_schema',
                                     table_name='test_table',
                                     size_bytes=1000,
                                     is_temporary=False,
                                     skip_csv_header=False)
        assert self.bigquery.executed_queries == [
            'COPY INTO test_schema."TEST_TABLE" FROM \'@dummy_stage/s3_key\''
            ' FILE_FORMAT = (type=CSV escape=\'\\x1e\' escape_unenclosed_field=\'\\x1e\''
            ' field_optionally_enclosed_by=\'\"\' skip_header=0'
            ' compression=GZIP binary_format=HEX)']

        # COPY table with reserved word in table and column names in temp table
        self.bigquery.executed_queries = []
        self.bigquery.copy_to_table(s3_key='s3_key',
                                     target_schema='test_schema',
                                     table_name='full',
                                     size_bytes=1000,
                                     is_temporary=True,
                                     skip_csv_header=False)
        assert self.bigquery.executed_queries == [
            'COPY INTO test_schema."FULL_TEMP" FROM \'@dummy_stage/s3_key\''
            ' FILE_FORMAT = (type=CSV escape=\'\\x1e\' escape_unenclosed_field=\'\\x1e\''
            ' field_optionally_enclosed_by=\'\"\' skip_header=0'
            ' compression=GZIP binary_format=HEX)']

        # COPY table with space and uppercase in table name and s3 key
        self.bigquery.executed_queries = []
        self.bigquery.copy_to_table(s3_key='s3 key with space',
                                     target_schema='test_schema',
                                     table_name='table with SPACE and UPPERCASE',
                                     size_bytes=1000,
                                     is_temporary=True,
                                     skip_csv_header=False)
        assert self.bigquery.executed_queries == [
            'COPY INTO test_schema."TABLE WITH SPACE AND UPPERCASE_TEMP" FROM \'@dummy_stage/s3 key with space\''
            ' FILE_FORMAT = (type=CSV escape=\'\\x1e\' escape_unenclosed_field=\'\\x1e\''
            ' field_optionally_enclosed_by=\'\"\' skip_header=0'
            ' compression=GZIP binary_format=HEX)']

    def test_grant_select_on_table(self):
        """Validate if GRANT command generated correctly"""
        # GRANT table with standard table and column names
        self.bigquery.executed_queries = []
        self.bigquery.grant_select_on_table(target_schema='test_schema',
                                             table_name='test_table',
                                             role='test_role',
                                             is_temporary=False)
        assert self.bigquery.executed_queries == [
            'GRANT SELECT ON test_schema."TEST_TABLE" TO ROLE test_role']

        # GRANT table with reserved word in table and column names in temp table
        self.bigquery.executed_queries = []
        self.bigquery.grant_select_on_table(target_schema='test_schema',
                                             table_name='full',
                                             role='test_role',
                                             is_temporary=False)
        assert self.bigquery.executed_queries == [
            'GRANT SELECT ON test_schema."FULL" TO ROLE test_role']

        # GRANT table with with space and uppercase in table name and s3 key
        self.bigquery.executed_queries = []
        self.bigquery.grant_select_on_table(target_schema='test_schema',
                                             table_name='table with SPACE and UPPERCASE',
                                             role='test_role',
                                             is_temporary=False)
        assert self.bigquery.executed_queries == [
            'GRANT SELECT ON test_schema."TABLE WITH SPACE AND UPPERCASE" TO ROLE test_role']

    def test_grant_usage_on_schema(self):
        """Validate if GRANT command generated correctly"""
        self.bigquery.executed_queries = []
        self.bigquery.grant_usage_on_schema(target_schema='test_schema',
                                             role='test_role')
        assert self.bigquery.executed_queries == [
            'GRANT USAGE ON SCHEMA test_schema TO ROLE test_role']

    def test_grant_select_on_schema(self):
        """Validate if GRANT command generated correctly"""
        self.bigquery.executed_queries = []
        self.bigquery.grant_select_on_schema(target_schema='test_schema',
                                              role='test_role')
        assert self.bigquery.executed_queries == [
            'GRANT SELECT ON ALL TABLES IN SCHEMA test_schema TO ROLE test_role']

    def test_swap_tables(self):
        """Validate if swap table commands generated correctly"""
        # Swap tables with standard table and column names
        self.bigquery.executed_queries = []
        self.bigquery.swap_tables(schema='test_schema',
                                   table_name='test_table')
        assert self.bigquery.executed_queries == [
            'ALTER TABLE test_schema."TEST_TABLE_TEMP" SWAP WITH test_schema."TEST_TABLE"',
            'DROP TABLE IF EXISTS test_schema."TEST_TABLE_TEMP"']

        # Swap tables with reserved word in table and column names in temp table
        self.bigquery.executed_queries = []
        self.bigquery.swap_tables(schema='test_schema',
                                   table_name='full')
        assert self.bigquery.executed_queries == [
            'ALTER TABLE test_schema."FULL_TEMP" SWAP WITH test_schema."FULL"',
            'DROP TABLE IF EXISTS test_schema."FULL_TEMP"']

        # Swap tables with with space and uppercase in table name and s3 key
        self.bigquery.executed_queries = []
        self.bigquery.swap_tables(schema='test_schema',
                                   table_name='table with SPACE and UPPERCASE')
        assert self.bigquery.executed_queries == [
            'ALTER TABLE test_schema."TABLE WITH SPACE AND UPPERCASE_TEMP" '
            'SWAP WITH test_schema."TABLE WITH SPACE AND UPPERCASE"',
            'DROP TABLE IF EXISTS test_schema."TABLE WITH SPACE AND UPPERCASE_TEMP"']
