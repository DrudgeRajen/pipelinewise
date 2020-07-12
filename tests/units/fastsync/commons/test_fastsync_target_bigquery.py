import pytest
from unittest.mock import Mock, patch, ANY
from pipelinewise.fastsync.commons.target_bigquery import FastSyncTargetBigquery

@pytest.fixture
def query_result():
    """
    Mocked Bigquery Run Query Results
    """
    qr = Mock()
    qr.return_value = 1
    qr.total_rows = 0
    return qr

@pytest.fixture
def query_job(query_result):
    """
    Mocked Bigquery Job Query
    """
    qj = Mock()
    qj.job_id = 1
    qj.result().total_rows = 0
    return qj

class FastSyncTargetBigqueryMock(FastSyncTargetBigquery):
    """
    Mocked FastSyncTargetBigquery class
    """
    def __init__(self, connection_config, transformation_config=None):
        super().__init__(connection_config, transformation_config)

        self.executed_queries = []

# pylint: disable=attribute-defined-outside-init
class TestFastSyncTargetBigquery:
    """
    Unit tests for fastsync target bigquery
    """
    def setup_method(self):
        """Initialise test FastSyncTargetPostgres object"""
        self.bigquery = FastSyncTargetBigqueryMock(connection_config={'project_id': 'dummy-project'},
                                                   transformation_config={})

    @patch("pipelinewise.fastsync.commons.target_bigquery.bigquery.Client")
    def test_create_schema(self, Client, query_job):
        """Validate if create schema queries generated correctly"""
        Client().query.return_value = query_job
        self.bigquery.create_schema('new_schema')
        Client().create_dataset.assert_called_with('new_schema')

    @patch("pipelinewise.fastsync.commons.target_bigquery.bigquery.Client")
    def test_drop_table(self, Client, query_job):
        """Validate if drop table queries generated correctly"""
        Client().query.return_value = query_job

        self.bigquery.drop_table('test_schema', 'test_table')
        Client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`test_table`', job_config=ANY)

        self.bigquery.drop_table('test_schema', 'test_table', is_temporary=True)
        Client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`test_table_temp`', job_config=ANY)

        self.bigquery.drop_table('test_schema', 'UPPERCASE_TABLE')
        Client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`uppercase_table`', job_config=ANY)

        self.bigquery.drop_table('test_schema', 'UPPERCASE_TABLE', is_temporary=True)
        Client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`uppercase_table_temp`', job_config=ANY)

        self.bigquery.drop_table('test_schema', 'test table with space')
        Client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`test table with space`', job_config=ANY)

        self.bigquery.drop_table('test_schema', 'test table with space', is_temporary=True)
        Client().query.assert_called_with(
            'DROP TABLE IF EXISTS test_schema.`test table with space_temp`', job_config=ANY)

    @patch("pipelinewise.fastsync.commons.target_bigquery.bigquery.Client")
    def test_create_table(self, Client, query_job):
        """Validate if create table queries generated correctly"""
        Client().query.return_value = query_job

        # Create table with standard table and column names
        self.bigquery.executed_queries = []
        self.bigquery.create_table(target_schema='test_schema',
                                    table_name='test_table',
                                    columns=['`id` INTEGER',
                                             '`txt` STRING'])
        Client().query.assert_called_with(
            'CREATE OR REPLACE TABLE test_schema.`test_table` ('
            '`id` integer,`txt` string,'
            '_sdc_extracted_at TIMESTAMP,'
            '_sdc_batched_at TIMESTAMP,'
            '_sdc_deleted_at TIMESTAMP',
            job_config=ANY)

        # Create table with reserved words in table and column names
        self.bigquery.executed_queries = []
        self.bigquery.create_table(target_schema='test_schema',
                                    table_name='order',
                                    columns=['`id` INTEGER',
                                             '`txt` STRING',
                                             '`select` STRING'])
        Client().query.assert_called_with(
            'CREATE OR REPLACE TABLE test_schema.`order` ('
            '`id` integer,`txt` string,`select` string,'
            '_sdc_extracted_at TIMESTAMP,'
            '_sdc_batched_at TIMESTAMP,'
            '_sdc_deleted_at TIMESTAMP',
            job_config=ANY)

        # Create table with mixed lower and uppercase and space characters
        self.bigquery.executed_queries = []
        self.bigquery.create_table(target_schema='test_schema',
                                    table_name='TABLE with SPACE',
                                    columns=['`ID` INTEGER',
                                             '`COLUMN WITH SPACE` STRING'])
        Client().query.assert_called_with(
            'CREATE OR REPLACE TABLE test_schema.`table with space` ('
            '`id` integer,`column with space` string,'
            '_sdc_extracted_at TIMESTAMP,'
            '_sdc_batched_at TIMESTAMP,'
            '_sdc_deleted_at TIMESTAMP',
            job_config=ANY)

        # Create table with no primary key
        self.bigquery.executed_queries = []
        self.bigquery.create_table(target_schema='test_schema',
                                    table_name='test_table_no_pk',
                                    columns=['`ID` INTEGER',
                                             '`TXT` STRING'])
        Client().query.assert_called_with(
            'CREATE OR REPLACE TABLE test_schema.`test_table_no_pk` ('
            '`id` integer,`txt` string,'
            '_sdc_extracted_at TIMESTAMP,'
            '_sdc_batched_at TIMESTAMP,'
            '_sdc_deleted_at TIMESTAMP',
            job_config=ANY)

    @patch("pipelinewise.fastsync.commons.target_bigquery.bigquery")
    def test_copy_to_table(self):
        """Validate if COPY command generated correctly"""
        # COPY table with standard table and column names
        self.bigquery.executed_queries = []
        self.bigquery.copy_to_table(filepath='/dev/null',
                                     target_schema='test_schema',
                                     table_name='test_table',
                                     size_bytes=1000,
                                     is_temporary=False,
                                     skip_csv_header=False)
        assert self.bigquery.commands == {
             'command': 'load_table_from_file',
             'filepath': '/dev/null',
             'destination': 'destasdf',
             'job_config': 'jobasdf'}


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

    @patch("pipelinewise.fastsync.commons.target_bigquery.bigquery")
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

    @patch("pipelinewise.fastsync.commons.target_bigquery.bigquery")
    def test_grant_usage_on_schema(self):
        """Validate if GRANT command generated correctly"""
        self.bigquery.executed_queries = []
        self.bigquery.grant_usage_on_schema(target_schema='test_schema',
                                             role='test_role')
        assert self.bigquery.executed_queries == [
            'GRANT USAGE ON SCHEMA test_schema TO ROLE test_role']

    @patch("pipelinewise.fastsync.commons.target_bigquery.bigquery")
    def test_grant_select_on_schema(self):
        """Validate if GRANT command generated correctly"""
        self.bigquery.executed_queries = []
        self.bigquery.grant_select_on_schema(target_schema='test_schema',
                                              role='test_role')
        assert self.bigquery.executed_queries == [
            'GRANT SELECT ON ALL TABLES IN SCHEMA test_schema TO ROLE test_role']

    @patch("pipelinewise.fastsync.commons.target_bigquery.bigquery")
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
