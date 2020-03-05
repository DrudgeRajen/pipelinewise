#!/usr/bin/env python3

import os
import sys
import time

import multiprocessing

from datetime import datetime, timedelta
from .commons import utils
from .commons.tap_mysql import FastSyncTapMySql
from .commons.target_bigquery import FastSyncTargetBigquery, GCSObjectStreamUpload


REQUIRED_CONFIG_KEYS = {
    'tap': [
        'host',
        'port',
        'user',
        'password'
    ],
    'target': [
        'project_id'
    ]
}

lock = multiprocessing.Lock()


def tap_type_to_target_type(mysql_type, mysql_column_type):
    """Data type mapping from MySql to Bigquery"""
    return {
        'char':'STRING',
        'varchar':'STRING',
        'binary':'STRING',
        'varbinary':'STRING',
        'blob':'STRING',
        'tinyblob':'STRING',
        'mediumblob':'STRING',
        'longblob':'STRING',
        'geometry':'STRING',
        'text':'STRING',
        'tinytext':'STRING',
        'mediumtext':'STRING',
        'longtext':'STRING',
        'enum':'STRING',
        'int':'NUMERIC',
        'tinyint':'BOOL' if mysql_column_type == 'tinyint(1)' else 'NUMERIC',
        'smallint':'NUMERIC',
        'bigint':'NUMERIC',
        'bit':'BOOL',
        'decimal':'NUMERIC',
        'double':'NUMERIC',
        'float':'NUMERIC',
        'bool':'BOOL',
        'boolean':'BOOL',
        'date':'TIMESTAMP',
        'datetime':'TIMESTAMP',
        'timestamp':'TIMESTAMP'
    }.get(mysql_type, 'STRING')


def sync_table(table):
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    mysql = FastSyncTapMySql(args.tap, tap_type_to_target_type)
    bigquery = FastSyncTargetBigquery(args.target, args.transform)

    try:
        dbname = args.tap.get("dbname")
        filename = "pipelinewise_fastsync_{}_{}_{}.csv".format(dbname, table, time.strftime("%Y%m%d-%H%M%S"))
        filepath = os.path.join(args.export_dir, filename)
        target_schema = utils.get_target_schema(args.target, table)

        # Open connection
        mysql.open_connections()

        # Get bookmark - LSN position or Incremental Key value
        bookmark = utils.get_bookmark_for_table(table, args.properties, mysql, dbname=dbname)

        # Exporting table data, get table definitions and close connection to avoid timeouts
        blob_name = 'fastsync/{}/{}'.format(table, filename)
        with GCSObjectStreamUpload(bigquery_target=bigquery, blob_name=blob_name) as s:
            mysql.copy_table(table, s)
        bigquery_types = mysql.map_column_types_to_target(table)
        bigquery_columns = bigquery_types.get("columns", [])
        mysql.close_connections()

        # Creating temp table in Bigquery
        bigquery.create_schema(target_schema)
        bigquery.create_table(target_schema, table, bigquery_columns, is_temporary=True)

        # Load into Bigquery table
        bigquery.copy_to_table(blob_name, target_schema, table, is_temporary=True)

        # Obfuscate columns
        bigquery.obfuscate_columns(target_schema, table)

        # Create target table and swap with the temp table in Bigquery
        bigquery.create_table(target_schema, table, bigquery_columns)
        bigquery.swap_tables(target_schema, table)

        # Save bookmark to singer state file
        # Lock to ensure that only one process writes the same state file at a time
        lock.acquire()
        try:
            utils.save_state_file(args.state, table, bookmark)
        finally:
            lock.release()

        # Table loaded, grant select on all tables in target schema
        grantees = utils.get_grantees(args.target, table)
        utils.grant_privilege(target_schema, grantees, bigquery.grant_usage_on_schema)
        utils.grant_privilege(target_schema, grantees, bigquery.grant_select_on_schema)

    except KeyError as exc:
        utils.log("CRITICAL: {}".format(exc))
        return "{}: {}".format(table, exc)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    cpu_cores = utils.get_cpu_cores()
    start_time = datetime.now()
    table_sync_excs = []

    # Log start info
    utils.log("""
        -------------------------------------------------------
        STARTING SYNC
        -------------------------------------------------------
            Tables selected to sync        : {}
            Total tables selected to sync  : {}
            CPU cores                      : {}
        -------------------------------------------------------
        """.format(
            args.tables,
            len(args.tables),
            cpu_cores
        ))

    # Start loading tables in parallel in spawning processes by
    # utilising all available CPU cores
    with multiprocessing.Pool(cpu_cores) as p:
        table_sync_excs = list(filter(None, p.map(sync_table, args.tables)))

    # Log summary
    end_time = datetime.now()
    utils.log("""
        -------------------------------------------------------
        SYNC FINISHED - SUMMARY
        -------------------------------------------------------
            Total tables selected to sync  : {}
            Tables loaded successfully     : {}
            Exceptions during table sync   : {}

            CPU cores                      : {}
            Runtime                        : {}
        -------------------------------------------------------
        """.format(
            len(args.tables),
            len(args.tables) - len(table_sync_excs),
            str(table_sync_excs),
            cpu_cores,
            end_time  - start_time
        ))
    if len(table_sync_excs) > 0:
        sys.exit(1)


def main():
    try:
        main_impl()
    except KeyError as exc:
        utils.log("CRITICAL: {}".format(exc))
        raise exc

