import boto3
from collections import OrderedDict
import logging
import json

import avro.schema
import click
from jinja2 import Template
import yaml

from constants import TMP_S3_OUTPUT_PATH, DEFAULT_ATHENA_DATABASE

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
LOG.addHandler(logging.StreamHandler())

def run_query(client, database, query, s3_output=TMP_S3_OUTPUT_PATH):    
    LOG.debug('SQL: ' + query)
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': s3_output
        }
    )
    LOG.info('Execution ID: ' + response['QueryExecutionId'])
    return response

def create_database(client, database):
    sql =  'CREATE DATABASE IF NOT EXISTS %s;' % (database)
    return run_query(client, sql, database)

def create_table(client, database, table, schema, s3_input):
    # TODO generate table definition from avro schema
    sql_table_schema = ' \
        `id` string, \
        `account_id` bigint, \
        `project_id` bigint, \
        `experiment_id` bigint, \
        `visitor_id` string, \
        `visitor_uuid` bigint, \
        `original_session_id` int, \
        `session_id` int, \
        `layer_id` string, \
        `timestamp` bigint, \
        `event_id` bigint, \
        `count` int, \
        `revenue` bigint, \
        `value` double, \
        `variation_id` string, \
        `segments` array<struct<id:bigint,value:string>>, \
        `should_start_counting_session` boolean, \
        `should_start_counting_visitor` boolean, \
        `received_timestamp` bigint'
        
    sql = '''
        CREATE EXTERNAL TABLE `{}`({})
        PARTITIONED BY ( 
            `received_year` int, 
            `received_month` int, 
            `received_day` int)
        ROW FORMAT SERDE
            'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT 
            'org.apache.hadoop.mapred.TextInputFormat' 
        OUTPUTFORMAT 
            'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION
            '{}' 
        '''.format(table, sql_table_schema, s3_input)
    
    return run_query(client, sql, database)

def add_table_partition(client, database, table, partitions, s3_input):
    partitions_str = [ k+'='+v for k, v in partitions.items()]

    print(partitions_str)
    sql = 'ALTER TABLE {} ADD PARTITION ({}) LOCATION \'{}\''.format(
        table,
        ','.join(partitions_str),
        s3_input
    )

    return run_query(client, database, sql)
    

def start_workflow(steps, database=DEFAULT_ATHENA_DATABASE):
    client = boto3.client('athena', region_name='us-east-1')
    print(steps)

    if 'create_database' in steps:
        create_database(client, database)
    
    if 'create_table' in steps:
        create_table(client, database, **steps['create_database'])

    if 'add_table_partition' in steps:
        add_table_partition(client, database, **steps['add_table_partition'])

    for q in steps.get('queries', []):
        run_query(client, database, q['query'], q['s3_output_path'])


@click.command()
@click.argument('f', type=click.Path(exists=True))
@click.option('--workflow_inputes', '-i', nargs=2, type=click.Tuple([str, str]), multiple=True)
def main(f, workflow_inputes):
    inputs = dict(workflow_inputes)
    print(inputs)

    # read configuration from file path
    job_conf_template = {}
    with open(click.format_filename(f)) as job_file:
        job_conf_template = yaml.safe_load(job_file)
    
    template = Template(str(job_conf_template))
    render_yaml = template.render(**dict(workflow_inputes))
    job_conf = yaml.safe_load(render_yaml)

    # TODO generate table definition from avro schema
    # schemas = avro.schema.Parse(open(job_conf.get('schema_path'), "rb").read())
    # print(schemas.schemas[2].name)
    # schema = list(filter(lambda s: s.name == job_conf.get('schema'), schemas.schemas))
    # print(schema[0].field_map['id'].type)
    # if schema:
    #     start_workflow(
    #         job_conf['database'],
    #         job_conf['table'],
    #         job_conf['s3_input'],
    #         job_conf['s3_output'],
    #         schema[0],
    #         job_conf['queries']
    #       )

    start_workflow(job_conf['steps'])
if __name__ == "__main__":
    main()
