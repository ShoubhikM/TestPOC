# Inbuilt libraries
import sys
import json
import datetime
from datetime import timedelta
import logging

# aws sdk
import time
import boto3

# aws glue libraries
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Pyspark libraries
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql import functions as sf
from pyspark.sql.functions import col
from hdbcli import dbapi

#####################################Defining Aurora Tables ########################################
CONFIG_TABLE = 'odx_generic_tbl_extract_config'
CONFIG_TABLE_MANUAL = 'odx_generic_tbl_extract_config_manual'
AUDIT_JOB_SPECIFICATIONS_TABLE = 'odx_generic_tbl_audit_job_specifications'
AUDIT_LINE_TABLE = 'odx_generic_tbl_audit_control_line'
RESTART_TABLE = 'odx_generic_tbl_extract_manage_restartability'
TEMP_TABLE = 'odx_generic_tbl_extract_work_control_parameters'
AUDIT_HEADER_TABLE = 'odx_generic_tbl_audit_control_header'
ODW_EXTRACT_LOG = 'ODX_GENERIC_TBL_EXTRACT_LOG'
ODW_SCHEMA_EXTRACT_LOG = 'ODW_COMMON'

###Code Commit Test##


class Driver(object):
    def __init__(self):
        ##################Fetching Job Arguments#################################
        print(sys.argv)
        param_flag=None
        if '--odx_config_table_schema' in sys.argv and '--odx_config_table_name' in sys.argv and '--odx_config_id' in sys.argv:
            print("Params odx_config_table_schema, odx_config_table_name and odx_config_id are defined in job paramaters")
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'aurora_db_secret',
                                                 'region_name', 'odx_schema_name',
                                                 'odx_config_table_schema', 'odx_config_table_name', 'odx_config_id'])
        elif (('--{}'.format('odx_config_table_schema') in sys.argv) and ('--{}'.format('odx_config_table_name') not in sys.argv)) :
            param_flag='Exit'
            print("Params odx_config_table_schema is defined in job paramater ,odx_config_table_name is not defined in job paramater ")
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'aurora_db_secret',
                                                     'region_name', 'odx_schema_name',
                                                     'odx_config_table_schema'])
        elif (('--{}'.format('odx_config_table_name') in sys.argv) and ('--{}'.format('odx_config_table_schema') not in sys.argv)) :
            param_flag = 'Exit'
            print("Params odx_config_table_name is defined in job paramater ,odx_config_table_schema is not defined in job paramater ")
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'aurora_db_secret',
                                                     'region_name', 'odx_schema_name',
                                                     'odx_config_table_schema'])
        elif (('--{}'.format('odx_config_id') in sys.argv) and (('--{}'.format('odx_config_table_schema') not in sys.argv) or ('--{}'.format('odx_config_table_name') not in sys.argv))):
            param_flag='Exit'
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'aurora_db_secret',
                                                 'region_name', 'odx_schema_name',
                                                 'odx_config_id'])
            print("Params odx_config_table_name /odx_config_table_schema is not defined in job paramater ")
        elif (('--{}'.format('odx_config_table_name') in sys.argv) and (('--{}'.format('odx_config_table_schema') in sys.argv) )):
            param_flag='Exit'
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'aurora_db_secret',
                                                 'region_name', 'odx_schema_name',
                                                 'odx_config_table_schema','odx_config_table_name'])
            print("Params odx_config_table_name and odx_config_table_schema are defined in job paramater ")
        else:
            print("Params odx_config_table_schema, odx_config_table_name and odx_config_id are not defined")
            args = getResolvedOptions(sys.argv, ['JOB_NAME', 'WORKFLOW_NAME', 'WORKFLOW_RUN_ID', 'aurora_db_secret',
                                                 'region_name', 'odx_schema_name'])
            # setting this flag to refrain the code from re-checking for config params beneath
            param_flag=''
        print("args: {}".format(args))
        self.ingest_job_name = args['JOB_NAME']
        self.ingest_job_id = args['JOB_RUN_ID']
        self.workflow_name = args['WORKFLOW_NAME']
        self.workflow_run_id = args['WORKFLOW_RUN_ID']
        self.aurora_secret_name = args['aurora_db_secret']
        self.region_name = args['region_name']
        self.odx_schema_name = args['odx_schema_name']
        self.odx_config_table_schema = None
        self.odx_config_table_name = None
        self.odx_config_id = None
        # added as a part of code enhancement
        self.manual_flag = 'N'
        self.cloudwatch_event_name=None
        self.config_table_name=None
        self.odw_audit_update=False
        self.max_value=None

        print('workflow_name value: ', str(self.workflow_name))
        print('workflow_run_id value: ', str(self.workflow_run_id))

        ####################################### Logging ############################################

        # setting log messages
        MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
        DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
        logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
        self.logger = logging.getLogger(self.ingest_job_name)

        self.logger.setLevel(logging.INFO)
        self.logger.info('Program started!....')

        ###############Connecing to Clients using boto3##########################
        self.glue_client = boto3.client('glue')
        self.sm_client = boto3.client("secretsmanager", region_name=self.region_name)
        self.rds_data = boto3.client('rds-data')
        self.s3_client = boto3.client('s3')

        #############Getting Workflow Run Properties############################
        workflow_params = \
            self.glue_client.get_workflow_run_properties(Name=self.workflow_name, RunId=self.workflow_run_id)[
                "RunProperties"]
        print(workflow_params)
        # Added part of enhancements
        if 'manual_flag' not in workflow_params:
            self.logger.info('Manual Flag is set to Y')
            self.manual_flag='Y'

        self.logger.info('Value of manual_flag is: {}'.format(self.manual_flag))
        if self.manual_flag == 'Y':
            # setting workflow level parameters for manual run.
            if 'odx_config_table_schema' in workflow_params and workflow_params['odx_config_table_schema'] != '' \
                    and workflow_params['odx_config_table_schema'] != '0':
                self.logger.info('odx_config_table_schema parameter exists in workflow params: {}'.format(workflow_params['odx_config_table_schema']))

                self.schema_name = workflow_params['odx_config_table_schema']
                if 'odx_config_table_name' in workflow_params and workflow_params['odx_config_table_name'] != '' \
                         and workflow_params['odx_config_table_name'] != '0':
                    self.logger.info('odx_config_table_name parameter exists in workflow params: {}'.format(workflow_params['odx_config_table_name']))
                    self.table_name = workflow_params['odx_config_table_name']
                else:
                    self.logger.info('workflow parameter odx_config_table_name for manual run not properly mentioned/missing')
                    sys.exit(1)

                if 'odx_config_id' in workflow_params and workflow_params['odx_config_id'] != '' \
                        and workflow_params['odx_config_id'] != '0':
                    self.logger.info('odx_config_id parameter exists in workflow params')

                    self.config_id = workflow_params['odx_config_id']
                else:
                    self.logger.info('odx_config_id is null at workflow level')
                    self.config_id = None
            elif param_flag!='' or param_flag=='Exit':
                self.logger.info('when params are set at job level')
                if 'odx_config_table_schema' in args and args['odx_config_table_schema'] != '0' :
                    self.logger.info('odx_config_table_schema parameter exists in job params:')
                    self.schema_name = args['odx_config_table_schema']
                    if 'odx_config_table_name' in args and args['odx_config_table_name'] != '0' :
                        self.table_name = args['odx_config_table_name']
                        self.logger.info('odx_config_table_name parameter exists in job params')
                    else:
                        self.table_name = None
                        self.logger.info('odx_config_table_name not present in job parameters/not defined properly')
                        sys.exit(1)

                    if 'odx_config_id' in args and args['odx_config_id'] != '0':
                        self.config_id = args['odx_config_id']
                    else:
                        self.config_id = None
                        self.logger.info('odx_config_id is null')
                else:
                    self.logger.info('odx_config_table_schema parameter does not exist in job params:')
                    sys.exit(1)
            else:
                self.schema_name = None
                self.logger.info('No odx_config_table_schema present at workflow/job level')
                sys.exit(1)
            self.config_table_name = CONFIG_TABLE_MANUAL


        else:
            self.table_name = None
            self.schema_name = None
            self.config_id = None
            self.config_table_name = CONFIG_TABLE
            if workflow_params['cloudwatch_event_name']=='':
                self.logger.info('No cloudwatch event present at in rds config for scheduled run')
                sys.exit(1)
            else:
                self.cloudwatch_event_name = workflow_params['cloudwatch_event_name']

        ####################################### Spark job/Ojbects ###################################
        self.sc = SparkContext()
        self.glueContext = GlueContext(self.sc)
        self.spark = self.glueContext.spark_session
        self.job = Job(self.glueContext)

        ####################################### Aurora MySQL Connection ################################

        sm_response = self.sm_client.get_secret_value(SecretId=self.aurora_secret_name)
        aurora_secret = sm_response['SecretString']
        aurora_secret = json.loads(aurora_secret)

        self.aurora_uname = aurora_secret.get('username')
        self.aurora_pwd = aurora_secret.get('password')
        self.aurora_jdbcurl = aurora_secret.get('jdbcurl')
        self.aurora_driver = aurora_secret.get('driver')
        self.aurora_dbname = aurora_secret.get('dbname')
        self.aurora_secret_arn = aurora_secret.get('secretARN')
        self.aurora_resource_arn = aurora_secret.get('resourcename(ARN)')

        print('post setting up connections for RDS')
        
        ################Calling Read RDS##############################
        readrdsobj = Readrds(self.manual_flag, self.schema_name, self.table_name, self.config_id,
                             self.cloudwatch_event_name, self.logger, self.spark, self.aurora_uname, self.aurora_pwd,
                             self.aurora_jdbcurl, self.aurora_driver, self.aurora_dbname, self.odx_schema_name,self.config_table_name)
        formatted_arguments = readrdsobj.get_rds()
        print('printing formatted arguments')
        print(formatted_arguments)
        
        ####################################### SAP Connection ############################################
        get_secret_value_response = self.sm_client.get_secret_value(
            SecretId=formatted_arguments['source_db_secret']
        )
        secret = get_secret_value_response['SecretString']
        secret = json.loads(secret)
        print("Printing secrets: {}".format(secret))
        self.db_username = secret.get('db_username')
        self.db_password = secret.get('db_password')
        self.db_url = secret.get('db_url')
        self.jdbc_driver_name = secret.get('driver_name')

        print('post setting up connections for sap')

        ##############Calling Audit###################################
        auditobj = Auditcheck(self.logger, self.spark, self.aurora_uname, self.aurora_pwd, self.aurora_jdbcurl,
                              self.aurora_driver, self.aurora_dbname, self.ingest_job_name, self.schema_name,
                              self.table_name, self.rds_data, self.aurora_resource_arn, self.aurora_secret_arn,
                              self.workflow_run_id, self.workflow_name, self.glue_client,
                              self.ingest_job_id, self.odx_schema_name, formatted_arguments['id'])
        audit_id, audit_line_id = auditobj.write_audit_spec(formatted_arguments['schema_name'],
                                                            formatted_arguments['table_name'])
        print(audit_id)
        print(audit_line_id)

        auditobj.update_audit_header(audit_id,0,0,0)
        auditobj.write_audit_header(audit_id)
        auditobj.update_audit_line(audit_id, audit_line_id)
        auditobj.write_audit_line(audit_id, audit_line_id)

        self.logger.info('Checking restart table for an last job not completed')
        restartobj = Restart(self.manual_flag, self.spark, self.aurora_jdbcurl, self.aurora_driver,
                             self.aurora_uname, self.aurora_pwd, self.logger, self.rds_data, self.aurora_resource_arn,
                             self.aurora_secret_arn, self.aurora_dbname, self.odx_schema_name,
                             formatted_arguments['id'])
        exit_flag = restartobj.restart(formatted_arguments['schema_name'],
                                                       formatted_arguments['table_name'])

        workflow = Workflow(self.workflow_name, self.workflow_run_id, self.logger, self.glue_client,self.config_id)
        workflow.put_wf_properties(formatted_arguments['table_name'],formatted_arguments['schema_name'],formatted_arguments['crawler_name'])

        if exit_flag == 'N':
            self.logger.info('Delete entry from restart table after restarting job.')
            restartobj.delete_restart_step()
            self.logger.info('Initialize extract data step')
            extractobj = Extractdata(self.manual_flag, self.logger, self.spark,
                                     self.glueContext, self.jdbc_driver_name, self.db_url, self.db_username,
                                     self.db_password, self.aurora_jdbcurl, self.aurora_driver,
                                     self.aurora_uname, self.aurora_pwd, self.rds_data, self.aurora_resource_arn,
                                     self.aurora_secret_arn, self.aurora_dbname, self.ingest_job_name,
                                     self.odx_schema_name, formatted_arguments['id'], formatted_arguments['cdc_column'],
                                     formatted_arguments['cdc_timestamp'], formatted_arguments['cdc_ts_format'],formatted_arguments['to_cdc_timestamp'])

            print('printing source partition column', str(formatted_arguments['source_partition_column']))
            print('printing query parameter', str(formatted_arguments['query_parameter']))
            print('printing load stratergy', str(formatted_arguments['load_strategy']))
            print('printing schema name', str(formatted_arguments['schema_name']))
            print('printing table name', str(formatted_arguments['table_name']))
            print('printing audit_id', str(audit_id))

            # 			self.logger.info('Reading partition key values from source')
            partition_list = []
            if formatted_arguments['source_partition_column']:
                print('going to start source partitioning')
                partition_list = extractobj.read_sap_partition(formatted_arguments['source_partition_column'],
                                                               formatted_arguments['query_parameter'],
                                                               formatted_arguments['load_strategy'],
                                                               formatted_arguments['schema_name'],
                                                               formatted_arguments['table_name'],
                                                               formatted_arguments['agg_column'],
                                                               formatted_arguments['agg_column_function'],
                                                               formatted_arguments['agg_operation'])
                print(partition_list)

                # if partition_path:
                #     deleteobj=Deleteobjects(self.s3_client)
                #     deleteobj.delete_objects(partition_path,formatted_arguments['curate_bucket'],formatted_arguments['curate_path'])
                # print(f"Partition list is as follows: {partition_list}")
            self.logger.info('Extracting data from source')
            extracted, ingestion_cdc_timestamp, cnt_read, cnt_write, extract_error, to_cdc_tmstmp  = extractobj.extract(partition_list,
                                                                                                                       formatted_arguments['load_strategy'],
                                                                                                                       formatted_arguments['query_parameter'],
                                                                                                                       formatted_arguments['select_columns'],
                                                                                                                       formatted_arguments['primary_keys'],
                                                                                                                       formatted_arguments['target_partition_column'],
                                                                                                                       formatted_arguments['curate_path'],
                                                                                                                       formatted_arguments['schema_name'],
                                                                                                                       formatted_arguments['table_name'],
                                                                                                                       formatted_arguments['agg_column'],
                                                                                                                       formatted_arguments['agg_column_function'],
                                                                                                                       formatted_arguments['agg_operation'],
                                                                                                                       formatted_arguments['source_partition_column']
                                                                                                                       )

            print('printing extract flag and timestamp')
            print(extracted)
            print(ingestion_cdc_timestamp)
            print('to_cdc_tmstmp: ',format(to_cdc_tmstmp))
            print ('source count: ',cnt_read)
            print ('target count: ', cnt_write)

            auditobj.update_audit_header(audit_id,1,cnt_read,cnt_write)
            if extracted:
                if formatted_arguments['load_strategy'] in ('IO-SNAP', 'IO-CDC','TL-CDC','IO-SNAP-DLY','IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                    cdc_timestamp = formatted_arguments['cdc_timestamp']
                    cdc_timestamp_fmt = formatted_arguments['cdc_ts_format']
                    to_cdc_timestamp = formatted_arguments['to_cdc_timestamp']
                    
                    print("Current cdc_timestamp: {}".format(cdc_timestamp))
                    print("Current to_cdc_timestamp: {}".format(to_cdc_timestamp))
                    
                    
                    if formatted_arguments['load_strategy'] in ('IO-SNAP'):
                        cdc_timestamp = str(cdc_timestamp + timedelta(days=1))
                        to_cdc_timestamp = 'NULL'
                    elif formatted_arguments['load_strategy'] in ('IO-SNAP-DLY'):
                        cdc_timestamp = str(to_cdc_timestamp + timedelta(days=1)).split(' ')[0] + " 00:00:00"
                        to_cdc_timestamp = str(to_cdc_timestamp + timedelta(days=1))
                        print("New cdc_timestamp: {}".format(cdc_timestamp))
                        print("New to_cdc_timestamp: {}".format(to_cdc_timestamp))
                    elif formatted_arguments['load_strategy'] in ('IO-CDC','TL-CDC'):
                        cdc_timestamp = ingestion_cdc_timestamp
                        to_cdc_timestamp = 'NULL'
                    elif formatted_arguments['load_strategy'] in ('IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                        self.odw_audit_update, self.max_value = auditobj.write_odw_audit(partition_list, formatted_arguments['schema_name'], formatted_arguments['table_name'], formatted_arguments['cdc_column'], self.jdbc_driver_name, self.db_url, ODW_SCHEMA_EXTRACT_LOG, ODW_EXTRACT_LOG, self.db_username, self.db_password, audit_id, formatted_arguments['load_strategy'], to_cdc_tmstmp)
                        print("odw_audit_update value: {}".format(self.odw_audit_update))
                        print("max_value value: {}".format(self.max_value))
                        
                        if self.odw_audit_update:
                            if formatted_arguments['load_strategy'] in ('IO-CUSTOM-CDC'):
                                cdc_timestamp = str(to_cdc_tmstmp + timedelta(days=1)).split(' ')[0] + " 00:00:00"
                                to_cdc_timestamp = str(to_cdc_tmstmp + timedelta(days=1)).split(' ')[0] + " 23:59:59"
                            else:
                                cdc_timestamp = str(self.max_value + timedelta(days=1)).split(' ')[0] + " 00:00:00"
                                to_cdc_timestamp = str(self.max_value + timedelta(days=1)).split(' ')[0] + " 23:59:59"
                            print("New cdc_timestamp: {}".format(cdc_timestamp))
                            print("New to_cdc_timestamp: {}".format(to_cdc_timestamp))
                        else:
                            cdc_timestamp = None

                    print("cdc_timestamp: {}".format(cdc_timestamp))
                    if cdc_timestamp is not None :
                        workobj = Worktable(self.logger, self.rds_data, self.aurora_resource_arn, self.aurora_secret_arn,
                                            self.aurora_dbname, self.odx_schema_name)
                        workobj.insert_temp_table(formatted_arguments['id'], cdc_timestamp, cdc_timestamp_fmt,to_cdc_timestamp)

                    self.logger.info("Extraction finished successfully for the table_name=%s and schema_name=%s",
                                     formatted_arguments['table_name'], formatted_arguments['schema_name'])
                    
                else:
                    self.logger.info("Extraction finished successfully for the table_name=%s and schema_name=%s",
                                     formatted_arguments['table_name'], formatted_arguments['schema_name'])
            else:
                if extract_error: 
                    self.logger.info("Extraction did not happen properly for the table_name=%s and schema_name=%s",
                                 formatted_arguments['table_name'], formatted_arguments['schema_name'])
                    sys.exit(1)
                else:
                    self.logger.info("No data found for the table_name=%s and schema_name=%s",
                                 formatted_arguments['table_name'], formatted_arguments['schema_name'])

class Readrds(object):
    def __init__(self, manual_flag, schema_name, table_name, config_id, cloudwatch_event_name, logger, spark,
                 aurora_uname, aurora_pwd, aurora_jdbcurl, aurora_driver, aurora_dbname, odx_schema_name,config_table_name):
        self.manual_flag = manual_flag
        self.schema_name = schema_name
        self.table_name = table_name
        self.config_id = config_id
        self.cloudwatch_event_name = cloudwatch_event_name
        self.logger = logger
        self.spark = spark
        self.aurora_uname = aurora_uname
        self.aurora_pwd = aurora_pwd
        self.aurora_jdbcurl = aurora_jdbcurl
        self.aurora_driver = aurora_driver
        self.aurora_dbname = aurora_dbname
        self.odx_schema_name = odx_schema_name
        self.odx_config_table_name = config_table_name

    def get_rds(self):
        try:
            # added as a part of code enhancement
            if self.config_id  is None and self.manual_flag == 'Y':
                where_clause_manual_string = "schema_name={} and table_name={}".format(repr(self.schema_name),repr(self.table_name))
            elif self.config_id  is not None and self.manual_flag == 'Y':
                where_clause_manual_string = "schema_name={} and table_name={} and id = {}".format(repr(self.schema_name),repr(self.table_name),repr(self.config_id))
            else:
                where_clause_manual_string ="1=1"
            
            if self.cloudwatch_event_name is  None:
                where_clause_schedule_string = "1=1"
            else:
                where_clause_schedule_string = "cloudwatch_event_name={}".format(repr(self.cloudwatch_event_name))

            config_tbl_query = "SELECT * from {}.".format(
                        self.odx_schema_name) + "{} where active_flag = 'Y' and manual_flag = {} and case when manual_flag='Y' then {}  else {} end".format(self.odx_config_table_name,repr(self.manual_flag),where_clause_manual_string
                        ,where_clause_schedule_string)
            print(config_tbl_query)


            SELECT_COLUMNS = ['id', 'select_columns', 'curate_prefix',
                              'source_partition_column',
                              "schema_name", 'table_name', 'primary_keys', 'cdc_column', 'cdc_timestamp',
                              'query_parameter', 'curate_bucket',  'cdc_ts_format',
                              'manual_flag', 'target_partition_column',
                              'load_strategy', 'overlap_in_min', 'agg_column', 'agg_column_function', 'agg_operation','crawler_name','to_cdc_timestamp','source_db_secret']
            self.logger.info("Config table query %s", config_tbl_query)
            config_df = self.spark.read.format("jdbc").option("url", self.aurora_jdbcurl).option(
                "driver", self.aurora_driver).option("query", config_tbl_query).option("user",
                                                                                       self.aurora_uname).option(
                "password",
                self.aurora_pwd).load()
            print("Reading config Table")
            query_df = config_df.select(*SELECT_COLUMNS)
            print(query_df.count())
            if query_df.count() == 0:
                self.logger.info('No matching RDS Config found for tablename: {}, schemaname: {}, config_id: {} , cloudwatch_event_name: {}'.format(
                    self.table_name, self.schema_name, self.config_id, self.cloudwatch_event_name))
                raise Exception('No matching RDS Config found for tablename, schemaname, config_id')

            if query_df.count() > 1:
                self.logger.info(
                    'More than 1 matching RDS Config found for tablename: {}, schemaname: {}, config_id: {}, cloudwatch_event_name: {}'.format(
                        self.table_name, self.schema_name, self.config_id, self.cloudwatch_event_name))
                raise Exception('More than 1 matching RDS Config found for tablename, schemaname, config_id')

            #query_df = query_df.withColumn('query_parameter', regexp_replace('query_parameter', '\\{', ' '))
            #query_df = query_df.withColumn('query_parameter', regexp_replace('query_parameter', '\\}', ' '))
            #query_df = query_df.withColumn('query_parameter', regexp_replace('query_parameter', '\\:', ' in '))
            #query_df = query_df.withColumn('query_parameter', regexp_replace('query_parameter', '\\,\\(', ' and '))
            #query_df = query_df.withColumn('query_parameter', regexp_replace('query_parameter', '\\)\\,', ') and '))
            query_df = query_df.withColumn('query_parameter', coalesce(sf.col('query_parameter'), sf.lit(' 1=1 ')))
            query_df = query_df.withColumn('cdc_column', regexp_replace('cdc_column', '\\+', "\\|\\|\\' \\'\\|\\|"))
            
            try:
                id = query_df.first()['id']
                print("id value is: {}".format(id))
                print('no id')
            except Exception as e:
                print('passed id')
                sys.exit(1)
                pass

            try:
                overlap_min = query_df.first()['overlap_in_min']
                print('no overlap_in_min')
            except Exception as e:
                print('passed overlap')
                overlap_min=None
                pass

            if overlap_min:
                overlap_min = "INTERVAL " + query_df.first()['overlap_in_min'] + " minutes"
                query_df = query_df.withColumn('cdc_timestamp', col("cdc_timestamp") - expr(overlap_min))



            self.logger.info("Reading config table parameters...")
            try:
                query_parameter = query_df.first()['query_parameter']
            except Exception as e:
                print('passed parameters for query_parameter method')
                query_parameter = None
                pass

            try:
                select_columns = query_df.first()['select_columns']
            except Exception as e:
                print('passed parameters for select_columns method')
                select_columns = None
                pass
            try:
                source_partition_column = query_df.first()['source_partition_column']
            except Exception as e:
                print('passed parameters for source_partition_column method')
                source_partition_column = None
                pass
            primary_keys = []
            try:
                if query_df.first()['primary_keys']:
                    primary_keys = list(column.strip() for column in (query_df.first()['primary_keys']).split(','))
            except Exception as e:
                print('finally reached primary key')
                primary_keys = None
                pass
            try:
                cdc_column = query_df.first()['cdc_column']
            except Exception as e:
                print('passed parameters for cdc_column method')
                cdc_column = None
                pass

            try:
                cdc_timestamp = query_df.first()['cdc_timestamp']
            except Exception as e:
                print('passed parameters for cdc_timestamp method')
                cdc_timestamp = None
                pass
            try:
                cdc_ts_format = query_df.first()['cdc_ts_format']
            except Exception as e:
                print('passed parameters for cdc_ts_format method')
                cdc_ts_format = None
                pass

            try:
                curate_prefix = query_df.first()['curate_prefix']
            except Exception as e:
                print('passed parameters for curate_prefix method')
                curate_prefix = None
                pass
            try:
                curate_bucket = query_df.first()['curate_bucket']
            except Exception as e:
                print('passed parameters for curate_bucket method')
                curate_bucket = None
                pass
            curate_path = 's3://'+curate_bucket+'/'+curate_prefix+'/'
            try:
                target_partition_column = list(
                    column.strip() for column in (config_df.first()['target_partition_column']).split(','))
            except Exception as e:
                print('passed parameters for target_partition_column method')
                target_partition_column = None
                pass
            try:
                load_strategy = query_df.first()['load_strategy']
            except Exception as e:
                print('passed parameters for load_strategy method')
                load_strategy = None
                pass
            try:
                schema_name = query_df.first()['schema_name']
            except Exception as e:
                print('passed parameters for schema_name method')
                schema_name = None
                pass
            try:
                table_name = query_df.first()['table_name']
            except Exception as e:
                print('passed parameters for table_name method')
                table_name = None
                pass
            try:
                agg_column = query_df.first()['agg_column']
            except Exception as e:
                agg_column = None
                pass
            try:
                agg_column_function = query_df.first()['agg_column_function']
            except Exception as e:
                agg_column_function = None
                pass
            try:
                agg_operation = query_df.first()['agg_operation']
            except Exception as e:
                agg_operation = None
                pass

            try:
                crawler_name =query_df.first()['crawler_name']
            except Exception as e:
                crawler_name=None
                pass
            try:
                to_cdc_timestamp = query_df.first()['to_cdc_timestamp']
            except Exception as e:
                print('passed parameters for to_cdc_timestamp method')
                to_cdc_timestamp = None
                pass
            
            try:
                source_db_secret = query_df.first()['source_db_secret']
                print("source_db_secret value is: {}".format(source_db_secret))
                
                if source_db_secret is None:
                    print("No value found for source_db_secret")
                    raise Exception('Need to set value for source_db_secret in config')
            except Exception as e:
                print(e)
                print('Error while retrieving value for source_db_secret')
                sys.exit(1)

            print('inside finding config parameters')

            formatted_arguments = {}
            formatted_arguments['id'] = id
            formatted_arguments['source_partition_column'] = source_partition_column
            formatted_arguments['query_parameter'] = query_parameter
            formatted_arguments['cdc_column'] = cdc_column
            formatted_arguments['cdc_ts_format'] = cdc_ts_format
            formatted_arguments['cdc_timestamp'] = cdc_timestamp
            formatted_arguments['load_strategy'] = load_strategy
            formatted_arguments['primary_keys'] = primary_keys
            formatted_arguments['select_columns'] = select_columns
            formatted_arguments['target_partition_column'] = target_partition_column
            formatted_arguments['curate_path'] = curate_path
            formatted_arguments['schema_name'] = schema_name
            formatted_arguments['table_name'] = table_name
            formatted_arguments['agg_column'] = agg_column
            formatted_arguments['agg_column_function'] = agg_column_function
            formatted_arguments['agg_operation'] = agg_operation
            formatted_arguments['crawler_name'] = crawler_name
            formatted_arguments['to_cdc_timestamp'] = to_cdc_timestamp
            formatted_arguments['source_db_secret'] = source_db_secret

            return formatted_arguments
        except Exception as e:
            self.logger.error("Failed while reading RDS Config")
            print(e)
            sys.exit(1)


class Auditcheck(object):
    def __init__(self, logger, spark, aurora_uname, aurora_pwd, aurora_jdbcurl, aurora_driver, aurora_dbname,
                 ingest_job_name, schema_name, table_name, rds_data, aurora_resource_arn, aurora_secret_arn,
                 workflow_run_id, workflow_name, glue_client,
                 ingest_job_id, odx_schema_name, config_id):
        self.logger = logger
        self.spark = spark
        self.aurora_uname = aurora_uname
        self.aurora_pwd = aurora_pwd
        self.aurora_jdbcurl = aurora_jdbcurl
        self.aurora_driver = aurora_driver
        self.aurora_dbname = aurora_dbname
        self.ingest_job_name = ingest_job_name
        self.rds_data = rds_data
        self.aurora_resource_arn = aurora_resource_arn
        self.aurora_secret_arn = aurora_secret_arn
        self.workflow_run_id = workflow_run_id
        self.workflow_name = workflow_name
        self.glue_client = glue_client
        self.ingest_job_id = ingest_job_id
        self.odx_schema_name = odx_schema_name
        self.odx_config_id = config_id
        self.src_rec_count = 0
        self.tgt_rec_count = 0

    def write_audit_spec(self, schema_name, table_name):
        try:
            self.logger.info("Inserting audit job specifications table")
            audit_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

            audit_spec_query = "SELECT * from {}.{} where schema_name='{}' and table_name='{}' and job_name='{}' and audit_id='{}' ".format(
                self.odx_schema_name, AUDIT_JOB_SPECIFICATIONS_TABLE, schema_name, table_name, self.ingest_job_name,
                self.odx_config_id)
            audit_spec_df = self.spark.read.format("jdbc").option("url", self.aurora_jdbcurl).option(
                "driver", self.aurora_driver).option("query", audit_spec_query).option("user",
                                                                                       self.aurora_uname).option(
                "password", self.aurora_pwd).load()

            print(audit_spec_df.count())
            if audit_spec_df.count() == 0:
                insert_audit_spec_query = "insert into {}.{} values('{}',md5(concat(upper('{}'),upper('{}'),upper('{}'))),'{}','Ingesting into S3 raw','{}','{}','{}') ".format(
                    self.odx_schema_name, AUDIT_JOB_SPECIFICATIONS_TABLE, self.odx_config_id, self.ingest_job_name,
                    schema_name, table_name,
                    self.ingest_job_name, schema_name, table_name, audit_timestamp)
                self.logger.info("Audit job specification insert query: %s", insert_audit_spec_query)
                audit_spec_response = self.rds_data.execute_statement(resourceArn=self.aurora_resource_arn,
                                                                      secretArn=self.aurora_secret_arn,
                                                                      database=self.aurora_dbname,
                                                                      sql=insert_audit_spec_query)
                self.logger.info("Inserting to an audit specification table response %s", audit_spec_response)
                audit_spec_query = "SELECT * from {}.{} where schema_name='{}' and table_name='{}' and job_name='{}' and audit_id='{}' ".format(
                    self.odx_schema_name, AUDIT_JOB_SPECIFICATIONS_TABLE, schema_name, table_name, self.ingest_job_name,
                    self.odx_config_id)
                audit_spec_df = self.spark.read.format("jdbc").option("url", self.aurora_jdbcurl).option(
                    "driver", self.aurora_driver).option("query", audit_spec_query).option("user",
                                                                                           self.aurora_uname).option(
                    "password",
                    self.aurora_pwd).load()

            else:
                self.logger.info(
                    "Record already present in audit specification table for this table_name=%s, schema_name=%s, job_name=%s and audit_id=%s",
                    table_name, schema_name, self.ingest_job_name, self.odx_config_id)

            audit_id = audit_spec_df.first()['audit_id']
            audit_line_id = audit_spec_df.first()['audit_line_job_id']
            print(audit_id)
            return audit_id, audit_line_id

        except Exception as e:
            self.logger.error("Inserting audit job specifications failed")
            print(e)
            sys.exit(1)

    def update_audit_header(self, audit_id, flag, cnt_read, cnt_write):
        try:
            if flag == 0:
                self.logger.info("Updating audit header table for the latest record indicator to 0")
                update_query = "update {}.{} set latest_record_ind=0 where audit_id='{}' and workflow_run_id<>'{}' and workflow_header_status!='RUNNING'".format(
                    self.odx_schema_name, AUDIT_HEADER_TABLE, audit_id, self.workflow_run_id)
                print("Audit header update query", update_query)
                audit_header_response = self.rds_data.execute_statement(resourceArn=self.aurora_resource_arn,
                                                                        secretArn=self.aurora_secret_arn,
                                                                        database=self.aurora_dbname,
                                                                        sql=update_query)
                self.logger.info("Updated audit header table response", audit_header_response)
            elif flag == 1:
                self.logger.info("Updating audit header table for the source_record_count and target_record_count")
                update_query = "update {}.{} set src_record_count = {}, target_record_count = {} where audit_id= '{}' and latest_record_ind='1' and workflow_run_id='{}'".format(
                    self.odx_schema_name, AUDIT_HEADER_TABLE, cnt_read, cnt_write, audit_id, self.workflow_run_id)
                print("update_query = {}".format(update_query))
                print ("source_record_count and target_record_count updated")
                audit_header_response = self.rds_data.execute_statement(resourceArn=self.aurora_resource_arn,
                                                                        secretArn=self.aurora_secret_arn,
                                                                        database=self.aurora_dbname,
                                                                        sql=update_query)
                self.logger.info("Updated source_record_count and target_record_count")

        except Exception as e:
            self.logger.error("Updating latest record indicator failed for the audit header table")
            print(e)
            sys.exit(1)

    def write_audit_header(self, audit_id):
        try:
            self.logger.info("Inserting into audit header table..")
            workflow_response = self.glue_client.get_workflow_run(Name=self.workflow_name, RunId=self.workflow_run_id,
                                                                  IncludeGraph=False
                                                                  )
            workflow_start_ts = str(workflow_response['Run']['StartedOn']).split('.')[0]
            workflow_status = workflow_response['Run']['Status']
            latest_record = 1
            audit_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')


            insert_query = "insert into {}.{} (audit_id, insert_timestamp, latest_record_ind, workflow_run_id, workflow_header_status,workflow_start_ts) values('{}','{}','{}','{}','{}','{}') ".format(
                self.odx_schema_name, AUDIT_HEADER_TABLE, audit_id, audit_timestamp, latest_record,
                self.workflow_run_id, workflow_status,
                workflow_start_ts)
            print("workflow insert header query", insert_query)
            audit_header_response = self.rds_data.execute_statement(resourceArn=self.aurora_resource_arn,
                                                                    secretArn=self.aurora_secret_arn,
                                                                    database=self.aurora_dbname,
                                                                    sql=insert_query)

            self.logger.info("Inserting audit header table response", audit_header_response)
        except Exception as e:
            self.logger.error("Inserting into audit header table failed")
            print(e)
            sys.exit(1)

    def update_audit_line(self, audit_id, audit_line_job_id):
        try:
            self.logger.info("Updating audit line table for the latest record indicator to 0")
            update_query = "update {}.{} set latest_record_ind=0 where audit_id={} and audit_line_job_id={} and job_status!='RUNNING'".format(
                self.odx_schema_name, AUDIT_LINE_TABLE, repr(audit_id), repr(audit_line_job_id))

            audit_line_response = self.rds_data.execute_statement(resourceArn=self.aurora_resource_arn,
                                                                  secretArn=self.aurora_secret_arn,
                                                                  database=self.aurora_dbname,
                                                                  sql=update_query)
            self.logger.info("Updated audit line table response", audit_line_response)

        except Exception as e:
            self.logger.error("Updating latest record indicator failed for the audit line table")
            print(e)
            sys.exit(1)

    def write_audit_line(self, audit_id, audit_line_job_id):
        try:
            self.logger.info("Inserting into audit line table..")
            job_response = self.glue_client.get_job_run(JobName=self.ingest_job_name, PredecessorsIncluded=False,
                                                        RunId=self.ingest_job_id)

            job_start_ts = str(job_response['JobRun']['StartedOn']).split('.')[0]
            job_run_id = job_response['JobRun']['Id']
            job_status = job_response['JobRun']['JobRunState']
            latest_record_ind = 1
            audit_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')


            insert_query = "insert into {}.{} (audit_id,audit_line_job_id,job_start_ts,job_run_id,job_status,latest_record_ind,insert_timestamp) values('{}','{}','{}','{}','{}','{}','{}') ".format(
                self.odx_schema_name, AUDIT_LINE_TABLE, audit_id, audit_line_job_id, job_start_ts, job_run_id,
                job_status, latest_record_ind,
                audit_timestamp)

            audit_line_response = self.rds_data.execute_statement(resourceArn=self.aurora_resource_arn,
                                                                  secretArn=self.aurora_secret_arn,
                                                                  database=self.aurora_dbname,
                                                                  sql=insert_query)

            self.logger.info("Inserting audit line table response", audit_line_response)

        except Exception as e:
            self.logger.error("Inserting into audit line table failed")
            print(e)
            sys.exit(1)
            
    def get_audit_header_src_tgt_count(self, audit_id):
        try:
            self.logger.info("Checking in audit header table for source and target record count..")
            
            select_hdr_query = "select src_record_count,target_record_count from {}.{} where latest_record_ind='1' and audit_id='{}' and workflow_run_id='{}'".format(
                self.odx_schema_name, AUDIT_HEADER_TABLE, audit_id,self.workflow_run_id)
            print(select_hdr_query)

            audit_hdr_response = self.rds_data.execute_statement(resourceArn=self.aurora_resource_arn,
                                                                 secretArn=self.aurora_secret_arn,
                                                                 database=self.aurora_dbname,
                                                                 sql=select_hdr_query)


            print(audit_hdr_response)
            self.src_rec_count = audit_hdr_response['records'][0][0]['longValue']
            self.tgt_rec_count = audit_hdr_response['records'][0][1]['longValue']
            
            print("src_rec_count: {}".format(self.src_rec_count))
            print("tgt_rec_count: {}".format(self.tgt_rec_count))

            self.logger.info("Retrieval of source and target record count from audit header table successful")
        except Exception as e:
            self.logger.error("Retrieval of source and target record count from audit header table failed!!!!")
            self.src_rec_count = 0
            self.tgt_rec_count = 0
            print(e)
            sys.exit(1)
            
    def write_odw_audit(self, partition_list, odw_schema_name, odw_table_name, odw_cdc_column, odw_jdbc_driver_name, odw_db_url, odw_schema_extract_log, odw_extract_log, odw_db_username, odw_db_password, audit_id, load_strategy, update_date):
        try:
            self.logger.info("Call method for getting source and target record count from audit header table")
            self.get_audit_header_src_tgt_count(audit_id)
            odw_audit_update = False
            max_value = None
            audit_rec_present = 0
            stripped_cdc_column = '"' + odw_cdc_column + '"' + '='
            print("cdc_column: {}".format(stripped_cdc_column))
            current_timestamp = datetime.datetime.fromtimestamp(time.time())
            print("current_timestamp: {}".format(current_timestamp))
            
            port=odw_db_url.split(':')[-1].split('/')[0]
            hana_url=odw_db_url.split(':')[2].replace('//','')
        
            print('port',port)
            print('hana url',hana_url)
            
            new_list = []
            if self.src_rec_count !=0 and self.tgt_rec_count !=0 and self.src_rec_count == self.tgt_rec_count:
                self.logger.info("Source record count matches Target record count and is non-zero....")
                odw_audit_query = "SELECT * FROM {}.{} WHERE SCHEMA_NAME='{}' AND TABLE_NAME='{}'".format(odw_schema_extract_log,odw_extract_log,odw_schema_name,odw_table_name)
                print(odw_audit_query)
            
                df_sap_audit_read = self.spark.read.format("jdbc").option("driver", odw_jdbc_driver_name).option("url",odw_db_url).option("query", odw_audit_query).option("user", odw_db_username).option("password", odw_db_password).option("fetchsize",100000).load()
                audit_rec_present = df_sap_audit_read.count()
                print("Record count from ODW Audit table for schema and table: {}".format(audit_rec_present))
                
                self.logger.info("Logging into ODW Audit table started....")
                if load_strategy == 'IO-CUSTOM-CDC':
                    update_date = update_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    new_list.append(current_timestamp)
                    print("new_list: {}".format(new_list))
                else:
                    new_list=list(map(lambda x: datetime.datetime.strptime(x.replace(stripped_cdc_column,'').lstrip('\'').rstrip('\''), '%Y-%m-%d'),partition_list))
                print("new_List values in datetime: {}".format(new_list))
                
                if len(new_list) != 0:
                    new_list.sort(reverse=True)
                    max_value = new_list[0]
                    print("Max value is: {}".format(max_value))
                    
                    if load_strategy == 'IO-CUSTOM-CDC':
                        self.logger.info("For load strategy: {}, the max extracted date is the delta end date and not the max date available in source ODW table".format(load_strategy))
                        max_value = update_date
                    
                    self.logger.info("Opening a connection to SAP Hana using hdbcli....")
                    conn = dbapi.connect(address=hana_url,port=port,user=odw_db_username, password=odw_db_password)
                    self.logger.info("Successfully opened connection to SAP Hana using hdbcli....")
                    
                    self.logger.info("Opening a cursor with SAP Hana....")
                    cursor = conn.cursor()
                    self.logger.info("Successfully opened cursor with SAP Hana....")
                    
                    if audit_rec_present != 0:
                        odw_audit_query = "UPDATE {}.{} SET DELTA_COMPLETION_DATE='{}',LAST_UPDATE_TIMESTAMP='{}' WHERE SCHEMA_NAME='{}' AND TABLE_NAME='{}'".format(odw_schema_extract_log,odw_extract_log,max_value,current_timestamp,odw_schema_name,odw_table_name)
                    else:
                        odw_audit_query = "INSERT INTO {}.{} VALUES('{}','{}','{}','{}','COMPLETED','{}')".format(odw_schema_extract_log,odw_extract_log,odw_schema_name,odw_table_name,odw_cdc_column,max_value,current_timestamp)
                    
                    print("odw_audit_query: {}".format(odw_audit_query))
                    conn.setautocommit(False)
                    cursor.execute(odw_audit_query)
                    
                    conn.commit()
                    cursor.close()
                    conn.close()
                    self.logger.info("Logging into ODW Audit table successfully completed")
                    odw_audit_update = True
            else:
                self.logger.info("Source record count matches Target record count do not match or maybe zero indicating loading error....")
                
            return odw_audit_update, max_value
        except Exception as e:
            self.logger.error("Inserting into ODW audit table failed")
            print(e)
            cursor.close()
            conn.close()
            sys.exit(1)


class Restart(object):
    def __init__(self, manual_flag, spark, aurora_jdbcurl, aurora_driver, aurora_uname, aurora_pwd, logger,
                 rds_data, aurora_resource_arn, aurora_secret_arn, aurora_dbname, odx_schema_name, config_id):
        self.manual_flag = manual_flag
        self.spark = spark
        self.aurora_jdbcurl = aurora_jdbcurl
        self.aurora_driver = aurora_driver
        self.aurora_uname = aurora_uname
        self.aurora_pwd = aurora_pwd
        self.logger = logger
        self.rds_data = rds_data
        self.aurora_resource_arn = aurora_resource_arn
        self.aurora_secret_arn = aurora_secret_arn
        self.aurora_dbname = aurora_dbname
        self.odx_schema_name = odx_schema_name
        self.audit_id = config_id

    def restart(self, schema_name, table_name):
        restart_query = "SELECT * from {}.{} where audit_id='{}' and manual_flag='{}'".format(self.odx_schema_name,
                                                                                              RESTART_TABLE,
                                                                                              self.audit_id,
                                                                                              self.manual_flag)
        exit_flag = 'N'
        self.logger.info("restartability table query %s", restart_query)


        restart_df = self.spark.read.format("jdbc").option("url", self.aurora_jdbcurl).option(
            "driver", self.aurora_driver).option("query", restart_query).option("user", self.aurora_uname).option(
            "password", self.aurora_pwd).load()

        if restart_df.count() == 0:
            self.logger.info("Restart table empty for table_name=%s and schema_name=%s ", table_name, schema_name)

        if restart_df.count() > 0:
            restart_df = restart_df.select('step_name')
            step_name = restart_df.first()['step_name']

            if step_name == 'Extract':
                self.logger.info("Extract  had failed earlier, hence restarting extraction")
            else:
                self.logger.info("Moving to update rds config, since, extraction was already completed earlier'")
                exit_flag = 'Y'
        return exit_flag

    def delete_restart_step(self):
        try:
            self.logger.info("Delete restart step from the restart table")
            delete_query = "delete from {}.{} where audit_id='{}' and manual_flag='{}'".format(self.odx_schema_name,
                                                                                              RESTART_TABLE,
                                                                                              self.audit_id,
                                                                                              self.manual_flag)

            print("Delete query for restart: {}".format(delete_query))
            rds_response = self.rds_data.execute_statement(
                resourceArn=self.aurora_resource_arn,
                secretArn=self.aurora_secret_arn,
                database=self.aurora_dbname,
                sql=delete_query)

            self.logger.info("Delete restart table response", rds_response)
        except Exception as e:
            self.logger.error("Unable to delete the restart step from the table")
            print(e)
            sys.exit(1)

    def restartability(self, ingest_job_name, schema_name, table_name, manual_flag):
        STEP_ID = '1'
        STEP_NAME = 'Extract'
        STEP_JOB_NAME = ingest_job_name
        DESCRIPTION = "Failed at extraction"

        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        audit_id = {'name': 'audit_id', 'value': {'stringValue': self.audit_id}}
        step_id = {'name': 'step_id', 'value': {'stringValue': STEP_ID}}
        step_name = {'name': 'step_name', 'value': {'stringValue': STEP_NAME}}
        schema_name = {'name': 'schema_name', 'value': {'stringValue': schema_name}}
        table_name = {'name': 'table_name', 'value': {'stringValue': table_name}}
        job_name = {'name': 'job_name', 'value': {'stringValue': ingest_job_name}}
        step_job_name = {'name': 'step_job_name', 'value': {'stringValue': STEP_JOB_NAME}}
        insert_timestamp = {'name': 'insert_timestamp', 'value': {'stringValue': timestamp}}
        description = {'name': 'description', 'value': {'stringValue': DESCRIPTION}}
        manual_flag = {'name': 'manual_flag', 'value': {'stringValue': manual_flag}}

        print("In RESTARTIBILITY")

        insert_row = [audit_id, step_id, step_name, table_name, job_name, step_job_name, insert_timestamp, schema_name,
                      description, manual_flag]
        column_names = """audit_id, step_id, step_name, table_name,job_name,step_job_name,insert_timestamp,schema_name,description,manual_flag"""
        str_sep = ','
        insert_query = "insert into {}.".format(
            self.odx_schema_name) + RESTART_TABLE + "(" + column_names + ") VALUES(" + str_sep.join(
            [":" + column.strip() for column in column_names.split(',')]) + ")"

        print(insert_query)

        try:
            self.logger.info("Insert restart step to table...")
            insert_response = self.rds_data.execute_statement(resourceArn=self.aurora_resource_arn,
                                                              secretArn=self.aurora_secret_arn,
                                                              database=self.aurora_dbname,
                                                              sql=insert_query,
                                                              parameters=insert_row)
            self.logger.info("Insert restart table response", insert_response)
            inserted = int(insert_response["numberOfRecordsUpdated"])
            return inserted

        except Exception as e:
            self.logger.error("Exception occured while inserting restartability step: ")
            print(e)
            sys.exit(1)


class Extractdata(object):
    def __init__(self, manual_flag, logger, spark, glueContext, jdbc_driver_name, db_url, db_username, db_password,
                 aurora_jdbcurl, aurora_driver, aurora_uname, aurora_pwd, rds_data, aurora_resource_arn,
                 aurora_secret_arn, aurora_dbname, ingest_job_name, odx_schema_name, config_id, cdc_col, cdc_tmstmp,
                 cdc_fmt,to_cdc_tmstmp):
        self.manual_flag = manual_flag
        self.logger = logger
        self.spark = spark
        self.glueContext = glueContext
        self.jdbc_driver_name = jdbc_driver_name
        self.db_url = db_url
        self.db_username = db_username
        self.db_password = db_password
        self.aurora_jdbcurl = aurora_jdbcurl
        self.aurora_driver = aurora_driver
        self.aurora_uname = aurora_uname
        self.aurora_pwd = aurora_pwd
        self.rds_data = rds_data
        self.aurora_resource_arn = aurora_resource_arn
        self.aurora_secret_arn = aurora_secret_arn
        self.aurora_dbname = aurora_dbname
        self.ingest_job_name = ingest_job_name
        self.odx_schema_name = odx_schema_name
        self.odx_config_id = config_id
        self.cdc_col = cdc_col
        self.cdc_tmstmp = cdc_tmstmp
        self.cdc_fmt = cdc_fmt
        self.delta_tmstmp = '1=1'
        self.ingest_timestamp = None
        self.to_cdc_tmstmp = to_cdc_tmstmp
        self.to_cdc_tmstmp_dt = None
        self.cnt_read = 0
        self.cnt_write = 0

    def read_cdc_column(self,load_strategy):
        try:
            multi_col_check = ","
            multi_col_flag = 'N'
            if multi_col_check in self.cdc_col:
                self.logger.info("Multiple column found for cdc_column")
                multi_col_flag = 'Y'
                self.logger.info("multi_col_flag value: {}".format(multi_col_flag))

            if multi_col_flag == "Y":
                muti_cdc_column = []
                muti_cdc_column = self.cdc_col.split(',')
                print("muti_cdc_column: {}".format(muti_cdc_column))

                final_col_list = None
                var_cols = 0
                for col in muti_cdc_column:
                    if final_col_list is None:
                        final_col_list = 'CONCAT(' + col + ','
                        var_cols = var_cols + 1
                    else:
                        if var_cols == 2:
                            final_col_list = 'CONCAT(' + final_col_list.rstrip(',') + ")" + ',' + col + ','
                        else:
                            final_col_list = final_col_list + col + ','
                            var_cols = var_cols + 1
                final_col_list = final_col_list.rstrip(',') + ")"
                print("final_col_list: {}".format(final_col_list))

                delta_tmstmp = final_col_list + ">=" + "TO_TIMESTAMP(" + "'" + str(
                    self.cdc_tmstmp) + "','" + self.cdc_fmt + "')"
                self.delta_tmstmp = delta_tmstmp
            else:
                if load_strategy == 'IO-SNAP-DLY':
                     print("I am in history data extraction")
                     delta_tmstmp = self.cdc_col + ">=" + "TO_TIMESTAMP(" + "'" + str(
                    self.cdc_tmstmp) + "','" + self.cdc_fmt + "')" + " AND " + self.cdc_col + "<=" + "TO_TIMESTAMP(" + "'" + str(self.to_cdc_tmstmp) + "','" + self.cdc_fmt + "')"
                else:
                    if load_strategy in ('IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                        self.logger.info("I in read_cdc_column and load_strategy={}".format(load_strategy))
                        self.to_cdc_tmstmp = datetime.datetime.fromtimestamp(time.time())
                        print("self.to_cdc_tmstmp current timestamp: {}".format(self.to_cdc_tmstmp))
                        
                        self.to_cdc_tmstmp = self.to_cdc_tmstmp + timedelta(days=-1)
                        print("self.to_cdc_tmstmp current timestamp -1 : {}".format(self.to_cdc_tmstmp))
                        
                        self.to_cdc_tmstmp_dt = self.to_cdc_tmstmp
                        self.to_cdc_tmstmp = self.to_cdc_tmstmp.strftime('%Y-%m-%d %H:%M:%S')
                        self.to_cdc_tmstmp = self.to_cdc_tmstmp.split(' ')[0] + " 23:59:59"
                        
                        print("I am in CDC extraction")
                        delta_tmstmp = self.cdc_col + ">=" + "TO_TIMESTAMP(" + "'" + str(self.cdc_tmstmp) + "','" + self.cdc_fmt + "')" + " AND " + self.cdc_col + "<=" + "TO_TIMESTAMP(" + "'" + str(self.to_cdc_tmstmp) + "','" + self.cdc_fmt + "')"
                        print("delta_tmstmp = {}".format(delta_tmstmp))
                    else:
                        print("I am in Snapshot extraction")
                        if load_strategy in ('IO-SNAP'):
                            delta_tmstmp = self.cdc_col + "=" + "TO_TIMESTAMP(" + "'" + str(self.cdc_tmstmp) + "','" + self.cdc_fmt + "')"
                        else:
                            delta_tmstmp = self.cdc_col + ">=" + "TO_TIMESTAMP(" + "'" + str(self.cdc_tmstmp) + "','" + self.cdc_fmt + "')"
                self.delta_tmstmp = delta_tmstmp
            print("delta_tmstmp: {}".format(delta_tmstmp))
        except Exception as e:
            self.logger.error("Failed at read cdc column")
            sys.exit(1)

    def read_sap_partition(self, source_partition_column, query_parameter, load_strategy, schema_name, table_name,
                            agg_column, agg_column_function, agg_operation):
        try:
            partition_list = []
            self.logger.info("Read sap partition")
            if load_strategy == 'TL-AGG':
                source_partition_column = source_partition_column + ' , ' + agg_column
            chars_to_check = [","]
            comma_flag = 'N'
            for chars_to_check in source_partition_column:
                comma_flag = 'Y'

            print(comma_flag)
            source_str = ''
            index = 0
            group_by_str = ''
            if comma_flag == 'Y':
                source_partition_list = source_partition_column.split(",")
                source_str=','.join(list(map(lambda x: x.replace(x, 'cast(' + x + ' as char) as ' + x), source_partition_list)))
                group_by_str=','.join(list(map(lambda x: x.replace(x, 'cast(' + x + ' as char) '), source_partition_list)))
            else:
                source_str = source_partition_column
                group_by_str = source_partition_column

            print('source partition string')
            print(source_str)

            print('group by string')
            print(group_by_str)
            if load_strategy in ('TL-SRC-TGT'):
                print("I am in read_sap_partition in truncate and load.")

                sap_query = "SELECT {} from {}.{} where {} GROUP BY {} ".format(source_str, schema_name,
                                                                                table_name, query_parameter,
                                                                                group_by_str)

            if load_strategy in ('IO', 'IO-SNAP', 'IO-CDC','TL-CDC'):
                print("I am in read_sap_partition in {} (with source partition)".format(load_strategy))

                print("self.cdc_col: {}".format(self.cdc_col))
                print("self.cdc_tmstmp: {}".format(self.cdc_tmstmp))
                print("self.cdc_fmt: {}".format(self.cdc_fmt))

                if self.cdc_col is None or self.cdc_col == "":
                    self.logger.info("cdc_column has no value: {}. Pulling all data from source.".format(self.cdc_col))
                else:
                    if not (self.cdc_tmstmp and self.cdc_fmt):
                        self.logger.info(
                            "cdc_column is set but one of cdc_timestamp or cdc_timestamp_format does not have a value. Pulling all data from source.")
                    else:
                        self.logger.info(
                            "cdc_column is set: {}, cdc_timestamp is set: {} and cdc_timestamp_format is set: {}. Pulling delta data from source.".format(
                                self.cdc_col, self.cdc_tmstmp, self.cdc_fmt))
                        if load_strategy not in ('IO'):
                            self.read_cdc_column(load_strategy)

                sap_query = "SELECT {} from {}.{} where {} and {} GROUP BY {} ".format(source_str, schema_name,
                                                                                       table_name, query_parameter,
                                                                                       self.delta_tmstmp, group_by_str)
                                                                                       
            if load_strategy in ('IO-SNAP-DLY','IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                print("I am in read_sap_partition in {} (with source partition)".format(load_strategy))

                print("self.cdc_col: {}".format(self.cdc_col))
                print("self.cdc_tmstmp: {}".format(self.cdc_tmstmp))
                print("self.cdc_fmt: {}".format(self.cdc_fmt))
                print("self.to_cdc_tmstmp: {}".format(self.to_cdc_tmstmp))

                if self.cdc_col is None or self.cdc_col == "":
                    self.logger.info("Load strategy: {}, cdc_column has no value: {}. Pulling all data from source.".format(load_strategy,self.cdc_col))
                else:
                    if not (self.cdc_tmstmp and self.cdc_fmt):
                        self.logger.info(
                            "Load strategy, cdc_column is set but one of cdc_timestamp or cdc_timestamp_format does not have a value. Pulling all data from source.")
                    else:
                        self.logger.info(
                            "cdc_column is set: {}, cdc_timestamp is set: {}, cdc_timestamp_format is set: {} and to_cdc_timestamp is set: {}. Pulling delta data from source.".format(
                                self.cdc_col, self.cdc_tmstmp, self.cdc_fmt, self.to_cdc_tmstmp))
                        if load_strategy not in ('IO'):
                            self.read_cdc_column(load_strategy)

                sap_query = "SELECT {} from {}.{} where {} and {} GROUP BY {} ".format(source_str, schema_name,
                                                                                       table_name, query_parameter,
                                                                                       self.delta_tmstmp, group_by_str)

            if load_strategy == 'TL-AGG':
                agg_column_function_str = str(agg_column_function) + '(' + str(agg_column) + ')'
                sap_query = "SELECT {} from {}.{} where {} and {} {} (select {} from {}.{} where {}) GROUP BY {} ".format(
                    source_str, schema_name,
                    table_name, query_parameter, agg_column, agg_operation, agg_column_function_str, schema_name,
                    table_name, query_parameter, group_by_str)

            sap_query = sap_query.replace("None", "NULL")
            print("SAP Query : %s", sap_query)
            

            df_sap_read = self.glueContext.read.format("jdbc").option("driver", self.jdbc_driver_name).option("url",
                                                                                                              self.db_url).option(
                "query", sap_query).option("user", self.db_username).option("password", self.db_password).option(
                "fetchsize",
                100000).load()

            self.ingest_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("Ingest delta timestamp (with source partition) for next run: {}".format(self.ingest_timestamp))
            for select_partition in df_sap_read.collect():
                partition_str = ""
                print('select partition')
                print(select_partition)
                for incr_value in range(len(df_sap_read.columns)):
                    # Double quotes are placed around df_sap_read.columns[incr_value] to retain the column casing as obtained from the database. Example:  "Block_Category"='PLM Block'
                    if incr_value == len(df_sap_read.columns)-1:
                        partition_str = partition_str + '"' + df_sap_read.columns[incr_value] + '"' + '=' + repr(
                            select_partition[incr_value])
                        print('partition_str1 :'+ partition_str)
                    else:
                        partition_str = partition_str + '"' + df_sap_read.columns[incr_value] + '"' + '=' + repr(
                            select_partition[incr_value]) + ' and '
                        print('partition_str2 :' + partition_str)
                partition_list.append(str(partition_str))
                print(partition_list)
                

            print("Partition list %s", partition_list)
            return partition_list

        except Exception as e:
            self.logger.error("Failed at read SAP")
            restartobj = Restart(self.manual_flag, self.spark, self.aurora_jdbcurl, self.aurora_driver,
                                 self.aurora_uname, self.aurora_pwd, self.logger, self.rds_data,
                                 self.aurora_resource_arn,
                                 self.aurora_secret_arn, self.aurora_dbname, self.odx_schema_name, self.odx_config_id)
            restartobj.restartability(self.ingest_job_name, schema_name, table_name,
                                      self.manual_flag)
            print(e)
            sys.exit(1)

    def extract(self, partition_list, load_strategy, query_parameter, select_columns, primary_keys,
                target_partition_column, curate_path, schema_name, table_name, agg_column,
                agg_column_function, agg_operation, source_partition_column):
        try:
            first_index = True
            is_extracted = False
            is_error = False
            self.logger.info("Ingestion started from SAP to S3")
            print(partition_list)
            
            print("source_partition_column: {}".format(source_partition_column))

            self.logger.info("Getting the extraction datetimestamp before extarcting data from SAP")
            if load_strategy in ('IO-SNAP','IO-CDC','TL-CDC'):
                if self.cdc_tmstmp is None:
                    partition_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    partition_timestamp = self.cdc_tmstmp.strftime('%Y-%m-%d %H:%M:%S')
            else:
                partition_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            print("Partition Timestamp is: {}".format(partition_timestamp))

            print('source partition list', partition_list)
            if partition_list and source_partition_column is not None:
                is_extracted = True
                for partitioning in partition_list:
                    print('breaking queries into multiple smaller queries')
                    if load_strategy in ('TL-SRC-TGT', 'IO', 'IO-SNAP', 'IO-CDC','TL-CDC','IO-SNAP-DLY','IO-SNAP-MAX-CDC','IO-CUSTOM-CDC') or load_strategy == 'TL-AGG':
                        sap_query = "SELECT {} from {}.{} where {} and {} and {}".format(select_columns, schema_name,
                                                                                         table_name,
                                                                                         query_parameter,
                                                                                         self.delta_tmstmp,
                                                                                         partitioning)

                        print("SAP Query for extraction: %s", sap_query)
                        df_sap_read = self.glueContext.read.format("jdbc").option("driver",
                                                                                  self.jdbc_driver_name).option("url",
                                                                                                                self.db_url).option(
                            "query", sap_query).option("user", self.db_username).option("password",
                                                                                        self.db_password).option(
                            "fetchsize",
                            100000).load()
                        if load_strategy in ('IO-SNAP-DLY','IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                            self.cnt_read = self.cnt_read + df_sap_read.count()

                        if target_partition_column:
                            target_load_partition = target_partition_column
                        else:
                            target_load_partition = None

                        print('after target partition column')

                        if load_strategy in ('IO', 'IO-SNAP', 'IO-CDC','TL-CDC','IO-SNAP-DLY','IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                            print("I am of type: {} (with source partition)".format(load_strategy))
                            
                            if load_strategy not in ('IO-SNAP-DLY','IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                                target_load_partition = partition_timestamp
                                print("target_load_partition is always current timestamp or cdc_timestamp if column is present")
                            
                            print("target_load_partition = {}".format(target_load_partition))
                            
                            if load_strategy in ('TL-CDC'):
                                if first_index:
                                    df_sap_read.withColumn("EXTRACT_DATE", sf.lit(target_load_partition)).write.option(
                                        "maxRecordsPerFile", 10000).partitionBy("EXTRACT_DATE").mode('overwrite').parquet(curate_path)
                                    first_index = False
                                else:
                                    df_sap_read.withColumn("EXTRACT_DATE", sf.lit(target_load_partition)).write.option(
                                        "maxRecordsPerFile", 10000).partitionBy("EXTRACT_DATE").mode('append').parquet(curate_path)
                            else:
                                if load_strategy in ('IO-SNAP-DLY','IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                                    print("I am of load type: {}, files will be partitioned based on target_partition_column value in config table".format(load_strategy))
                                    if target_load_partition:
                                        df_sap_read.write.option(
                                            "maxRecordsPerFile", 10000).partitionBy(*target_load_partition).mode('append').parquet(
                                                curate_path)
                                        self.cnt_write = self.cnt_write + df_sap_read.count()
                                    else:
                                        df_sap_read.write.option(
                                            "maxRecordsPerFile", 10000).mode('append').parquet(
                                                curate_path)
                                        self.cnt_write = self.cnt_write + df_sap_read.count()
                                else:
                                    df_sap_read.withColumn("EXTRACT_DATE", sf.lit(target_load_partition)).write.option(
                                        "maxRecordsPerFile", 10000).partitionBy("EXTRACT_DATE").mode('append').parquet(
                                            curate_path)
                        else:
                            if target_load_partition:
                                print('Target partition column present')
                                if first_index:
                                    df_sap_read.write.option("maxRecordsPerFile", 10000).partitionBy(
                                        *target_load_partition).mode('overwrite').parquet(curate_path)
                                    first_index = False
                                else:
                                    df_sap_read.write.option("maxRecordsPerFile", 10000).partitionBy(
                                        *target_load_partition).mode('append').parquet(curate_path)
                            else:
                                if first_index:
                                    df_sap_read.write.option("maxRecordsPerFile", 10000).mode('overwrite').parquet(
                                        curate_path)
                                    first_index = False
                                else:
                                    df_sap_read.write.option("maxRecordsPerFile", 10000).mode('append').parquet(
                                        curate_path)
                        self.logger.info("Ingest delta timestamp in extraction (wth source partitioning): %s",
                                         self.ingest_timestamp)
                        self.logger.info("Ingestion completed from SAP to S3")
            elif source_partition_column is None:
                if load_strategy in ('TL-SRC-TGT'):
                    sap_query = "SELECT {} from {}.{} where {}".format(select_columns, schema_name, table_name,
                                                                       query_parameter)

                if load_strategy in ('IO', 'IO-SNAP', 'IO-CDC','TL-CDC'):
                    print("In extract in for load_type: {} (without source partition)".format(load_strategy))

                    print("self.cdc_col: {}".format(self.cdc_col))
                    print("self.cdc_tmstmp: {}".format(self.cdc_tmstmp))
                    print("self.cdc_fmt: {}".format(self.cdc_fmt))

                    if self.cdc_col is None or self.cdc_col == "":
                        self.logger.info(
                            "cdc_column has no value: {}. Pulling all data from source.".format(self.cdc_col))
                    else:
                        if not (self.cdc_tmstmp and self.cdc_fmt):
                            self.logger.info(
                                "cdc_column is set but one of cdc_timestamp or cdc_timestamp_format does not have a value. Pulling all data from source.")
                        else:
                            self.logger.info(
                                "cdc_column is set: {}, cdc_timestamp is set: {} and cdc_timestamp_format is set: {}. Pulling delta data from source.".format(
                                    self.cdc_col, self.cdc_tmstmp, self.cdc_fmt))
                            if load_strategy not in ('IO'):
                                self.read_cdc_column(load_strategy)

                    sap_query = "SELECT {} from {}.{} where {} and {}".format(select_columns, schema_name, table_name,
                                                                              query_parameter, self.delta_tmstmp)
                                                                              
                if load_strategy in ('IO-SNAP-DLY','IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                    print("In extract in for load_type: {} (without source partition)".format(load_strategy))

                    print("self.cdc_col: {}".format(self.cdc_col))
                    print("self.cdc_tmstmp: {}".format(self.cdc_tmstmp))
                    print("self.cdc_fmt: {}".format(self.cdc_fmt))
                    print("self.to_cdc_tmstmp: {}".format(self.to_cdc_tmstmp))

                    if self.cdc_col is None or self.cdc_col == "":
                        self.logger.info(
                            "Load strategy: {}, cdc_column has no value: {}. Pulling all data from source.".format(load_strategy,self.cdc_col))
                    else:
                        if not (self.cdc_tmstmp and self.cdc_fmt):
                            self.logger.info(
                                "Load strategy, cdc_column is set but one of cdc_timestamp or cdc_timestamp_format does not have a value. Pulling all data from source.")
                        else:
                            self.logger.info(
                                "cdc_column is set: {}, cdc_timestamp is set: {}, cdc_timestamp_format is set: {} and to_cdc_timestamp is set: {}. Pulling delta data from source.".format(
                                    self.cdc_col, self.cdc_tmstmp, self.cdc_fmt, self.to_cdc_tmstmp))
                            if load_strategy not in ('IO'):
                                self.read_cdc_column(load_strategy)

                    sap_query = "SELECT {} from {}.{} where {} and {}".format(select_columns, schema_name, table_name,
                                                                              query_parameter, self.delta_tmstmp)

                if load_strategy == 'TL-AGG':
                    agg_column_function_str = str(agg_column_function) + '(' + str(agg_column) + ')'
                    sap_query = "SELECT {} from {}.{} where {} and {} {} (select {} from {}.{} where {}) ".format(
                        select_columns, schema_name,
                        table_name, query_parameter, agg_column, agg_operation, agg_column_function_str, schema_name,
                        table_name, query_parameter)
                sap_query = sap_query.replace("None", "NULL")
                # self.logger.info(" %s Ingestion SAP Query : %s", sap_query)
                print(sap_query)

                df_sap_read = self.glueContext.read.format("jdbc").option("driver", self.jdbc_driver_name).option("url",
                                                                                                                  self.db_url).option(
                    "query", sap_query).option("user", self.db_username).option("password", self.db_password).option(
                    "fetchsize",
                    100000).load()
                
                if df_sap_read.count() != 0:
                    is_extracted = True

                if load_strategy in ('IO-SNAP-DLY','IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                    self.cnt_read = self.cnt_read + df_sap_read.count()

                self.ingest_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
                print("Ingest delta timestamp (without source partition) for next run:".format(self.ingest_timestamp))

                if primary_keys:
                    df_sap_read = df_sap_read.dropDuplicates(primary_keys)
                    print('after primary key')

                if target_partition_column:
                    target_load_partition = target_partition_column
                else:
                    target_load_partition = None

                print('after target partition column')

                if load_strategy in ('IO', 'IO-SNAP', 'IO-CDC','TL-CDC','IO-SNAP-DLY','IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                    print("I am of type: {} and no source partition".format(load_strategy))
                    
                    if load_strategy not in ('IO-SNAP-DLY','IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                        target_load_partition = partition_timestamp
                        print("target_load_partition is always current timestamp or cdc_timestamp if column is present")
                    
                    print("target_load_partition = {}".format(target_load_partition))
                    
                    if load_strategy in ('TL-CDC'):
                        if first_index:
                            df_sap_read.withColumn("EXTRACT_DATE", sf.lit(target_load_partition)).write.option(
                                "maxRecordsPerFile", 10000).partitionBy("EXTRACT_DATE").mode('overwrite').parquet(curate_path)
                            print("D")
                            first_index = False
                        else:
                            df_sap_read.withColumn("EXTRACT_DATE", sf.lit(target_load_partition)).write.option(
                                "maxRecordsPerFile", 10000).partitionBy("EXTRACT_DATE").mode('append').parquet(curate_path)
                    else:
                        if load_strategy in ('IO-SNAP-DLY','IO-SNAP-MAX-CDC','IO-CUSTOM-CDC'):
                            print("I am of load type: {}, files will be partitioned based on target_partition_column value in config table".format(load_strategy))
                            if target_load_partition:
                                df_sap_read.write.option(
                                    "maxRecordsPerFile", 10000).partitionBy(*target_load_partition).mode('append').parquet(
                                        curate_path)
                                self.cnt_write = self.cnt_write + df_sap_read.count()
                            else:
                                df_sap_read.write.option(
                                    "maxRecordsPerFile", 10000).mode('append').parquet(
                                        curate_path)
                                self.cnt_write = self.cnt_write + df_sap_read.count()
                        else:
                            df_sap_read.withColumn("EXTRACT_DATE", sf.lit(target_load_partition)).write.option(
                                "maxRecordsPerFile", 10000).partitionBy("EXTRACT_DATE").mode('append').parquet(curate_path)
                else:
                    if target_load_partition:
                        if first_index:
                            df_sap_read.write.option("maxRecordsPerFile", 10000).partitionBy(
                                *target_load_partition).mode('overwrite').parquet(curate_path)
                            first_index = False
                        else:
                            df_sap_read.write.option("maxRecordsPerFile", 10000).partitionBy(
                                *target_load_partition).mode('append').parquet(curate_path)
                    else:
                        if first_index:
                            df_sap_read.write.option("maxRecordsPerFile", 10000).mode('overwrite').parquet(curate_path)
                            first_index = False
                        else:
                            df_sap_read.write.option("maxRecordsPerFile", 10000).mode('append').parquet(curate_path)
                self.logger.info("Ingest delta timestamp (without source partition): %s", self.ingest_timestamp)
                self.logger.info("Ingestion completed from SAP to S3")

            return is_extracted, self.ingest_timestamp, self.cnt_read, self.cnt_write,is_error,self.to_cdc_tmstmp_dt
        except Exception as e:
            self.logger.error("Failed at ingestion step from SAP to S3")
            is_error = True
            print(e)
            restartobj = Restart(self.manual_flag, self.spark, self.aurora_jdbcurl, self.aurora_driver,
                                 self.aurora_uname, self.aurora_pwd, self.logger, self.rds_data,
                                 self.aurora_resource_arn,
                                 self.aurora_secret_arn, self.aurora_dbname, self.odx_schema_name, self.odx_config_id)
            restartobj.restartability(self.ingest_job_name, schema_name, table_name,
                                      self.manual_flag)
            print(e)
            return False, self.ingest_timestamp, self.cnt_read, self.cnt_write, is_error,self.to_cdc_tmstmp_dt
            sys.exit(1)

class Worktable(object):
    def __init__(self, logger, rds_data, aurora_resource_arn, aurora_secret_arn, aurora_dbname,
                 odx_schema_name):
        self.logger = logger
        self.rds_data = rds_data
        self.aurora_resource_arn = aurora_resource_arn
        self.aurora_secret_arn = aurora_secret_arn
        self.aurora_dbname = aurora_dbname
        self.odx_schema_name = odx_schema_name

    def insert_temp_table(self, audit_id, cdc_timestamp, cdc_timestamp_fmt, to_cdc_timestamp):
        """
        :param id: is the md5 of schema and table name
        :param partition_list: partition list of strings
        :return: inserted record counts
        """
        self.logger.info("Logging cdc_timestamp value in odx_generic_tbl_extract_work_control_parameters table")
        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        insert_timestamp = {'name': 'insert_timestamp', 'value': {'stringValue': timestamp}}
        audit_id = {'name': 'audit_id', 'value': {'stringValue': audit_id}}

        cdc_timestamp = {'name': 'cdc_timestamp', 'value': {'stringValue': cdc_timestamp}}
        cdc_timestamp_fmt = {'name': 'cdc_timestamp_fmt', 'value': {'stringValue': cdc_timestamp_fmt}}
        to_cdc_timestamp = {'name': 'to_cdc_timestamp', 'value': {'stringValue': to_cdc_timestamp}}

        print("cdc_timestamp value: {}".format(cdc_timestamp))

        insert_row = [audit_id, insert_timestamp, cdc_timestamp, cdc_timestamp_fmt,to_cdc_timestamp]
        column_names = "audit_id, insert_timestamp, cdc_timestamp,cdc_timestamp_fmt,to_cdc_timestamp"
        str_sep = ','
        insert_query = "insert into {}.".format(
            self.odx_schema_name) + TEMP_TABLE + "(" + column_names + ") VALUES(" + str_sep.join(
            [":" + column.strip() for column in column_names.split(',')]) + ")"

        print("insert into TEMP_TABLE query: {}".format(insert_query))
        try:
            self.logger.info("Inserting cdc_timestamp values to temp table")
            insert_response = self.rds_data.execute_statement(resourceArn=self.aurora_resource_arn,
                                                              secretArn=self.aurora_secret_arn,
                                                              database=self.aurora_dbname,
                                                              sql=insert_query,
                                                              parameters=insert_row)

            self.logger.info("Inserting cdc_timestamp response", insert_response)
            inserted = int(insert_response["numberOfRecordsUpdated"])
            self.logger.info("cdc_timestamp values inserted successully into temp table")

        except Exception as e:
            self.logger.error("Exception occured while inserting partition list to temp table")
            print(e)
            sys.exit(1)


class Workflow(object):

    def __init__(self, workflow_name, workflow_run_id, logger, glue_client,config_id):
        self.workflow_name = workflow_name
        self.workflow_run_id = workflow_run_id
        self.logger = logger
        self.glue_client = glue_client
        self.config_id=config_id

    def put_wf_properties(self, table_name,schema_name,crawler_name):
        try:
            print('inside worfklow properties set')
            self.logger.info("Setting workflow properites")
            if self.config_id is None:
                config_id=''
            else:
                config_id=self.config_id

            if crawler_name is None:
                crawler_name=''
            wf_response = self.glue_client.put_workflow_run_properties(
                Name=self.workflow_name,
                RunId=self.workflow_run_id,
                RunProperties={
                    'table_name': table_name,
                    'schema_name': schema_name,
                    'odx_config_id': config_id,
                    'crawler_name':crawler_name
                }
            )
            self.logger.info('Workflow properites :')
            workflow_params = \
                self.glue_client.get_workflow_run_properties(Name=self.workflow_name, RunId=self.workflow_run_id)[
                    "RunProperties"]
            self.logger.info(workflow_params)
        except Exception as e:
            self.logger.error("While adding table name to workflow properties job got failed")
            print(e)
            sys.exit(1)


class Deleteobjects(object):
    def __init__(self, s3_client, logger):
        self.s3_client = s3_client
        self.logger = logger
        print('inside delete objects class')

    def delete_objects(self, curate_bucket, curate_path):
        try:
            delete_obects_list = []

            paginator = self.s3_client.get_paginator('list_objects_v2')
            self.logger.info("Deleting ")
            pages = paginator.paginate(Bucket=curate_bucket,
                                       Prefix=curate_path)
            for page in pages:
                for s3_obj in page['Contents']:
                    # logger.info(s3_obj['Key'])
                    delete_obects_list.append({'Key': s3_obj['Key']})

            self.logger.info("Total objects to be deleted is %s ", str(len(delete_obects_list)))
            self.logger.info("Deleting these objects %s", delete_obects_list)
            for i in range(0, len(delete_obects_list), 1000):
                delete_response = self.s3_client.delete_objects(
                    Bucket=curate_bucket,
                    Delete={
                        'Objects': delete_obects_list[i:i + 1000],
                        'Quiet': True
                    }
                )
                print(delete_response)
            return len(delete_obects_list)

        except Exception as e:
            self.logger.info(e)
            sys.exit(1)


def main():
    Driver()


if __name__ == "__main__":
    main()