import datetime
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Key, Attr
import time
try:

    import env_info_compact as ENV_INFO
    from custom_logger_compact import CustomLogger, STAT_LEVEL

except ModuleNotFoundError:

    import hudi_compaction.env_info_compact as ENV_INFO
    from util.custom_logger_compact import CustomLogger, STAT_LEVEL


print('ANil')

class S3FileStatusInfo:

    def __init__(self, table_info, items_by_status_idx_info, items_by_dag_runid_idx_info, expiry_timestamp,client, logger):
        self.table_info = table_info
        self.items_by_status_idx_info = items_by_status_idx_info
        self.items_by_dag_runid_idx_info = items_by_dag_runid_idx_info
        self.expiry_timestamp =  expiry_timestamp
        self.client = client
        self.logger = logger

    def get_table(self):
        return self.client.Table(self.table_info['table_name'])

    def get_table_info(self):
        return self.table_info

    def getlogger(self):
        return self.logger

    def get_queued_items(self, sch_tbl):
        try:
            log = self.getlogger()
            table = self.get_table()
            idx_info = self.items_by_status_idx_info
            log.info("-------FETCHING DYANMODB QUEUED ITEMS FOR {}---------".format(sch_tbl))
            datetime_object_1 = datetime.now()
            log.log(STAT_LEVEL,ENV_INFO.QUEUED_FETCH_START_TIME.format(None, datetime_object_1))
            resp = table.query(IndexName=idx_info['idx_name'],
                                        KeyConditionExpression=Key(idx_info['hashkey']).eq(
                                            'QUEUED') & Key(idx_info['sortkey']).eq(sch_tbl),
                                        )
            items = resp['Items']
            while 'LastEvaluatedKey' in resp:
                print(resp['LastEvaluatedKey'])
                resp = table.query(IndexName=idx_info['idx_name'],
                                        KeyConditionExpression=Key(idx_info['hashkey']).eq(
                                            'QUEUED') & Key(idx_info['sortkey']).eq(sch_tbl), ExclusiveStartKey=resp['LastEvaluatedKey'],)
                items.extend(resp['Items'])
            log.info("-------SUCESSFULLY FETCHED DYANMODB QUEUED ITEMS FOR {}---------".format(sch_tbl))
            datetime_object_2 = datetime.now()
            time_taken=str(datetime_object_2-datetime_object_1)
            log.log(STAT_LEVEL,ENV_INFO.QUEUED_ITEM_COUNT.format(None, len(items)))
            log.log(STAT_LEVEL,ENV_INFO.QUEUED_FETCH_END_TIME.format(None, datetime_object_2,time_taken))
            return items
            
        except Exception as e:
            log.error("error occured while reading data from dynamodb {}".format(e))

    def get_all_items_for_dag_run(self, dag_run_id):
        try:
            log = self.getlogger()
            table = self.get_table()
            idx_info = self.items_by_dag_runid_idx_info
            resp = table.query(IndexName=idx_info['idx_name'],
                                        KeyConditionExpression=Key(idx_info['hashkey']).eq(
                                            dag_run_id)
                                        )
            items = resp['Items']
            while 'LastEvaluatedKey' in resp:
                print(resp['LastEvaluatedKey'])
                resp = table.query(IndexName=idx_info['idx_name'],
                                   KeyConditionExpression=Key(idx_info['hashkey']).eq(
                                       dag_run_id), ExclusiveStartKey=resp['LastEvaluatedKey'],
                                   )
                items.extend(resp['Items'])
            log.info("-------SUCESSFULLY FETCHED DYANMODB DAG RUN ITEMS FOR {}---------".format(dag_run_id))
            return items

        except Exception as e:
            log.error("error occured while reading data from dynamodb {}".format(e))
                                

    def get_failed_items_for_dag_run(self, dag_run_id, failed_status):
        try:
            log = self.getlogger()
            table = self.get_table()
            idx_info = self.items_by_dag_runid_idx_info
            resp = table.query(IndexName=idx_info['idx_name'],
                                        KeyConditionExpression=Key(idx_info['hashkey']).eq(
                                            dag_run_id) & Key(idx_info['sortkey']).eq(failed_status),
                                        )
            items = resp['Items']
            while 'LastEvaluatedKey' in resp:
                print(resp['LastEvaluatedKey'])
                resp = table.query(IndexName=idx_info['idx_name'],
                                   KeyConditionExpression=Key(idx_info['hashkey']).eq(
                                       dag_run_id) & Key(idx_info['sortkey']).eq(failed_status), ExclusiveStartKey=resp['LastEvaluatedKey'],
                                   )
                items.extend(resp['Items'])
            log.info("-------SUCESSFULLY FETCHED DYANMODB FAILED ITEMS FOR {}---------".format(dag_run_id))
            return items
        except Exception as e:
            log.error("error occured while reading data from dynamodb {}".format(e))

    def update_item(self,s3uri,dag_run_id,sucess,glue_job_id,reason,sh):
        try:
            log = self.getlogger()
            table = self.get_table()
            table_info = self.get_table_info()
            expiry_timestamp = int(time.time() + self.expiry_timestamp)
            
            if sucess:
                key_attribute={table_info['hashkey']: s3uri}
                updateexpression_attribute="set #c = :s, #dag = :d,#t = :ttl,#glue = :gluejobid,#r = :reason,#h  = list_append(if_not_exists(#h, :empty_list), :sh)" 
                expressionattributevalues_attribute={':s': "COMPLETED",':d': dag_run_id,':ttl':expiry_timestamp,':gluejobid':glue_job_id,':reason':reason,':sh': sh,":empty_list":[]}
                expressionattributenames_attribute={'#c': 'status','#dag': 'dag_run_id','#t':'expire','#glue':'glue_job_id','#r':'reason','#h': 'status_history'}
            else:
                key_attribute={table_info['hashkey']: s3uri}
                updateexpression_attribute="set #c = :d,#glue = :gluejobid,#r = :reason,#h  = list_append(if_not_exists(#h, :empty_list), :sh)"
                expressionattributevalues_attribute={':d': dag_run_id,':gluejobid':glue_job_id,':reason':reason,':sh': sh,":empty_list":[]}
                expressionattributenames_attribute={'#c': 'dag_run_id','#glue':'glue_job_id','#r':'reason','#h': 'status_history'}
                
            response = table.update_item(
                Key=key_attribute,
                UpdateExpression=updateexpression_attribute,
                ExpressionAttributeValues=expressionattributevalues_attribute,
                ExpressionAttributeNames=expressionattributenames_attribute,
                ReturnValues="UPDATED_NEW"
            )
            log.info("-----UPDATED STATUS IN DYNAMODB------")
            return response
        except Exception as e:
            log.error("error occured while updating data from dynamodb",e)

    def updatestatushistory(self,s3uri,queued_status,completed_status,failed_status,result):

        try:
            log = self.getlogger()
            table = self.get_table()
            table_info = self.get_table_info()
            statuslst1 = [queued_status,completed_status]
            statuslst2 = [queued_status,failed_status]
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
            if result:    
                sh=[{"s3uri": s3uri, "status": status, "update_time": dt_string} for status in statuslst1]
            else:
                sh=[{"s3uri": s3uri, "status": status, "update_time": dt_string} for status in statuslst2]
            response = table.update_item(
                Key={
                    table_info['hashkey']: s3uri
                },
                UpdateExpression="set #h  = list_append(if_not_exists(#h, :empty_list), :sh)",
                ExpressionAttributeValues={
                    ':sh': sh,
                    ":empty_list":[],
                },
                ExpressionAttributeNames={
                    '#h': 'status_history',
                },
                ReturnValues="UPDATED_NEW"
            )
            log.info("-------UPDATED STATUS HISTORY OF IN DYNAMODB--------")
            return response
        except Exception as e:
            log.error("error occured while updating status history data from dynamodb",e)


class ClientErrorException(Exception):

    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message in error_mapping.keys():
            return (error_mapping.get(self.message, self.message))
        else:
            return {self.message}

print('Adding this line to commit')