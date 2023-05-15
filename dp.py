from google.cloud import bigquery
from target import connect_to_target, connect_to_datasets

def data_patrol(environment, schema):
  client = connect_to_target(environment)
  if client != 'fail':
    result_list = connect_to_datasets(client, environment, schema)
    if result_list:
      return result_list
    else:
      return 'empty datasets'

  else:
    return 'operation failed, target communication failed'
