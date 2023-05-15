from google.cloud import bigquery

def connect_to_target(projectid):
  try:
    client_target = bigquery.Client(project=projectid)
  except:
    return 'fail'
  else:
    return client_target

def connect_to_datasets(client, environment, schema):

  datasets = list(client.list_datasets())  # Make an API request.
  tables_dict = {}

  if datasets:
    for dataset in datasets:
      tables = list(client.list_tables(dataset.dataset_id))
      for table in tables:
        if schema.lower() == table.table_id.lower():
          print(f"{schema} exists in {dataset.dataset_id}, {table.table_id}")
          dataset_name = dataset.dataset_id
          query = """ SELECT COUNT(*) as cont FROM `{}.{}.{}` """.format(environment, dataset.dataset_id, schema)
          query_result = client.query(query) # execute dynamic query
          result = query_result.result()  # wait execution to finish

          for row in result:
            table_id = dataset.dataset_id + "." + schema
            tables_dict[table_id] = row.cont

        else:
          continue
  else:
      return 'No datasets available'

  formatted_str = ""
  for key, value in tables_dict.items():
     formatted_str += f"{key} : {value}"
  
  return formatted_str, client, dataset_name

def compare_structures(client, dataset_name1, dataset_name2, schema1, schema2):

      query = f"""SELECT CONCAT(b.column_name, '-', b.table_name) AS colname
      FROM {dataset_name1}.INFORMATION_SCHEMA.COLUMNS AS b
      WHERE b.table_name = "{schema1}"
      AND b.column_name NOT IN (
        SELECT t.column_name
        FROM {dataset_name2}.INFORMATION_SCHEMA.COLUMNS AS t
        WHERE t.table_name = "{schema2}" )
        UNION ALL
        SELECT CONCAT(t.column_name, '-', t.table_name) AS colname
        FROM {dataset_name2}.INFORMATION_SCHEMA.COLUMNS AS t
        WHERE t.table_name = "{schema2}"
        AND t.column_name NOT IN (
        SELECT b.column_name
        FROM {dataset_name1}.INFORMATION_SCHEMA.COLUMNS AS b
        WHERE b.table_name = "{schema1}" ) """

      query_result = client.query(query) # execute dynamic query
      result = query_result.result()  # wait execution to finish

      tables_list = []
      
      for row in result:
        tables_list.append({'Field': f'{row.colname}'})

      return tables_list
  

