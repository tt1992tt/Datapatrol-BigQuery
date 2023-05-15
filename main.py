from environment import project
from dp import data_patrol
from target import compare_structures
from flask import jsonify
import json

def big_query(request):
  
  source = request.args.get('source')
  target = request.args.get('target')

  if source != None and target != None:
    print("query request: "+source)
    result_source1, client, dataset_source1 = data_patrol(project, source)
    result_source2, client2, dataset_source2 = data_patrol(project, target)
    struct_variance = compare_structures(client, dataset_source1, dataset_source2, source, target)
    response = {
      "Source count" : result_source1,
      "Target count" : result_source2,
      "Structure variance" : struct_variance
    }
    return jsonify(json.dumps(response, indent=4))
  else:
    request_json = request.get_json()
    print("JSON request: "+str(request_json['source'])+" "+str(request_json['target']))
    if 'source' in request_json and 'target' in request_json:
      source = request_json['source']
      target = request_json['target']
      result_source1, client, dataset_source1 = data_patrol(project, source)
      result_source2, client2, dataset_source2 = data_patrol(project, target)
      struct_variance = compare_structures(client, dataset_source1, dataset_source2, source, target)
      response = {
        "Source count" : result_source1,
        "Target count" : result_source2,
        "Structure variance" : struct_variance
      }
      return jsonify(json.dumps(response, indent=4))
    else:
      response = {
        "DP Result" : "No values for the providad schema"
      }
      return jsonify(response)
