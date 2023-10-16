import time

def deploy_model(client, model_id):
    response = client.transport.perform_request('POST','/_plugins/_ml/models/%s/_deploy'%model_id)
    task_id = response["task_id"]
    while(1):
        response=client.transport.perform_request("GET","/_plugins/_ml/tasks/%s"%task_id,{})
        if response["state"]=="COMPLETED":
            return response
        print(response["state"])
        time.sleep(3)