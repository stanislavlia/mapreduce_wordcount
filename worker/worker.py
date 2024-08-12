from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Tuple
import json

import boto3
import json
from botocore.client import Config
from count_utils import *

minio_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',  
    aws_access_key_id='your_username',      
    aws_secret_access_key='your_password',  
    config=Config(signature_version='s3v4'),
    region_name='us-east-1' 
)


app = FastAPI()

class MapTaskRequest(BaseModel):
    data: str
    task_name: str

class ReduceTaskRequest(BaseModel):
    intermediate_data: List[Tuple[str, int]]

class TaskResponse(BaseModel):
    result: Dict[str, int]
    

def put_json_to_s3(json_data, name, client):
    
    client.put_object(
        Bucket='count-data',  
        Key=name,    
        Body=json_data,
        ContentType='application/json')


@app.post("/map_task")
def map_task(request : MapTaskRequest):
    
    intermediate_results = map_count(request.data)
    
    intermediate_results = [list(item) for item in intermediate_results]
    json_data = json.dumps(intermediate_results, indent=3).encode("utf-8")

    put_json_to_s3(json_data, request.task_name + ".json", minio_client)
    
    return {"status" : "done",
            "result_path" :  request.task_name + ".json",}
    
    

    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8010, reload=True)
    
    
