import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Tuple
import json
import boto3
import json
from botocore.client import Config
from pprint import pprint


WORKERS_CLUSTER = ["http://0.0.0.0:8000",
                   "http://0.0.0.0:8001",
                   "http://0.0.0.0:8002"]


minio_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',  
    aws_access_key_id='your_username',      
    aws_secret_access_key='your_password',  
    config=Config(signature_version='s3v4'),
    region_name='us-east-1' 
)


def split_text_to_chunks(text, n_chunks):
    lines = text.split('\n')
    
    if len(lines) >= n_chunks:
        chunk_size = len(lines) // n_chunks
        chunks = ['\n'.join(lines[i*chunk_size:(i+1)*chunk_size]) for i in range(n_chunks)]
        
        if len(lines) % n_chunks != 0:
            chunks[-1] += '\n' + '\n'.join(lines[n_chunks*chunk_size:])
    else:
        words = text.split(' ')
        chunk_size = len(words) // n_chunks
        chunks = [' '.join(words[i*chunk_size:(i+1)*chunk_size]) for i in range(n_chunks)]
        
        if len(words) % n_chunks != 0:
            chunks[-1] += ' ' + ' '.join(words[n_chunks*chunk_size:])
    
    return chunks

def get_json_from_s3(bucket_name, file_key, s3_client):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        
        json_data = response['Body'].read().decode('utf-8')        
        data = json.loads(json_data)
        return data
    
    except Exception as e:
        print(f"Error fetching the file: {e}")
        return None

def submit_chunk_to_worker(text_chunk, task_name, worker_url):

    data = {"data" : text_chunk,
            "task_name" : task_name}

    response = httpx.post(worker_url, json=data).json()
    
    if (response["status"] == "done"):
        return response["result_path"]


def collect_all_result(object_names : List, s3_client, bucket_name="count-data"):
    
    results = []
    
    for obj_name in object_names:
        
        inter_results = get_json_from_s3(bucket_name,
                                         file_key=obj_name,
                                         s3_client=s3_client)
        results.extend(inter_results)
    
    return results


def group_results(results : List):
    
    grouped_results = dict()
    
    for key, val in results:
        if key in grouped_results:
            grouped_results[key].append(val)
        else:
            grouped_results[key] = [val]
            
    
    return grouped_results




# result = submit_chunk_to_worker("Hello world my name is Stas! Hello Hello",
#                                 "task_1_chunk", WORKERS_CLUSTER[0] + "/map_task")

# result2 = submit_chunk_to_worker("Bye world my name is Stas! Bye Hello",
#                                 "task_2_chunk", WORKERS_CLUSTER[1] + "/map_task")

# result3 = submit_chunk_to_worker("Hello world my name is Stas! Hello Hello",
#                                 "task_3_chunk", WORKERS_CLUSTER[2] + "/map_task")

names = ["task_1_chunk.json", "task_1_chunk.json", "task_3_chunk.json"]

results = collect_all_result(names, minio_client)

pprint(group_results(results))