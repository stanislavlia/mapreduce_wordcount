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


def round_robin_url(cluster : List):
    
    i = 0
    
    while True:
        
        url_to_return = cluster[i % len(cluster)]
        i += 1
        yield url_to_return

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
            grouped_results[key].append((key, val))
        else:
            grouped_results[key] = [(key, val)]
            
    
    return grouped_results


def submit_to_reducers(grouped_results : Dict, url_generator):
    
    counts = []
    
    for key in grouped_results.keys():
        
        worker_url = next(url_generator)
        
        reduced_result = httpx.post(worker_url + "/reduce_task",
                                    json={"intermediate_data" : grouped_results[key]})
        print("Reduce task submitted to worker: ", worker_url)
        
        reduced_result = reduced_result.json()
        counts.append(reduced_result)
    
    return counts
        
    
    
    
def mapreduce_word_count(text : str, k=10):
    
    
    url_generator = round_robin_url(WORKERS_CLUSTER)
    
    map_tasks_results = []
    
    
    chunks_of_text = split_text_to_chunks(text, n_chunks=k)
    print(f"Text splitted into {k} chunks...")
    
    for i, chunk in enumerate(chunks_of_text):
        
        name = f"map_task_{i}"
        worker_url = next(url_generator)
        
        path = submit_chunk_to_worker(text_chunk=chunk,
                               task_name=name,
                               worker_url=worker_url + "/map_task")
        
        print(f"Map task submitted to {worker_url}")
        
        map_tasks_results.append(path)
    
    print("Collect all results and group them...")
    all_results = collect_all_result(map_tasks_results, minio_client)
    
    grouped_results = group_results(all_results)
    
    counts = submit_to_reducers(grouped_results, url_generator)
    
    return counts
    
    
    
    


if __name__ == "__main__":
    
    with open("tiny_shakespeare.txt", "r") as f:
        TEXT = f.read()
        
    pprint(mapreduce_word_count(text=TEXT, k=21))


