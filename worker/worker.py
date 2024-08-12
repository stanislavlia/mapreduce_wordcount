from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Tuple
import json
import re
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



    

print(reduce_count(map_count("Hello world! this is world!")))
    
    