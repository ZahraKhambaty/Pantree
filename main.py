#!/usr/bin/env python
# coding: utf-8

from google.auth.transport.urllib3 import AuthorizedHttp
from requests_oauthlib import OAuth2Session
import google.auth.transport.requests
from google.oauth2 import service_account
import googleapiclient.discovery

from google.cloud import datastore
from google.cloud import storage
from google.cloud import bigquery

from datetime import date
import datetime
import logging
import json
import os
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS']='google_cloud_key.development.json'


def datastore_to_storage(request):
	
 projectid ='pantree-198302'
 request_body = {
     "outputUrlPrefix": "gs://pantree_datastore_kind_backup",
     "entityFilter": {
       "kinds": [
       "User",
       "Order",
       "Product",
       "Recipe",
       "Conversion",
       "Location", 
       "ShoppingList",
       "Chef",
       "Session"    
     ],
     "namespaceIds": [
       "development.amete"
     ]
   }
 }

### Authenticate and Call Server to Server API ###
### Define scope and authentication file
 SCOPES = ['https://www.googleapis.com/auth/datastore','https://www.googleapis.com/auth/cloud-platform']
 SERVICE_ACCOUNT_FILE = 'google_cloud_key.development.json'
 credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
### Authorize creedentials and send a post request to export data from datastore to cloud storage
 authed_http = AuthorizedHttp(credentials)
 response = authed_http.request('POST',"https://datastore.googleapis.com/v1/projects/pantree-198302:export",body=str(request_body))
 print(response.status)
 print(response.data)
 return 'ok'

def storage_to_bigquery(request):
	
 projectid ='pantree-198302'
### Authenticate and Call Server to Server API ###
### Define scope and authentication file
 SCOPES = ['https://www.googleapis.com/auth/datastore','https://www.googleapis.com/auth/cloud-platform']
 SERVICE_ACCOUNT_FILE = 'google_cloud_key.development.json'
 credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

 client = storage.Client()
 bucket_name = "pantree_datastore_kind_backup"
 bucket = client.get_bucket(bucket_name)
 blobs = client.list_blobs(bucket_name)
 ## Choosing the latest blob
 df = []
 for blob in blobs:
    df.append((blob.updated))
 max_date = (max(df)).date()
 print(max_date)
 blobs = client.list_blobs(bucket_name)
 tables = []
 urls = []
 for blob in blobs:
  if ((blob.updated).date()==max_date) &(("export_metadata") in blob.name):
   if "namespace_development.amete" in blob.name:
    table_name = blob.name.split("/")
    table_name = table_name[2].split("_")
    tables.append(table_name[1])
    urls.append(blob.name)

 ### Exporting to Big Query 
 client = bigquery.Client(credentials= credentials,project=projectid)
 dataset_ref = client.dataset("Entities")
 dataset = bigquery.Dataset(dataset_ref)
 for i in range(len(tables)):
    table_ref = dataset.table(str(tables[i]))
    GS_URL = "gs://pantree_datastore_kind_backup/"+str(urls[i])
    job_id_prefix = "my_job"
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = 'DATASTORE_BACKUP'
    job_config.write_disposition = 'WRITE_TRUNCATE'
    load_job = client.load_table_from_uri(GS_URL, table_ref, job_config=job_config,job_id_prefix=job_id_prefix)  # API request
    load_job.result()
 return 'ok'

