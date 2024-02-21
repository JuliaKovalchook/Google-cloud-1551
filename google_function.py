from google.cloud import storage
import pandas as pd
import requests
from io import BytesIO



def api_to_gcs( filename):

    # get max index
    client = storage.Client(project='data eng')
    bucket = client.get_bucket('bucket_1551')

    blob_index = bucket.get_blob('max_index.csv')
    binary_stream = blob_index.download_as_string()
    try: 
        exist_index = BytesIO(binary_stream).getvalue()
        if len(exist_index)>0:
            exist_index=int(exist_index)
        else:
            exist_index=1026383
    except: 
        exist_index=1026383
   
    # get old data
    blob_df = bucket.get_blob('df_test.csv')
    binary_stream = blob_df.download_as_string()
    try: 
        df_old=pd.read_csv(BytesIO(binary_stream))
    except: 
        df_old = pd.DataFrame()

    # get new data
    site = "https://1551-back.kyivcity.gov.ua/api/tickets/" 
    df_new = pd.DataFrame()

    for tickets_id in range(exist_index+1,exist_index+50):
        url=site+str(tickets_id)
        data = requests.get(url)
        if data.status_code == 200:
            json = data.json()
            df0=pd.DataFrame.from_dict(json, orient='index')[['id', 'created_at', 'address', 'title']]#[['id', 'created_at', 'approx_done_date', 'address','title', 'status']]
            df0=df0.reset_index(drop=True)
            df_new=pd.concat([df_new, df0], ignore_index=True)



    #concat 
    #write  df_test
    df= pd.concat([df_old,df_new])
    blob_df.upload_from_string(df.to_csv(index = False),content_type = 'csv')

    #write max_index
    new_max_index=str(df['id'].max())
    if new_max_index==exist_index:
        new_max_index=exist_index+49
    blob_index.upload_from_string(new_max_index,content_type = 'csv')

def main( data, context):
    api_to_gcs('df_test.csv')
