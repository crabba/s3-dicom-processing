import json
import urllib.parse
import boto3
import hashlib
import io
import json
import os
import pydicom
import deid
from datetime import datetime, date, time, timezone
# from deid.data import get_dataset

s3_r = boto3.resource('s3')
ssm_c = boto3.client('ssm')
ddb_r = boto3.resource('dynamodb')
ddb_c = boto3.client('dynamodb')

DDB_TABLE = 'dicom-edits'
EDITS_CONFIG_PARAM = '/dicom-deident/edits-00'

def read_dicom(bucket_name, key):
    try:
        # Read S3 object into memory
        bucket = s3_r.Bucket(bucket_name)
        object = bucket.Object(key)
        file_stream = io.BytesIO()
        object.download_fileobj(file_stream)
        file_stream.seek(0)
        ds = pydicom.dcmread(file_stream)
        # print(ds)
    except pydicom.errors.InvalidDicomError as e:
        print(f"InvalidDicomError: {e}")
    except Exception as e:
        print(e)
    return(ds)
    
def write_dataset_to_bytes(dataset):
    # https://pydicom.github.io/pydicom/stable/auto_examples/memory_dataset.html
    # create a buffer
    with io.BytesIO() as buffer:
        # create a DicomFileLike object that has some properties of DataSet
        memory_dataset = pydicom.filebase.DicomFileLike(buffer)
        # write the dataset to the DicomFileLike object
        pydicom.dcmwrite(memory_dataset, dataset)
        # to read from the object, you have to rewind it
        memory_dataset.seek(0)
        # read the contents as bytes
        return memory_dataset.read()
        
def write_dicom(bucket_name, key, bytes):
    try:
        # /dicom/input/f.dcm -> /dicom/output/f.dcm
        (inpath, infile) = os.path.split(key)
        stem = '/'.join(inpath.split('/')[:-1])
        outpath = os.path.join(stem, 'output', infile)
        bucket = s3_r.Bucket(bucket_name)
        object_out = bucket.Object(outpath)
        object_out.put(Body = bytes)
    except Exception as e:
        print(e)
        raise(e)

def get_deident_config(config_name):
    """Read given parameter from Parameter Store, return as JSON
    """
    try:
        response = ssm_c.get_parameter(
            Name=config_name,
            WithDecryption=False
        )
        config_j = json.loads(response["Parameter"]["Value"])
    except Exception as e:
        print(e)
        raise(e)

    return config_j

def perform_deident(ds, config):
    """Perform de-identification on ds.  Config is in this format:
    {
        "0010,0020": {
            "111111": {
                "0010,0010": "Anon^100",
                "0010,0020": "100"
            }
        }
    }
    """

    patient_id = ds[0x10,0x20].value
    config_vals = config["0010,0020"]
    if patient_id in config_vals:
        patient_items = config_vals[patient_id]
        for item in patient_items:
            anon_val = patient_items[item]
            (group, element) = item.split(',')
            dsval = ds[group,element].value
            ds[group,element].value = anon_val
    else:
        print(f"patient_id {patient_id} not found")

def record_deident(table, key, ds, cs_before, cs_after):
    now = datetime.now(timezone.utc).isoformat()
    pri_key = f"{ds.PatientID}_{ds.SeriesDate}_{ds.SeriesTime}_{ds.InstanceNumber:03}"
    details = {
        'patient_id': pri_key,
        'file': key,
        'md5_before': cs_before,
        'md5_after': cs_after,
        'datetime': now
    }
    try:
        response = table.put_item(
            Item = details
        )
    except Exception as e:
        print(e)
        raise(e)

def ensure_ddb_table(table_name):

    try:
        table = ddb_r.Table(table_name)
        table.load()
    except Exception as e:
        try:            
            table = ddb_r.create_table(
                TableName=DDB_TABLE,
                KeySchema=[
                    {
                        'AttributeName': 'patient_id',
                        'KeyType': 'HASH'
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'patient_id',
                        'AttributeType': 'S'
                    },
                ],
                BillingMode='PAY_PER_REQUEST'
            )
        except Exception as e:
            print(e)
            raise(e)
    return(table)

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    table = ensure_ddb_table(DDB_TABLE)  # Logging table in DDB
    config = get_deident_config(EDITS_CONFIG_PARAM)  # De-ident JSON doc from SSM
    ds = read_dicom(bucket_name, key)
    cs_before = hashlib.md5(write_dataset_to_bytes(ds)).hexdigest()
    perform_deident(ds, config)  # Modify in memory
    bytes = write_dataset_to_bytes(ds)
    cs_after = hashlib.md5(bytes).hexdigest()
    write_dicom(bucket_name, key, bytes)
    record_deident(table, key, ds, cs_before, cs_after)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "key ": key
        })
    }