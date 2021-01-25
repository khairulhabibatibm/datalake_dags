import lithops
from MySQLdb import _mysql
import psycopg2
import pandas as pd

def copy(slicenum):
    db=_mysql.connect(host="cap-au-sg-prd-05.securegateway.appdomain.cloud",port=15208,user="habib",passwd="password123",db="testdev")
    db.query("""SELECT * FROM user""")
    r = db.store_result()
    user_result = r.fetch_row(maxrows=0,how=1)
    df = pd.DataFrame(user_result)
    print(df.head())

    for index,row in df.iterrows():
        print(row['username'].decode('utf-8'), row['dob'].decode('utf-8'))

    pgserver = '49ec7436-5643-423b-b0e4-158df3ec8b98.bqfh4fpt0vhjh7rs4ot0.databases.appdomain.cloud'
    pguser = 'ibm_cloud_cb7d01ec_dcac_47ec_8d74_52635347bb1c'
    pgpassword = Variable.get("PG_STAGING_PASSWORD")
    pgport = '31369'
    pgdb = 'ibmclouddb'

    pgconn = psycopg2.connect(user=pguser,password=pgpassword,host=pgserver,database=pgdb,port=pgport)
    pgcursor = pgconn.cursor()
    print(pgconn.get_dsn_parameters(),"\n")

    query_insert = """ INSERT INTO public.t_user(username,job,dob,country) values (%s,%s,%s,%s) """
    
    for index,row in df.iterrows():
        record = (row['username'].decode('utf-8'),row['occupation'].decode('utf-8'),row['dob'].decode('utf-8'),row['country'].decode('utf-8'))
        pgcursor.execute(query_insert,record)
    
    pgconn.commit()

    return "Success"

def lithops_run(*op_args):

    # iam_api_key = Variable.get("IAM_APIKEY")
    # cos_api_key = Variable.get("COS_APIKEY")
    # config = {
    #     "lithops": {
    #         "storage_bucket": "lithops-fn",
    #         "storage": "ibm_cos",
    #         "mode": "serverless"
    #     },
    #     "serverless": {
    #         "backend": "ibm_cf",
    #         "runtime": "khairulhabib/lithops-runtime-datalake:1.0.4"
    #     },
    #     "ibm": {
    #         "iam_api_key": iam_api_key
    #     },
    #     "ibm_cf": {
    #         "endpoint": "https://us-south.functions.cloud.ibm.com",
    #         "namespace": "Namespace-H5L",
    #         "namespace_id": "7fd17f8c-4a89-4d08-9529-f9aa7737c52d"
    #     },
    #     "ibm_cos": {
    #         "endpoint": "https://s3.sng01.cloud-object-storage.appdomain.cloud",
    #         "private_endpoint": "https://s3.private.sng01.cloud-object-storage.appdomain.cloud",
    #         "api_key": cos_api_key
    #     }
    # }
    config = op_args[0]
    print(config)
    fexec = lithops.FunctionExecutor(config=config)
    fut = fexec.call_async(copy,"hello")
    print(fut.result())