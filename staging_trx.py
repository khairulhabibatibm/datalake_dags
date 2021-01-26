import lithops
from MySQLdb import _mysql
import pandas as pd
import psycopg2

pgpassword = ""

def working(param):
    print(param)
    db=_mysql.connect(host="cap-au-sg-prd-05.securegateway.appdomain.cloud",port=15208,user="habib",passwd="password123",db="testdev")
    db.query("SELECT * FROM trx where trx_id = "+param+"")
    r = db.store_result()
    trx_result = r.fetch_row(maxrows=10,how=1)
    print("success load data from mysql")

    df = pd.DataFrame(trx_result)
    print(df.head())

    pgserver = '49ec7436-5643-423b-b0e4-158df3ec8b98.bqfh4fpt0vhjh7rs4ot0.databases.appdomain.cloud'
    pguser = 'ibm_cloud_cb7d01ec_dcac_47ec_8d74_52635347bb1c'
    pgport = '31369'
    pgdb = 'ibmclouddb'

    pgconn = psycopg2.connect(user=pguser,password=pgpassword,host=pgserver,database=pgdb,port=pgport)
    pgcursor = pgconn.cursor()
    print(pgconn.get_dsn_parameters(),"\n")


    query_insert = """ INSERT INTO public.t_trx(trx_id,username,item_id,qty,amount,trx_type) values (%s,%s,%s,%s,%s,%s) """
    
    for index,row in df.iterrows():
        record = (row['trx_id'].decode('utf-8'),
                row['username'].decode('utf-8'),
                row['item_id'].decode('utf-8'),
                row['qty'].decode('utf-8'),
                row['amount'].decode('utf-8'),
                row['trx_type'].decode('utf-8'))
        pgcursor.execute(query_insert,record)
        
    
    pgconn.commit()
    return "Success staging trx"

def run_staging_trx(*args):
    # print("hello")
    # config = Variable.get("lithops_config", deserialize_json=True)
    fexec = lithops.FunctionExecutor(config=args[0])
    pgpassword = args[1]
    # fexec.call_async(working,args[1])
    fexec.map(working,[10,11,12,13,14,15,16,17,18,19,20])
    print(fexec.get_result())