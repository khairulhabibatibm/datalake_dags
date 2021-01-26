import lithops
from MySQLdb import _mysql
import pandas as pd
import psycopg2

def working(param):
    print(param)
    db=_mysql.connect(host="cap-au-sg-prd-05.securegateway.appdomain.cloud",port=15208,user="habib",passwd="password123",db="testdev")
    db.query("""SELECT * FROM payment""")
    r = db.store_result()
    trx_result = r.fetch_row(maxrows=10,how=1)
    print("success load data from mysql")

    df = pd.DataFrame(trx_result)
    print(df.head())

    pgserver = '49ec7436-5643-423b-b0e4-158df3ec8b98.bqfh4fpt0vhjh7rs4ot0.databases.appdomain.cloud'
    pguser = 'ibm_cloud_cb7d01ec_dcac_47ec_8d74_52635347bb1c'
    pgpassword = param
    pgport = '31369'
    pgdb = 'ibmclouddb'

    pgconn = psycopg2.connect(user=pguser,password=pgpassword,host=pgserver,database=pgdb,port=pgport)
    pgcursor = pgconn.cursor()
    print(pgconn.get_dsn_parameters(),"\n")


    query_insert = """ INSERT INTO public.t_payment(payment_id,trx_id,payment_type,rc_code) values (%s,%s,%s,%s) """
    
    for index,row in df.iterrows():
        record = (row['payment_id'].decode('utf-8'),
                row['trx_id'].decode('utf-8'),
                row['payment_type'].decode('utf-8'),
                row['rc_code'].decode('utf-8'))
        pgcursor.execute(query_insert,record)
        
    
    pgconn.commit()
    return "Success staging payment"

def run_staging_payment(*args):
    fexec = lithops.FunctionExecutor(config=args[0])
    fexec.call_async(working,args[1])
    print(fexec.get_result())