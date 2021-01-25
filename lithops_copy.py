import lithops
from MySQLdb import _mysql
import psycopg2
import pandas as pd

def copy(staging_pass):
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
    pgpassword = staging_pass
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
    config = op_args[0]
    print(config)
    fexec = lithops.FunctionExecutor(config=config)
    fut = fexec.call_async(copy,op_args[1])
    print(fut.result())