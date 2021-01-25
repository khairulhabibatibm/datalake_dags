import lithops
from MySQLdb import _mysql
import pandas as pd
import psycopg2

def working(param):
    print(param)
    db=_mysql.connect(host="cap-au-sg-prd-05.securegateway.appdomain.cloud",port=15208,user="habib",passwd="password123",db="testdev")
    db.query("""SELECT * FROM user""")
    r = db.store_result()
    user_result = r.fetch_row(maxrows=0,how=1)
    print("success load data from mysql")

    df = pd.DataFrame(user_result)
    print(df.head())

    pgserver = '49ec7436-5643-423b-b0e4-158df3ec8b98.bqfh4fpt0vhjh7rs4ot0.databases.appdomain.cloud'
    pguser = 'ibm_cloud_cb7d01ec_dcac_47ec_8d74_52635347bb1c'
    pgpassword = '615b50b8183aadb25407cdc48e2e05e7a36300815649ae3c0e96fddc90ff00cf'
    pgport = '31369'
    pgdb = 'ibmclouddb'

    pgconn = psycopg2.connect(user=pguser,password=pgpassword,host=pgserver,database=pgdb,port=pgport)
    pgcursor = pgconn.cursor()
    print(pgconn.get_dsn_parameters(),"\n")

    return "Hello mysql pandas pgsql "

def run_lithops(*args):
    # print("hello")
    # config = Variable.get("lithops_config", deserialize_json=True)
    fexec = lithops.FunctionExecutor(config=args[0])
    fexec.call_async(working,args[1])
    print(fexec.get_result())