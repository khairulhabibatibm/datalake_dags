import lithops
from MySQLdb import _mysql

def working(param):
    print(param)
    db=_mysql.connect(host="cap-au-sg-prd-05.securegateway.appdomain.cloud",port=15208,user="habib",passwd="password123",db="testdev")
    db.query("""SELECT * FROM user""")
    r = db.store_result()
    user_result = r.fetch_row(maxrows=0,how=1)
    return "Hello staging " + param

def run_lithops(*args):
    # print("hello")
    # config = Variable.get("lithops_config", deserialize_json=True)
    fexec = lithops.FunctionExecutor(config=args[0])
    fexec.call_async(working,args[1])
    print(fexec.get_result())