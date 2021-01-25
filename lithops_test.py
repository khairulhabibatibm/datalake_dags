import lithops
# from airflow.models import Variable

def working(param):
    print(param)
    return "Hello " + param;

def run_lithops(*args):
    print("hello")
    # config = Variable.get("lithops_config", deserialize_json=True)
    # fexec = lithops.FunctionExecutor(config=config)
    # fexec.call_async(working,"World")
    # print(fexec.get_result())