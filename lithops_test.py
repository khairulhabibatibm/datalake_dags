import lithops

def working(param):
    print(param)
    return "Hello " + param;

def run_lithops(*args):
    # print("hello")
    # config = Variable.get("lithops_config", deserialize_json=True)
    fexec = lithops.FunctionExecutor(config=args[0])
    fexec.call_async(working,"World")
    print(fexec.get_result())