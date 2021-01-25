import lithops

def copy(staging_pass):
    print("without mysql and pandas and postgres")
    return "Success"

def lithops_run(*op_args):
    config = op_args[0]
    print(config)
    fexec = lithops.FunctionExecutor(config=config)
    result = fexec.call_async(copy,[op_args[1]])
    fexec.get_result()