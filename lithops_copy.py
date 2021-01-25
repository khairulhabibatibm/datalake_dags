import lithops

def copy(staging_pass):
    print("printing staging pass " + staging_pass)
    return "Success"

def lithops_run(*op_args):
    config = op_args[0]
    fexec = lithops.FunctionExecutor(config=config)
    fexec.call_async(copy,[op_args[1]])
    fexec.get_result()