from ratelimit import ratelimit, MINUTE


@ratelimit(10, per=MINUTE, session='chuj')
def hello(name):
    print(f"Hello, {name}!")
