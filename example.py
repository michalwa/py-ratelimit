from ratelimit import ratelimit, MINUTE


@ratelimit(10, per=MINUTE, session='name')
def hello(name):
    print(f"Hello, {name}!")
