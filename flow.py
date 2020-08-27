from prefect import task, Flow


@task(log_stdout=True)
def say_hello():
    print("Hello, world!")


with Flow("My First Flow") as flow:
    say_hello()
