from prefect import flow
from prefect.logging import get_run_logger
from prefect.tasks import task


@task
def say_hello(name: str):
    logger = get_run_logger()
    logger.info(f"Hello {name}")


@flow
def hello_world():
    say_hello("Doramon")


if __name__ == "__main__":
    hello_world()
