import os
import urllib.parse

from prefect.environments.storage import GitHub
from prefect.environments import LocalEnvironment
from prefect.engine.executors import DaskExecutor
from prefect import Flow, task
import pandas as pd
from dask_gateway import Gateway
from dask_gateway.auth import JupyterHubAuth
from dask_gateway.client import GatewaySecurity


def score_check(grade, subject, student):
    """
    This is a normal "business logic" function which is not a Prefect task.
    If a student achieved a score > 90, multiply it by 2 for their effort! But only if the subject is not NULL.
    :param grade: number of points on an exam
    :param subject: school subject
    :param student: name of the student
    :return: final nr of points
    """
    if pd.notnull(subject) and grade > 90:
        new_grade = grade * 2
        print(f'Doubled score: {new_grade}, Subject: {subject}, Student name: {student}')
        return new_grade
    else:
        return grade


@task
def extract():
    """ Return a dataframe with students and their grades"""
    data = {'Name': ['Hermione', 'Hermione', 'Hermione', 'Hermione', 'Hermione',
                     'Ron', 'Ron', 'Ron', 'Ron', 'Ron',
                     'Harry', 'Harry', 'Harry', 'Harry', 'Harry'],
            'Age': [12] * 15,
            'Subject': ['History of Magic', 'Dark Arts', 'Potions', 'Flying', None,
                        'History of Magic', 'Dark Arts', 'Potions', 'Flying', None,
                        'History of Magic', 'Dark Arts', 'Potions', 'Flying', None],
            'Score': [100, 100, 100, 68, 99,
                      45, 53, 39, 87, 99,
                      67, 86, 37, 100, 99]}

    df = pd.DataFrame(data)
    return df


@task(log_stdout=True)
def transform(x):
    x["New_Score"] = x.apply(lambda row: score_check(grade=row['Score'],
                                                     subject=row['Subject'],
                                                     student=row['Name']), axis=1)
    return x


@task(log_stdout=True)
def load(y):
    old = y["Score"].tolist()
    new = y["New_Score"].tolist()
    print(f"ETL finished. Old scores: {old}. New scores: {new}")


def get_executor():
    print("Creating executor")
    auth = JupyterHubAuth(os.environ["PANGEO_TOKEN"])
    # external
    gateway = Gateway(
        address="https://staging.us-central1-b.gcp.pangeo.io/services/dask-gateway/",
        auth=auth
    )
    cluster = gateway.new_cluster(shutdown_on_close=False)
    print(cluster.dashboard_link)
    cluster.adapt(0, 10)

    # Running on the cluster, use the internal address.
    parsed = urllib.parse.urlparse(cluster.scheduler_address)
    scheduler_address = urllib.parse.urlunparse(
        parsed._replace(netloc="traefik-gcp-uscentral1b-staging-dask-gateway.staging:80")
    )
    print(scheduler_address)

    executor = DaskExecutor(
        address=scheduler_address,
        client_kwargs={"security": cluster.security}
    )
    return executor


with Flow("etl-flow",
          storage=GitHub(repo="TomAugspurger/prefect-demo", path="etl-flow.py"),
          environment=LocalEnvironment(executor=get_executor())) as flow:
    extracted_df = extract()
    transformed_df = transform(extracted_df)
    load(transformed_df)


if __name__ == '__main__':
    flow.register(project_name='pangeo-forge')
