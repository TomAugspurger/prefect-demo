import os
import urllib.parse

from prefect.environments.storage import Docker
from prefect.environments import DaskKubernetesEnvironment
from prefect import Flow, task
import pandas as pd


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
        print(
            f"Doubled score: {new_grade}, Subject: {subject}, Student name: {student}"
        )
        return new_grade
    else:
        return grade


@task
def extract():
    """ Return a dataframe with students and their grades"""
    data = {
        "Name": [
            "Hermione",
            "Hermione",
            "Hermione",
            "Hermione",
            "Hermione",
            "Ron",
            "Ron",
            "Ron",
            "Ron",
            "Ron",
            "Harry",
            "Harry",
            "Harry",
            "Harry",
            "Harry",
        ],
        "Age": [12] * 15,
        "Subject": [
            "History of Magic",
            "Dark Arts",
            "Potions",
            "Flying",
            None,
            "History of Magic",
            "Dark Arts",
            "Potions",
            "Flying",
            None,
            "History of Magic",
            "Dark Arts",
            "Potions",
            "Flying",
            None,
        ],
        "Score": [100, 100, 100, 68, 99, 45, 53, 39, 87, 99, 67, 86, 37, 100, 99],
    }

    df = pd.DataFrame(data)
    return df


@task(log_stdout=True)
def transform(x):
    x["New_Score"] = x.apply(
        lambda row: score_check(
            grade=row["Score"], subject=row["Subject"], student=row["Name"]
        ),
        axis=1,
    )
    return x


@task(log_stdout=True)
def load(y):
    old = y["Score"].tolist()
    new = y["New_Score"].tolist()
    print(f"ETL finished. Old scores: {old}. New scores: {new}")


flow = Flow(
    "etl-flow",
    storage=Docker(
        "tomaugspurger",
        python_dependencies=[
            "pandas==1.1.0",
            "dask_gateway",
            "dask==2.25.0",
            "distributed==2.25.0",
        ],
        image_tag="latest",
    ),
    environment=DaskKubernetesEnvironment(min_workers=1, max_workers=3),
)
with flow:
    extracted_df = extract()
    transformed_df = transform(extracted_df)
    load(transformed_df)


if __name__ == "__main__":
    flow.register(project_name="pangeo-forge")
