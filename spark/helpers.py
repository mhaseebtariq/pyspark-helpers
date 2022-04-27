from typing import Union, Generator, MutableMapping

import pyspark.sql.functions as sf
from pyspark.sql.dataframe import DataFrame

from .join import JoinValidator, JoinStatement


def join(
        left: DataFrame, right: DataFrame, statement: JoinStatement,
        how: str = "inner", duplicate_keep: Union[str, list] = "left"
):
    """
    Joins two spark dataframes in a fool-proof manner

    [See JoinValidator and JoinStatement for details]
    """
    params = JoinValidator(left, right, statement, duplicate_keep)

    left = params.left.alias(JoinStatement.left_alias)
    right = params.right.alias(JoinStatement.right_alias)
    final_columns = (
        # Common columns
        [f"{JoinStatement.get_left_column(x)}" for x in params.left_duplicate_keep] +
        [f"{JoinStatement.get_right_column(x)}" for x in params.right_duplicate_keep] +
        # Non-common columns
        [f"{JoinStatement.get_left_column(x)}" for x in set(params.left_columns).difference(params.right_columns)] +
        [f"{JoinStatement.get_right_column(x)}" for x in set(params.right_columns).difference(params.left_columns)]
    )
    selected = sorted(["".join(x.split(".")[1:]) for x in final_columns])
    return left.join(right, on=statement.execute(left, right), how=how).select(*final_columns).select(*selected)


def group_iterator(dataframe: DataFrame, group: str) -> Generator[tuple, None, None]:
    """
    Iterate over Spark dataframe as you would on a Pandas dataframe

    Args:
        dataframe (Spark DataFrame): The Spark dataframe
        group (str): The column to group by

    Returns:
        A generator: [(group_name, group_dataframe), ...]
    """
    groups = [x[0] for x in dataframe.select(group).distinct().sort(group).collect()]
    for x in groups:
        yield x, dataframe.filter(sf.col(group) == x)


def change_schema(dataframe: DataFrame, schema: MutableMapping) -> DataFrame:
    """
    Change the schema of the dataframe based on the (complete or subset of) the schema provided

    Args:
        dataframe (Spark Dataframe): The input dataframe
        schema (MutableMapping): Key-value pair for the schema {column_name_1: new_type, column_name_2: ...}

    Returns:
        The dataframe with the new schema
    """
    columns = [x.name for x in dataframe.schema]
    missing_columns = sorted(set(schema.keys()).difference(columns))
    if missing_columns:
        raise ValueError(
            f"Some of the columns provided in the schema ({missing_columns}) are not present in the dataframe | "
            f"Available columns = {columns}"
        )
    for column, column_type in schema.items():
        dataframe = dataframe.withColumn(column, sf.col(column).cast(column_type))
    return dataframe
