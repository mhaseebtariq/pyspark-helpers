from typing import Union, Generator, MutableMapping

import pyspark.sql.functions as sf
from pyspark.sql.dataframe import DataFrame

from .join import JoinValidator, JoinStatement


def join(
        left: DataFrame, right: DataFrame, statement: JoinStatement,
        how: str = "inner", overwrite_strategy: Union[str, list] = None
) -> DataFrame:
    """
    Joins two Spark dataframes in a foolproof manner

    [See JoinValidator and JoinStatement for details]
    """
    params = JoinValidator(left, right, statement, overwrite_strategy)

    left = params.left.alias(JoinStatement.left_alias)
    right = params.right.alias(JoinStatement.right_alias)
    final_columns = (
        # Common columns
        [f"{JoinStatement.get_left_column(x)}" for x in params.left_overwrite_columns] +
        [f"{JoinStatement.get_right_column(x)}" for x in params.right_overwrite_columns] +
        # Non-common columns
        [f"{JoinStatement.get_left_column(x)}" for x in set(params.left_columns).difference(params.right_columns)] +
        [f"{JoinStatement.get_right_column(x)}" for x in set(params.right_columns).difference(params.left_columns)]
    )
    selected = sorted(["".join(x.split(".")[1:]) for x in final_columns])
    return left.join(right, on=statement.execute(left, right), how=how).select(*final_columns).select(*selected)


def group_iterator(dataframe: DataFrame, group_by: Union[str, list]) -> Generator[tuple, None, None]:
    """
    Iterate over Spark dataframe as you would on a Pandas dataframe

    Args:
        dataframe (Spark DataFrame): The Spark dataframe
        group_by (str_or_list): The column(s) to group by

    Returns:
        A generator: [(group_name, group_dataframe), ...]

    NOTE: ALL THE DISTINCT VALUES FOR THE `GROUP` WILL BE LOADED INTO MEMORY!
    """
    if type(group_by) is str:
        group_by = [group_by]
    groups = [list(x) for x in dataframe.select(group_by).distinct().sort(group_by).collect()]
    for values in groups:
        filtered = dataframe.alias("new")
        for column, group in zip(group_by, values):
            filtered = filtered.filter(sf.col(column) == group)
        values = values[0] if len(values) == 1 else tuple(values)
        yield values, filtered


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
