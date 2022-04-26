import operator

import pyspark.sql.functions as sf
from pyspark.sql.dataframe import DataFrame


def spark_group(dataframe, group):
    groups = [x[0] for x in dataframe.select(group).distinct().collect()]
    for x in groups:
        yield x, dataframe.filter(sf.col(group) == x)


class JoinStatement:
    left_alias = "left"
    right_alias = "right"
    type_bitwise = "bitwise"
    type_comparison = "comparison"
    operators = {
        type_comparison: {
            "eq": operator.eq,
            "le": operator.le,
            "ge": operator.ge,
            "lt": operator.lt,
            "gt": operator.gt,
        },
        type_bitwise: {
            "or": operator.or_,
            "and": operator.and_,
        }
    }

    def __init__(self, left, right, operation):
        self_types = [left.__class__ == self.__class__, right.__class__ == self.__class__]
        if any(self_types):
            if not all(self_types):
                raise ValueError(
                    f"Either both `left` and `right` should be of type {self.__class__.__name__}; or neither"
                )
            self.operator_type = str(self.type_bitwise)
        else:
            self.operator_type = str(self.type_comparison)
        self.operation = self._validate_operator(operation, self.operators[self.operator_type], self.operator_type)
        self.left = left
        self.right = right

    def execute(self, left_data, right_data, recursive=False):
        if self.operator_type == self.type_comparison:
            return self.operation(
                left_data[self.get_left_column(self.left)], right_data[self.get_right_column(self.right)]
            )
        elif self.operator_type == self.type_bitwise:
            if recursive:
                raise NotImplementedError("Recursive JoinStatement not implemented")
            return self.operation(
                self.left.execute(left_data, right_data, recursive=True),
                self.right.execute(left_data, right_data, recursive=True)
            )
        else:
            raise NotImplementedError(f"Operation type `{self.operator_type}` not implemented")

    @classmethod
    def get_left_column(cls, column):
        return f"{cls.left_alias}.{column}"

    @classmethod
    def get_right_column(cls, column):
        return f"{cls.right_alias}.{column}"

    @staticmethod
    def _validate_operator(operation, mapping, type_):
        operation = operation.strip().lower()
        error = f"Invalid operation ({operation} provided for {type_} | Valid operations = {list(mapping.keys())}"
        try:
            operation = mapping[operation]
        except KeyError:
            raise ValueError(error)
        return operation


def check_if_left_or_right(argument, error):
    if type(argument) is str:
        argument = argument.strip().lower()
        if argument not in ["left", "right"]:
            raise ValueError(error)
        return argument
    return False


def check_if_list_items_are_strings(argument, error):
    if type(argument) is not list:
        raise ValueError(error)
    if all([type(x) is str for x in argument]):
        return argument
    raise ValueError(error)


def check_if_list_has_two_items(argument, error):
    if type(argument) is not list:
        raise ValueError(error)
    try:
        left, right = argument
    except ValueError:
        raise ValueError(error)
    return left, right


def validate_duplicate_keep_argument(duplicate_keep, left_columns, right_columns):
    error = (
        f"The argument `duplicate_keep` ({duplicate_keep}) should be either:\n"
        "* A string with value 'left' or 'right'\n"
        "* A list of exactly 2 lists: e.g. [['column_w', 'column_x'], ['column_y']]"
    )
    common_columns = sorted(set(left_columns).intersection(right_columns))
    duplicate_keep = check_if_left_or_right(duplicate_keep, error)
    if duplicate_keep:
        if duplicate_keep == "left":
            left_duplicate_keep = list(common_columns)
            right_duplicate_keep = []
        else:
            left_duplicate_keep = []
            right_duplicate_keep = list(common_columns)
    else:
        left_duplicate_keep, right_duplicate_keep = check_if_list_has_two_items(duplicate_keep, error)

    left_duplicate_keep = check_if_list_items_are_strings(left_duplicate_keep, error)
    right_duplicate_keep = check_if_list_items_are_strings(right_duplicate_keep, error)
    common_keep = sorted(set(left_duplicate_keep).intersection(right_duplicate_keep))
    if common_keep:
        raise ValueError(f"Some of the `duplicate_keep` columns found for both the dataframes - {common_keep}")

    missing_or_extra = sorted(set(common_columns).symmetric_difference(left_duplicate_keep + right_duplicate_keep))
    if missing_or_extra:
        raise ValueError(
            f"Some of the provided `duplicate_keep` ({duplicate_keep}) columns are "
            f"either extra or missing in the subset of the common columns: {common_columns}"
        )

    return left_duplicate_keep, right_duplicate_keep


def validate_dataframe(dataframe, which):
    if not isinstance(dataframe, DataFrame):
        raise ValueError(f"The argument `{which}` ({dataframe.__class__}) should be a Spark DataFrame")
    return dataframe


def join(left, right, statement, how="left", duplicate_keep="left"):
    left = validate_dataframe(left, "left")
    right = validate_dataframe(right, "right")
    if not isinstance(statement, JoinStatement):
        raise ValueError(f"Argument `statement` ({statement.__class__}) must be an instance of JoinStatement")
    left_columns = [x.name for x in left.schema]
    right_columns = [x.name for x in right.schema]
    left_duplicate_keep, right_duplicate_keep = validate_duplicate_keep_argument(
        duplicate_keep, left_columns, right_columns
    )
    left = left.alias(JoinStatement.left_alias)
    right = right.alias(JoinStatement.right_alias)
    final_columns = sorted(
        # Common columns
        [f"{JoinStatement.get_left_column(x)}" for x in left_duplicate_keep] +
        [f"{JoinStatement.get_right_column(x)}" for x in right_duplicate_keep] +
        # Non-common columns
        [f"{JoinStatement.get_left_column(x)}" for x in set(left_columns).difference(right_columns)] +
        [f"{JoinStatement.get_right_column(x)}" for x in set(right_columns).difference(left_columns)]
    )
    return left.join(right, on=statement.execute(left, right), how=how).select(*final_columns)
