import operator
from typing import Union, Generator, Optional, MutableMapping


import pyspark.sql.functions as sf
from pyspark.sql.dataframe import DataFrame


JoinStatementOrString = Union[str, Optional['JoinStatement']]


class JoinStatement:
    """
    Class for constructing a join statement for the `join` helper function

    Args:
        left (str_or_JoinStatement): The name of the column for the left dataframe | or another JoinStatement
        right (str_or_JoinStatement): The name of the column for the left dataframe | or another JoinStatement
        operation (str): The name of the operation
    """
    left_alias = "left"
    right_alias = "right"
    type_bitwise = "bitwise"
    type_comparison = "comparison"
    operators = {
        type_comparison: {
            "eq": operator.eq,
            "ne": operator.ne,
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

    def __init__(self, left: JoinStatementOrString, right: JoinStatementOrString, operation: str = "eq"):
        self_types = [left.__class__ == self.__class__, right.__class__ == self.__class__]
        if any(self_types):
            if not all(self_types):
                raise ValueError(
                    f"Either both `left` and `right` should be of type {self.__class__.__name__}; or neither"
                )
            self.operator_type = str(self.type_bitwise)
        else:
            self.operator_type = str(self.type_comparison)
        self.operator = self._validate_operator(operation, self.operators[self.operator_type], self.operator_type)
        self.left = left
        self.right = right

    def execute(self, left_data: DataFrame, right_data: DataFrame, recursive: bool = False) -> DataFrame:
        """
        The main public method that executes the join statement

        Args:
            left_data (Spark DataFrame): The left dataframe
            right_data (Spark DataFrame): The right dataframe
            recursive (bool): A flag that checks if the execution has gone into recursive mode

        Returns:
            The executed join statement

        Raises:
            NotImplementedError if:
                * The method has gone into recursive mode
                * Unidentified operation has been passed
        """
        if self.operator_type == self.type_comparison:
            return self._execute_comparison(left_data, right_data)
        elif self.operator_type == self.type_bitwise:
            return self._execute_bitwise(left_data, right_data, recursive)
        else:
            raise NotImplementedError(f"Operation type `{self.operator_type}` not implemented")

    def _execute_comparison(self, left_data: DataFrame, right_data: DataFrame) -> DataFrame:
        """
        [See self.execute(...)]
        """
        left_columns = [x.name for x in left_data.schema]
        right_columns = [x.name for x in right_data.schema]
        if self.left not in left_columns:
            raise ValueError(
                f"Column {self.left} not found in the `left` dataframe | Available columns = {left_columns}"
            )
        if self.right not in right_columns:
            raise ValueError(
                f"Column {self.right} not found in the `right` dataframe | Available columns = {right_columns}"
            )
        return self.operator(left_data[self.get_left_column(self.left)], right_data[self.get_right_column(self.right)])

    def _execute_bitwise(self, left_data: DataFrame, right_data: DataFrame, recursive: bool) -> DataFrame:
        """
        [See self.execute(...)]
        """
        if recursive:
            raise NotImplementedError("Recursive JoinStatement not implemented")
        return self.operator(
            self.left.execute(left_data, right_data, recursive=True),
            self.right.execute(left_data, right_data, recursive=True)
        )

    @classmethod
    def get_left_column(cls, column: str) -> str:
        return f"{cls.left_alias}.{column}"

    @classmethod
    def get_right_column(cls, column: str) -> str:
        return f"{cls.right_alias}.{column}"

    @staticmethod
    def _validate_operator(operation: str, mapping: dict, type_: str) -> operator:
        """
        Given an operation as sting returns the relevant (see `self.operators`) operator.<*>

        Args:
            operation (str): The name of the operator
            mapping (dict): The mapping for the operators, see `self.operators`
            type_ (str): The type of the operator | `comparison` or `bitwise`

        Returns:
            The `operator` object
        """
        operation = operation.strip().lower()
        error = f"Invalid operation ({operation} provided for {type_} | Valid operations = {list(mapping.keys())}"
        try:
            operation = mapping[operation]
        except KeyError:
            raise ValueError(error)
        return operation


class JoinValidator:
    """
    Validates the arguments passed to the `join` helper function

    Args:
        left (Spark DataFrame): The left dataframe
        right (Spark DataFrame): The right dataframe
        statement (JoinStatement): The join statement as a JoinStatement object
        duplicate_keep (str_or_list): When there are duplicate columns in the 2 dataframes, which columns to keep
    """
    def __init__(
            self, left: DataFrame, right: DataFrame, statement: JoinStatement,
            duplicate_keep: Union[str, list] = "left"
    ):
        if not isinstance(statement, JoinStatement):
            raise ValueError(f"Argument `statement` ({statement.__class__}) must be an instance of JoinStatement")

        self.left = self._validate_dataframe(left, "left")
        self.right = self._validate_dataframe(right, "right")
        self.left_columns = [x.name for x in self.left.schema]
        self.right_columns = [x.name for x in self.right.schema]
        self.left_duplicate_keep, self.right_duplicate_keep = self._validate_duplicate_keep_argument(
            duplicate_keep, self.left_columns, self.right_columns
        )

    @staticmethod
    def _check_if_left_or_right(argument, error):
        if type(argument) is str:
            argument = argument.strip().lower()
            if argument not in ["left", "right"]:
                raise ValueError(error)
            return argument
        return False

    @staticmethod
    def _check_if_list_items_are_strings(argument, error):
        if type(argument) is not list:
            raise ValueError(error)
        if all([type(x) is str for x in argument]):
            return argument
        raise ValueError(error)

    @staticmethod
    def _check_if_list_has_two_items(argument, error):
        if type(argument) is not list:
            raise ValueError(error)
        try:
            left, right = argument
        except ValueError:
            raise ValueError(error)
        return left, right

    @classmethod
    def _validate_duplicate_keep_argument(cls, duplicate_keep, left_columns, right_columns):
        error = (
            f"\nThe argument `duplicate_keep` ({duplicate_keep}) should be either:\n"
            "* A string with value 'left' or 'right'\n"
            "* A list of exactly 2 lists containing only strings: e.g. [['column_w', 'column_x'], ['column_y']]"
        )
        common_columns = sorted(set(left_columns).intersection(right_columns))
        check_if_left_or_right = cls._check_if_left_or_right(duplicate_keep, error)
        if check_if_left_or_right:
            if check_if_left_or_right == "left":
                left_duplicate_keep = list(common_columns)
                right_duplicate_keep = []
            else:
                left_duplicate_keep = []
                right_duplicate_keep = list(common_columns)
        else:
            left_duplicate_keep, right_duplicate_keep = cls._check_if_list_has_two_items(duplicate_keep, error)

        left_duplicate_keep = cls._check_if_list_items_are_strings(left_duplicate_keep, error)
        right_duplicate_keep = cls._check_if_list_items_are_strings(right_duplicate_keep, error)
        common_keep = sorted(set(left_duplicate_keep).intersection(right_duplicate_keep))
        if common_keep:
            raise ValueError(f"Some of the `duplicate_keep` columns defined for both dataframes: {common_keep}")

        missing_or_extra = sorted(set(common_columns).symmetric_difference(left_duplicate_keep + right_duplicate_keep))
        if missing_or_extra:
            raise ValueError(
                f"Some of the provided `duplicate_keep` ({duplicate_keep}) columns are "
                f"either extra or missing in the subset of the common columns: {common_columns}"
            )

        return left_duplicate_keep, right_duplicate_keep

    @staticmethod
    def _validate_dataframe(dataframe, which):
        if not isinstance(dataframe, DataFrame):
            raise ValueError(f"The argument `{which}` ({dataframe.__class__}) should be a Spark DataFrame")
        return dataframe
