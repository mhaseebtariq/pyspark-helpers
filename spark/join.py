import operator
from typing import Union, Optional


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

    def __init__(self, left: JoinStatementOrString, right: JoinStatementOrString = None, operation: str = "eq"):
        if right is None:
            if isinstance(left, self.__class__):
                raise ValueError(f"Please provide `right`, when `left` is an instance of {self.__class__.__name__}")
            right = str(left)
        self_types = [isinstance(x, self.__class__) for x in [left, right]]
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
        overwrite_strategy (str_or_list): The selection strategy to adopt when there are overlapping columns:
            * "left": Use all the intersecting columns from the left dataframe
            * "right": Use all the intersecting columns from the right dataframe
            * [["column_x_in_left", "column_y_in_left"], ["column_z_in_left"]]: Provide column names for both
    """
    def __init__(
            self, left: DataFrame, right: DataFrame, statement: JoinStatement, overwrite_strategy: Union[str, list]
    ):
        if not isinstance(statement, JoinStatement):
            raise ValueError(f"Argument `statement` ({statement.__class__}) must be an instance of JoinStatement")

        self.left = self._validate_dataframe(left, "left")
        self.right = self._validate_dataframe(right, "right")
        self.left_columns = [x.name for x in self.left.schema]
        self.right_columns = [x.name for x in self.right.schema]
        self.left_overwrite_columns, self.right_overwrite_columns = self._validate_overwrite_strategy_argument(
            overwrite_strategy, self.left_columns, self.right_columns
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
    def _validate_overwrite_strategy_argument(cls, overwrite_strategy, left_columns, right_columns):
        error = (
            f"\nThe argument `overwrite_strategy` ({overwrite_strategy}) should be either:\n"
            "* A string with value 'left' or 'right'\n"
            "* A list of exactly 2 lists containing only strings: e.g. [['column_w', 'column_x'], ['column_y']]"
        )
        common_columns = sorted(set(left_columns).intersection(right_columns))
        if common_columns:
            if not overwrite_strategy:
                raise ValueError(
                    f"\n\nOverlapping columns found in the dataframes: {common_columns}"
                    "\nPlease provide the `overwrite_strategy` argument therefore, to select a selection strategy:"
                    '\n\t* "left": Use all the intersecting columns from the left dataframe'
                    '\n\t* "right": Use all the intersecting columns from the right dataframe'
                    '\n\t* [["x_in_left", "y_in_left"], ["z_in_right"]]: Provide column names for both\n'
                )
        else:
            overwrite_strategy = "left"
        check_if_left_or_right = cls._check_if_left_or_right(overwrite_strategy, error)
        if check_if_left_or_right:
            if check_if_left_or_right == "left":
                left_overwrite_columns = list(common_columns)
                right_overwrite_columns = []
            else:
                left_overwrite_columns = []
                right_overwrite_columns = list(common_columns)
        else:
            left_overwrite_columns, right_overwrite_columns = cls._check_if_list_has_two_items(
                overwrite_strategy, error
            )

        left_overwrite_columns = cls._check_if_list_items_are_strings(left_overwrite_columns, error)
        right_overwrite_columns = cls._check_if_list_items_are_strings(right_overwrite_columns, error)
        common_keep = sorted(set(left_overwrite_columns).intersection(right_overwrite_columns))
        if common_keep:
            raise ValueError(f"Some of the `overwrite_strategy` columns defined for both dataframes: {common_keep}")

        missing_or_extra = sorted(
            set(common_columns).symmetric_difference(left_overwrite_columns + right_overwrite_columns)
        )
        if missing_or_extra:
            raise ValueError(
                f"Some of the provided `overwrite_strategy` ({overwrite_strategy}) columns are "
                f"either extra or missing in the subset of the common columns: {common_columns}"
            )

        return sorted(set(left_overwrite_columns)), sorted(set(right_overwrite_columns))

    @staticmethod
    def _validate_dataframe(dataframe, which):
        if not isinstance(dataframe, DataFrame):
            raise ValueError(f"The argument `{which}` ({dataframe.__class__}) should be a Spark DataFrame")
        return dataframe
