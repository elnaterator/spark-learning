import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from learnspark.parser import parse_line_factory


def test_parse_line_factory():
    # Define a schema
    schema = StructType(
        [
            StructField("id", IntegerType(), True, {"length": 5}),
            StructField("name", StringType(), True, {"length": 10}),
            StructField("age", IntegerType(), True, {"length": 3}),
        ]
    )

    # Create the parse_line function
    parse_line = parse_line_factory(schema)

    # Test the parse_line function
    line = "1    John Doe   25"
    expected = [1, "John Doe", 25]
    assert parse_line(line) == expected

    # Test with different data
    line = "2    Jane Doe   30"
    expected = [2, "Jane Doe", 30]
    assert parse_line(line) == expected

    # Test with empty string fields
    line = "3               35"
    expected = [3, "", 35]
    assert parse_line(line) == expected

    # Test with trailing spaces
    line = "  4  Ann Brown 40       "
    expected = [4, "Ann Brown", 40]
    assert parse_line(line) == expected
