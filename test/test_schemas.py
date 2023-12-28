import pytest
from learnspark.schemas import load_schema, get_type
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)


def test_load_schema():
    # Test load_schema with a known schema file
    schema = load_schema("data/fwf_fields.yaml")
    expected = StructType(
        [
            StructField("id", IntegerType(), True, {"length": 5}),
            StructField("name", StringType(), True, {"length": 10}),
            StructField("age", IntegerType(), True, {"length": 3}),
        ]
    )
    assert schema == expected


def test_get_type():
    # Test get_type with different types
    assert get_type("integer") == IntegerType()
    assert get_type("string") == StringType()
    assert get_type("float") == FloatType()

    # Test get_type with an unknown type
    with pytest.raises(ValueError):
        get_type("unknown")
