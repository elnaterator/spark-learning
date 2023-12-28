import yaml
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DataType,
    FloatType,
    DoubleType,
    BooleanType,
)


def load_schema(schema_file: str, root: str = "fields") -> StructType:
    schema = StructType()

    with open(schema_file) as f:
        field_defs = yaml.safe_load(f)[root]

    for f in field_defs:
        schema.add(
            StructField(
                f["name"], get_type(f["type"]), True, metadata={"length": f["length"]}
            )
        )

    return schema


def get_type(type: str) -> DataType:
    if type == "string":
        return StringType()
    elif type == "integer":
        return IntegerType()
    elif type == "float":
        return FloatType()
    elif type == "double":
        return DoubleType()
    elif type == "boolean":
        return BooleanType()
    else:
        raise ValueError("Unknown type: " + type)
