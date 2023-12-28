# Create function factory to parse a line of text based on given schema
from pyspark.sql.types import StructType, IntegerType
from typing import List


def parse_line_factory(schema: StructType):
    def parse_line(line: str) -> List[str]:
        start = 0
        values = []
        for f in schema.fields:
            # get the string value
            length = f.metadata["length"]
            strVal = line[start : start + length].strip()
            start += length

            # convert to correct type and add to list
            type = f.dataType
            if type == IntegerType():
                if not strVal or not strVal.isdigit():
                    val = None
                else:
                    val = int(strVal)
            else:
                val = strVal
            values.append(val)

        return values

    return parse_line
