from pyspark.sql.types import StringType,FloatType,IntegerType,TimestampType,DoubleType,StructType,StructField

def readSchema(schema_arg):
    d_types={
        "StringType()":StringType(),
        "IntegerType()":IntegerType(),
        "TimestampType()":TimestampType(),
        "DoubleType()":DoubleType(),
        "FloatType()":FloatType()
    }
    split_values=schema_arg.split(",")
    sch=StructType()                   #BlankSchema
    for i in split_values:
        x=i.split(" ")
        sch.add(x[0],d_types[x[1]],True)
    return sch