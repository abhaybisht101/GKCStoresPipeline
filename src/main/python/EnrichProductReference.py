from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import StructField,StructType,StringType,FloatType,IntegerType,TimestampType,DoubleType
import configparser
from datetime import datetime,date,timedelta,time
from src.main.python.gkcFunctions import readSchema
from pyspark.sql.functions import col,when

spark=SparkSession.builder.appName("EnrichProductReference").master("local").getOrCreate()
config=configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation=config.get('paths','inputLocation')
outputLocation=config.get("paths","outputLocation")

validFileSchemaFromConf=config.get('schema','validFileSchema')
validFileSchema=readSchema(validFileSchemaFromConf)
yesterdaySuffix='_28062021'
todaySuffix='_29062021'

validDataDF=spark.read.schema(schema=validFileSchema)\
    .option("delimiter","|")\
    .option("header",True)\
    .csv(outputLocation + 'Valid/ValidData' +todaySuffix)
validDataDF.show()
validDataDF.createOrReplaceTempView("ValidView")

productFileSchemaFromConf=config.get('schema','productFileSchema')
productFileSchema=readSchema(productFileSchemaFromConf)

productDataDF=spark.read.schema(schema=productFileSchema)\
    .option("delimiter","|")\
    .option("header",True)\
    .csv(inputLocation + 'Products\GKProductList.dat')
productDataDF.show()
productDataDF.createOrReplaceTempView("ProductView")


productEnrichedDF=spark.sql("select v.Sale_ID,p.Product_ID,p.Product_Name, "
                         "v.Quantity_Sold,v.Vendor_ID,v.Sale_Date, "
                         "p.Product_Price * v.Quantity_Sold as Sale_Amount,v.Sale_Currency FROM "
                         "ValidView v INNER JOIN ProductView p "
                         "on v.Product_ID=p.Product_ID")
productEnrichedDF.createOrReplaceTempView("ProductEnrichedView")
productEnrichedDF.show()

productEnrichedDF.write.mode("overwrite")\
    .option("header",True)\
    .option("delimiter","|")\
    .csv(outputLocation + 'Enriched\SaleAmountEnrichment\SaleAmountEnriched' +todaySuffix)
