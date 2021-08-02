from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import StructField,StructType,StringType,FloatType,IntegerType,TimestampType,DoubleType
import configparser
from datetime import datetime,date,timedelta,time
from src.main.python.gkcFunctions import readSchema
from pyspark.sql.functions import col,when

spark=SparkSession.builder.appName("VendorEnrichment").master("local").getOrCreate()
config=configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation=config.get('paths','inputLocation')
outputLocation=config.get("paths","outputLocation")

yesterdaySuffix='_28062021'
todaySuffix='_29062021'

productEnrichedFileSchemaFromConf=config.get('schema','productEnrichedFileSchema')
productEnrichedFileSchema=readSchema(productEnrichedFileSchemaFromConf)

vendorFileSchemaFromConf=config.get('schema','vendorFileSchema')
vendorFileSchema=readSchema(vendorFileSchemaFromConf)

usdreferenceSchemaFromConf=config.get('schema','usdReferenceSchema')
usdReferenceSchema=readSchema(usdreferenceSchemaFromConf)

productEnrichedDF=spark.read.schema(schema=productEnrichedFileSchema)\
    .option("delimiter","|")\
    .option("header",True)\
    .csv(outputLocation + 'Enriched\SaleAmountEnrichment\SaleAmountEnriched' +todaySuffix)
productEnrichedDF.show()
productEnrichedDF.createOrReplaceTempView("ProductEnrichedView")

vendorDF=spark.read.schema(schema=vendorFileSchema)\
    .option("delimiter","|")\
    .option("header",False)\
    .csv(inputLocation + 'Vendors')
vendorDF.show()
vendorDF.createOrReplaceTempView("VendorView")

usdReferenceDF=spark.read.schema(schema=usdReferenceSchema)\
    .option("delimiter","|")\
    .option("header","false")\
    .csv(inputLocation + 'USD_Rates')
usdReferenceDF.show()
usdReferenceDF.createOrReplaceTempView("usdReferenceView")


VendorEnrichedDF=spark.sql("select p.*,v.Vendor_Name FROM "
                           "ProductEnrichedView p INNER JOIN VendorView v "
                           "ON p.Vendor_ID=v.Vendor_ID")
VendorEnrichedDF.createOrReplaceTempView("VendorEnrichedView")

usdEnrichedDF=spark.sql("select v.*, round((v.Sale_Amount/u.Exchange_Rate),2) as USDAmount FROM "
                        "VendorEnrichedView v INNER JOIN usdReferenceView u "
                        "ON v.Sale_Currency = u.Currency_Code")

usdEnrichedDF.write.mode("Overwrite")\
    .option("header",True)\
    .option("delimiter",'|')\
    .csv(outputLocation + 'Enriched\Vendor_USD_Enriched\Vendor_USD_Enriched' +todaySuffix)

usdEnrichedDF.write.format('jdbc').options(
    url='jdbc:mysql://localhost:3306/gkcstorepipelinedb',
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='final_sales',
    user='root',
    password='root').mode('append').save()