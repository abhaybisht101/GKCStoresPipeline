from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.types import StructField,StructType,StringType,FloatType,IntegerType,TimestampType,DoubleType
import configparser
from datetime import datetime,date,timedelta,time
from src.main.python.gkcFunctions import readSchema
from pyspark.sql.functions import col,when

spark=SparkSession.builder.appName("dailyDataIngestAndRefine").master("local").getOrCreate()
config=configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation=config.get("paths","inputLocation")
outputLocation=config.get("paths","outputLocation")

today=datetime.now()
yesterday=today - timedelta(1)
# yesterdaySuffix='_' + yesterday.strftime('%d%m%Y')
# todaySuffix='_' + today.strftime('%d%m%Y')
yesterdaySuffix='_28062021'
todaySuffix='_29062021'

landingFileSchemaFromConf=config.get('schema','landingFileSchema')
landingFileSchema=readSchema(landingFileSchemaFromConf)
holdFileSchemaFromConf=config.get('schema','holdFileSchema')
holdFileSchema=readSchema(holdFileSchemaFromConf)

landingFileDF=spark.read.schema(schema=landingFileSchema)\
    .option("delimiter","|")\
    .csv(inputLocation + 'Sales_Landing\SalesDump' + todaySuffix)
landingFileDF.show()

holdFileDF=spark.read.schema(schema=holdFileSchema)\
    .option("delimiter","|")\
    .option("header",True)\
    .csv(outputLocation + 'Hold/HoldData' + yesterdaySuffix)  

landingFileDF.createOrReplaceTempView("landingView")
holdFileDF.createOrReplaceTempView("holdView")

refreshedFileDF=spark.sql("select a.Sale_ID,a.Product_ID, "
                          "CASE "
                          "WHEN(a.Quantity_Sold IS NULL) THEN b.Quantity_Sold "
                          "ELSE a.Quantity_Sold "
                          "END as Quantity_Sold, "
                          "CASE "
                          "WHEN(a.Vendor_ID IS NULL) THEN b.Vendor_ID "
                          "ELSE a.Vendor_ID "
                          "END as Vendor_ID, "
                          "a.Sale_Date,a.Sale_Amount,a.Sale_Currency "                                                   
                          "from landingView a left join holdView b "
                          "on a.Sale_ID=b.Sale_ID ")
refreshedFileDF.show()
refreshedFileDF.createOrReplaceTempView("refreshedView")
# landingFileDF.show()
# landingFileDF.printSchema()

# validLandingData=landingFileDF.filter(col('Quantity_Sold').isNotNull() & col('Vendor_Id').isNotNull())
# invalidLandingData=landingFileDF.filter(col('Quantity_Sold').isNull() | col('Vendor_Id').isNull())

validLandingData=refreshedFileDF.filter(col('Quantity_Sold').isNotNull() & col('Vendor_Id').isNotNull())
validLandingData.createOrReplaceTempView("validLandingView")
# invalidLandingData.show()
# validLandingData.show()

releasedFromHoldDF=spark.sql("select v.* from validLandingView v "
                           "Inner Join holdView h "
                           "on v.Sale_ID = h.Sale_ID")
releasedFromHoldDF.createOrReplaceTempView("releasedFromHoldView")
print("releasedFromHoldDF")
releasedFromHoldDF.show()

notReleasedFromHoldDF=spark.sql("select * from holdView h "
                                "where Sale_ID NOT IN (select Sale_ID from releasedFromHoldView)")
notReleasedFromHoldDF.createOrReplaceTempView("notReleasedFromHoldView")
print("notReleasedFromHoldDF")
notReleasedFromHoldDF.show()

invalidLandingData=refreshedFileDF.filter(col('Quantity_Sold').isNull() | col('Vendor_Id').isNull())\
    .withColumn("HoldReason", when(col("Quantity_Sold").isNull(),"Quantity Sold Missing")\
                .otherwise(when(col("Vendor_ID").isNull(),"Vendor ID Missing")))\
    .union(notReleasedFromHoldDF)
print("invalidLandingData")
invalidLandingData.show()

invalidLandingData.write.mode("overwrite")\
    .option("header",True)\
    .option("delimiter","|")\
    .csv(outputLocation + 'Hold/HoldData' +todaySuffix)

validLandingData.write.mode("overwrite")\
    .option("header",True)\
    .option("delimiter","|")\
    .csv(outputLocation + 'Valid/ValidData' +todaySuffix)