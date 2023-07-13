from pyspark.sql import SparkSession
from pyspark.sql.functions import col,split
from pyspark.sql.types import *
import pandas as pd
import pathlib as Path

spark = SparkSession \
        .builder \
        .appName("getStructData") \
        .config("spark.sql.warehouse.dir", "path to warehouse") \
        .getOrCreate()

inputPath = '' # this was my path from local system

def getData(start, end):
    myList = []
    with open(inputPath) as f:
        try:
            for line in f:
                if line.startswith(start):
                    myList.append(line.strip())
                    for line in f:
                        if line.rstrip == end:
                            break
                        myList.append(line.rstrip())

        except Exception as e:
            print("Total failure")

    f.close()
    return(myList)




getList = getData('patient', 'days')

# lets put it into a dataframe

df = spark.createDataFrame(getList, StringType())
df.show()

#lets put it into a data frame

structdf = spark.createDataFrame(getList, StringType())
structdf.show()


#now we finish by creating columns

structdf = df.select(split(col("value"), ";").getItem(0).alia("col1"),
                     split(col("value"), ";").getItem(1).alia("col2")).drop("value")





