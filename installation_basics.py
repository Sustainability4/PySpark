# Pyspark is a highly beneficial tool to ensure that we are able to deal with large data at speed. 
# Pyspark also enables us to perform machine Learning with its ML library 
# Spark is developed by Apache. Its a framework that can be used with any language including java, scala, python
#Pyspark is the spark framework in Python 

!pip install pyspark
import pyspark

# If you are not already in a session its always better to create a session in pyspark 
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Practice").getOrCreate()

#Reading a file in spark 
df_spark = spark.read.csv("File_Path")
df_spark.display()

# The above function in spark will only lead to a situation where we can read the file without the header. 
# If you want to include the header then 

df_spark = spark.read.option('header','true').csv('File_path')

# The type of df_spark will be pyspark.sql.dataframe.DataFrame
# look at the head of the df_spark. This works the same as pandas datafranme

df_spark.head(3)

# Lets look the alternative in spark for pd.info()
df_spark.printSchema()
# There is something called nullable = true that you will see after executing the above command. This is important because 
# If nullable = true then the column cannot contain a null avriable. This is very similar to the idea in SQL regarding the same. 

df_spark = spark.read.option('header','true').csv('File_path')
# With the above command pyspark will read all the column as strigs. We need to justify one more option to ensure 
# that it reads the dataframe properly

df_spark = spark.read.option('header','true').csv('File_path',inferSchema = True)

# One more alternative of writing the same command
df_spark = spark.read.csv('File_path',header = True,inferSchema = True)

# To get the column names 
df_spark.columns()

# To get the specific columns 
# The type of this selected column will be a spark dataframe
df_spark.select(["column_1", "column_2"])

# To get the datatypes of the columns 
df_spark.dtypes()
df_spark.describe()

# To add, rename and drop a new column to a dataframe 
df_spark.withColumn(["Column_3"],df_spark["column_1"]+2)
df_spark.drop("Column_name")
df_spark.withRename("previous_columns","new_columns")


# Filter operations on Dataframe with Pyspark


