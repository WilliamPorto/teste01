from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, hash, col, expr, rand, lit, array
from pyspark import SparkContext

sc = SparkContext()
spark = SparkSession.builder.appName('apptest').master('local').getOrCreate()
spark.sparkContext.setLogLevel("WARN")


print("\n\n Inicio!!\n\n")


df = spark.range(100)                                    \
.withColumn('col2', hash(col('id')))                     \
.withColumn('col3', expr('pmod(col2, 4)'))               \
.withColumn('col4', rand())                              \
.withColumn('col5', ((rand()*10).cast('decimal(10,2)'))) \
.withColumn('col6', ((rand()*3).cast('int')) + 1)        \
.withColumn('col7', array( lit('Retail'),lit('SME'), lit('Cor'), ).getItem( (rand()*3).cast('int') ) )

df.show(truncate=False)

#orderItemsDF01                                          \
#  .write                                                \
#  .mode('overwrite')                                    \
#  .bucketBy(5, 'order_item_id')                         \
#  .sortBy('order_item_id')                              \
#  .option('path', '/user/will/data/tst/base01_bkt/')    \
#  .saveAsTable('orderItemsTB01')