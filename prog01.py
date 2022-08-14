from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import substring
from pyspark import SparkContext


sc = SparkContext()
HiveContext(sc)
spark = SparkSession.builder.appName('cca175_will').master('local').getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("\n\n Inicio!!\n\n")

ordersDF = spark.read             \
           .format('csv')         \
           .option('sep', ',')     \
           .schema('order_id int, orders_date string, order_customer_id int, order_status string') \
           .load('/user/will/data/retail_db/orders/order.csv')
           
ordersDF.show(truncate=False)

orderItemsDF = spark.                                                                                    \
             read.                                                                                     \
             format('json').                                                                            \
             schema('order_item_id int, order_item_order_id int, order_item_product_id int, order_item_quantity int, order_item_subtotal decimal(10,2), order_item_product_price decimal(10,2)').   \
             load('/user/will/data/retail_json_db/order_items/')
orderItemsDF.show(truncate=False)