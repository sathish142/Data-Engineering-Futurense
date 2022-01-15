from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, regexp_replace, split, explode
from pyspark.sql.types import FloatType


def load_data_to_s3(dataframe, output_directory):
    target = 'local'
    if target == 's3':
        # S3 BUCKET LOCATION FOR STORING THE OUTPUT FILE
        output_path = "s3a://projectbkforstoretransformedoutput/"
    elif target == 'local':
        # LOCAL SAVE FILE
        output_path = "/home/sathish/project/output/"
    # LOAD DATAFRAME AS CSV FILE IN S3 WITH OVERWRITE METHOD
    dataframe.write.format('csv').option('header', 'true').mode('overwrite').save(output_path + output_directory)


def restaurant_type_similar(dataframe):
    dataframe = dataframe.groupBy("city", "location", "rest_type").count().na.drop().\
        orderBy(col('count').desc()).withColumnRenamed('count', 'no_of_restaurants')
    # EXAMPLE FILTER Brigade Road' AND rest_type = 'Bar'"
    dataframe = dataframe.where("city = 'Brigade Road'").where("rest_type = 'Bar'")
    # S3 OUTPUT DIRECTORY PATH
    output_directory = "restaurant_type_similar"
    # LOAD DATAFRAME TO S3 FUNCTION CALL
    load_data_to_s3(dataframe, output_directory)


def neighborhood_similar_food(dataframe):
    dataframe = dataframe.groupBy("city", "location", "type_of_meal").count().na.drop().\
        withColumnRenamed('count', 'no_of_restaurants')
    # EXAMPLE FILTER 'Brigade Road' AND "type_of_meal = 'Delivery'"
    dataframe = dataframe.where("city = 'Brigade Road'").where("type_of_meal = 'Delivery'")
    # S3 OUTPUT DIRECTORY PATH
    output_directory = "neighborhood_similar_food"
    # LOAD DATAFRAME TO S3 FUNCTION CALL
    load_data_to_s3(dataframe, output_directory)


def location_best_restaurant(dataframe):
    # DROPPING NOT NEEDED FIELDS
    dataframe = dataframe.drop('online_order', 'book_table', 'votes', 'rest_type', 'dish_liked', 'cuisines',
                               'approx_cost_for_two_people', 'type_of_meal', 'menu_item', 'city')
    # DROP NULL FIELDS
    dataframe = dataframe.na.drop()
    # CHANGE DATA TYPE FOR 'rating'
    dataframe = dataframe.withColumn("label", col("rating").cast(FloatType())).drop('rating').\
        withColumnRenamed('label', 'rating')
    # DROP NULL FIELDS
    dataframe = dataframe.na.drop()
    # GROUPBY FOR 'location' & 'name' AND FIND COUNT
    dataframe = dataframe.groupby('location', 'name').agg({'rating': 'max'}).\
        withColumnRenamed('name', 'restaurant_name').withColumnRenamed('max(rating)', 'rating')
    # S3 OUTPUT DIRECTORY PATH
    output_directory = "location_best_restaurant"
    # LOAD DATAFRAME TO S3 FUNCTION CALL
    load_data_to_s3(dataframe, output_directory)


def online_offline_order_restaurants(dataframe):
    offline_order_restaurant = dataframe.where("online_order != 'Yes'").select('location', 'name').\
        withColumnRenamed('name', 'restaurant_name')
    online_order_restaurant = dataframe.where("online_order = 'Yes'").select('location', 'name').\
        withColumnRenamed('name', 'restaurant_name')
    # offline_order_restaurant_count = offline_order_restaurant.groupBy('location').count().orderBy(desc('count')).\
    #     withColumnRenamed('count', 'no_of_restaurants')
    # online_order_restaurant_count = online_order_restaurant.groupBy('location').count().orderBy(desc('count')).\
    #     withColumnRenamed('count', 'no_of_restaurants')
    # S3 OUTPUT DIRECTORY PATH
    output_directory_1 = "offline_order_restaurant"
    output_directory_2 = "online_order_restaurant"
    # LOAD DATAFRAME TO S3 FUNCTION CALL
    load_data_to_s3(offline_order_restaurant, output_directory_1)
    load_data_to_s3(online_order_restaurant, output_directory_2)


def location_most_liked_food(dataframe):
    # SPLIT THE 'dish_liked' FIELD, USING EXPLODE TO CREATE ARRAY TYPE, DROPPING 'dish_liked_arr' FIELD, SELECT REQUIRED
    # FIELD, GROUPBY WITH 'location' & 'most_liked_dish', SORTING RECORDS BY 'count' DESC
    dataframe = dataframe.select('*', split('dish_liked', ',').alias('dish_liked_arr')).\
        withColumn('most_liked_dish', explode('dish_liked_arr')).\
        drop('dish_liked_arr').\
        select('location', 'most_liked_dish').\
        groupBy('location', 'most_liked_dish').count().\
        orderBy(col('count').desc())
    # RENAME THE FIELD 'count' TO 'no_of_restaurants'
    dataframe = dataframe.withColumnRenamed('count', 'no_of_restaurants')
    # S3 OUTPUT DIRECTORY PATH
    output_directory = "location_most_liked_food"
    # LOAD DATAFRAME TO S3 FUNCTION CALL
    load_data_to_s3(dataframe, output_directory)


def load_data(spark):
    source = 'hdfs'
    if source == 'hdfs':
        # HDFS SOURCE DATASET FILE PATH
        source_location = 'hdfs://127.0.0.1:9000/dataset/zomato.csv'
    elif source == 's3':
        # AWS S3 SOURCE DATASET FILE PATH
        source_location = 's3a://projectbktforsourcedataset/zomato.csv'
    # READ THE SOURCE FILE
    dataframe = spark.read.format("csv").\
        option("multiline", True).\
        option('escape', "\"").\
        option("inferSchema", True).\
        option("header", "True").\
        load(source_location)
    # INITIAL TRANSFORM:
    # DROPPING AND RENAMING THE FIELDS
    dataframe = dataframe.drop('url', 'address', 'phone', 'reviews_list').\
        withColumn("new_rating", expr("substring(rate, 1, length(rate)-2)")).\
        withColumnRenamed("new_rating", "rating").\
        withColumn('menu_item_new', regexp_replace('menu_item', '\[\]', '')).\
        drop('rate', 'menu_item').\
        withColumnRenamed('menu_item_new', 'menu_item').\
        withColumnRenamed('approx_cost(for two people)', 'approx_cost_for_two_people').\
        withColumnRenamed("listed_in(type)", 'type_of_meal').\
        withColumnRenamed("listed_in(city)", 'city')
    # SELECT THE FIELDS BY AS INPUT ORDER
    dataframe = dataframe.select('name', 'online_order', 'book_table', 'rating', 'votes', 'location', 'rest_type',
                                 'dish_liked', 'cuisines', 'approx_cost_for_two_people', 'menu_item', 'type_of_meal',
                                 'city')
    # COPY ORIGINAL DATAFRAME
    original_dataframe = dataframe
    return dataframe, original_dataframe


def main():
    # CREATE SPARK SESSION
    spark = SparkSession.builder.appName("BengaluruRestaurant").getOrCreate()
    # LOADING AND INITIAL TRANSFORMATION FUNCTION AND GET THE DATAFRAME
    dataframe, original_dataframe = load_data(spark)
    # FINDING MOST LIKED FOOD FOR EACH LOCATION FUNCTION CALL
    location_most_liked_food(dataframe)
    online_offline_order_restaurants(dataframe)
    location_best_restaurant(dataframe)
    neighborhood_similar_food(dataframe)
    restaurant_type_similar(dataframe)


if __name__ == '__main__':
    main()
