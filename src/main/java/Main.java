import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;


public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .config("spark.jars", "/home/maria_dev/201_spark_batching/target/201_project_batching-1.0-SNAPSHOT.jar")
                .enableHiveSupport().getOrCreate();

        Dataset<Row> hotels = spark.read().option("header", "true")
                .csv("hdfs://sandbox-hdp.hortonworks.com:8020/201_hotels");
//        Dataset<Row> hotels_rounded = hotels.withColumn("Latitude_rounded", functions.round(hotels.col("Latitude"), 2))
//                .withColumn("Longitude_rounded", functions.round(hotels.col("Longitude"), 2));

        Dataset<Row> weather = spark.read()
                .parquet("hdfs://sandbox-hdp.hortonworks.com:8020/201_weather");
//        Dataset<Row> weather_rounded = weather.withColumn("lat_rounded", functions.round(weather.col("lat"), 2))
//                .withColumn("lng_rounded", functions.round(weather.col("lng"), 2));

        Dataset<Row> expedia = spark.read()
                .format("com.databricks.spark.avro")
                .load("hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia").persist();

//      Dataset<Row> hotels_weather_joined = hotels_rounded
//                .join(weather_rounded, hotels_rounded.col("Latitude_rounded").equalTo(weather_rounded.col("lat_rounded"))
//                .and(hotels_rounded.col("Longitude_rounded").equalTo(weather_rounded.col("lng_rounded"))));

        Dataset<Row> expedia_hotels_joined = hotels
                .join(expedia, hotels.col("Id").equalTo(expedia.col("hotel_id")));


        WindowSpec window = Window.partitionBy("hotel_id").orderBy("hotel_id");

//        Dataset<Row> df = expedia
//                .withColumn("srch_ci_date", expedia_hotels_joined.col("srch_ci").cast("date"))
//                .withColumn("srch_co_date", expedia_hotels_joined.col("srch_co").cast("date"))
//                .orderBy("hotel_id", "srch_ci_date")
//                .withColumn("lag_day", functions.lag("srch_ci_date", 1)
//                        .over(window));

        Dataset<Row> df = expedia
//                .withColumn("srch_ci_date", expedia_hotels_joined.col("srch_ci").cast("date"))
//                .withColumn("srch_co_date", expedia_hotels_joined.col("srch_co").cast("date"))
                .orderBy("hotel_id", "srch_ci")
                .withColumn("lag_day", functions.lag("srch_ci", 1)
                        .over(window));

        Dataset<Row> df2 = df
                .withColumn("diff", functions.datediff(df.col("srch_ci"), df.col("lag_day")))
                .select("*").persist();

        df2.select("id", "hotel_id", "date_time", "srch_ci", "srch_co", "lag_day", "diff").show(20);

        Dataset<Row> incorrect_data = df2.select("id", "hotel_id", "srch_ci", "srch_co", "lag_day", "diff")
                .where(df2.col("diff").isNotNull()
                        .and(df2.col("diff").$greater(2)
                                .and(df2.col("diff").$less(30)))).persist();

       // incorrect_data.show();

        incorrect_data.join(hotels, incorrect_data.col("hotel_id").equalTo(hotels.col("id")))
                .select("Name", "country", "city", "address").distinct().show(20);

        Dataset<Row> correct_data = df2.select("id", "hotel_id", "srch_ci", "srch_co", "lag_day", "diff")
                .where(df2.col("diff").isNull()
                        .or(df2.col("diff").$less(2)
                                .or(df2.col("diff").$greater(30)))).persist();

        Dataset<Row> correct_group_by_country = correct_data.withColumnRenamed("id", "data_id")
                .join(hotels, correct_data.col("hotel_id").equalTo(hotels.col("id")))
                .groupBy("country").agg(functions.count("id").as("count"));

        correct_group_by_country.show();

        Dataset<Row> correct_group_by_city = correct_data.withColumnRenamed("id", "data_id")
                .join(hotels, correct_data.col("hotel_id").equalTo(hotels.col("id")))
                .groupBy("city").agg(functions.count("id").as("count"));

        correct_group_by_city.show();

        correct_data.withColumn("ci_year", correct_data.col("srch_ci").substr(0,4))
                .write().format("com.databricks.spark.avro").partitionBy("ci_year")
                .save("hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output");




        spark.stop();
    }
}
