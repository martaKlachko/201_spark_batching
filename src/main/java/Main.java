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
        Dataset<Row> hotels_rounded = hotels.withColumn("Latitude_rounded", functions.round(hotels.col("Latitude"), 2))
                .withColumn("Longitude_rounded", functions.round(hotels.col("Longitude"), 2));

        Dataset<Row> weather = spark.read()
                .parquet("hdfs://sandbox-hdp.hortonworks.com:8020/201_weather/*");
        Dataset<Row> weather_rounded = weather.withColumn("lat_rounded", functions.round(weather.col("lat"), 2))
                .withColumn("lng_rounded", functions.round(weather.col("lng"), 2));

        Dataset<Row> expedia = spark.read()
                .format("com.databricks.spark.avro")
                .load("hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia/*");

//      Dataset<Row> hotels_weather_joined = hotels_rounded
//                .join(weather_rounded, hotels_rounded.col("Latitude_rounded").equalTo(weather_rounded.col("lat_rounded"))
//                .and(hotels_rounded.col("Longitude_rounded").equalTo(weather_rounded.col("lng_rounded"))));

        Dataset<Row> expedia_hotels_joined = hotels_rounded
                .join(expedia, hotels_rounded.col("Id").equalTo(expedia.col("hotel_id")));




        WindowSpec window  = Window.partitionBy("hotel_id").orderBy("hotel_id");

        Dataset<Row> df = expedia
                .withColumn("srch_ci_date", expedia_hotels_joined.col("srch_ci").cast("date"))
                .withColumn("srch_co_date", expedia_hotels_joined.col("srch_co").cast("date"))
                .orderBy("hotel_id", "srch_ci_date")
                .withColumn("lag_day", functions.lag("srch_ci_date", 1)
                        .over(window));

        Dataset<Row> df2 = df
                .withColumn("diff", functions.datediff(df.col("lag_day"),df.col("srch_ci_date")))
                .select("id", "hotel_id", "srch_ci_date", "srch_co_date", "lag_day");

                df2.show(20);


        spark.stop();
    }
}
