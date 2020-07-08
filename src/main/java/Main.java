import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

//
//        System.out.println(" hotels=");
//        hotels_rounded.limit(10).show();
//        System.out.println(" weather=");
//        weather_rounded.limit(10).show();

        Dataset<Row> hotels_weather_joined = hotels_rounded
                .join(weather_rounded, hotels_rounded.col("Latitude_rounded").equalTo(weather_rounded.col("lat_rounded"))
                        .and(hotels_rounded.col("Longitude_rounded").equalTo(weather_rounded.col("lng_rounded"))));

//       hotels_weather_joined.limit(10).show();
        //  expedia.limit(10).show();

        Dataset<Row> expedia_hotels_weather_joined = hotels_weather_joined
                .join(expedia, hotels_weather_joined.col("Id").equalTo(expedia.col("id")));

        System.out.println("count joined=");
        expedia_hotels_weather_joined.show(10);

        spark.stop();
    }
}
