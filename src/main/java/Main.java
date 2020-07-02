import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;


public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("yarn").appName("Application").enableHiveSupport().getOrCreate();
        Dataset<Row> hotels = spark.read().option("header", "true")
                .csv("hdfs://sandbox-hdp.hortonworks.com:8020/201_hotels");

        Dataset<Row>   hotels_rounded= hotels.withColumn("Latitude_rounded", functions.round(hotels.col("Latitude"), 3))
                .withColumn("Longitude_rounded", functions.round(hotels.col("Longitude"), 3));
        Dataset<Row> weather = spark.read()
                .parquet("hdfs://sandbox-hdp.hortonworks.com:8020/201_weather/*");
        Dataset<Row>   weather_rounded= weather.withColumn("lat_rounded", functions.round(weather.col("lat"), 3))
                .withColumn("lng_rounded", functions.round(weather.col("lng"), 3));
        Dataset<Row> expedia = spark.read().format("com.databricks.spark.avro").option("header", "true")
                .load("hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/expedia");

//        long counth= hotels_weather_joined.count();
//        long counte= expedia.count();
        System.out.println(" hotels=");
        hotels_rounded.printSchema();
        System.out.println(" expedia=");
        expedia.printSchema();
        System.out.println(" weather=");
        weather_rounded.printSchema();

       long  hotels_weather_joined = hotels_rounded
              .join(weather_rounded, hotels_rounded.col("Latitude_rounded").equalTo(weather_rounded.col("lat_rounded"))
                      .and(hotels_rounded.col("Longitude_rounded").equalTo(weather_rounded.col("lng_rounded")))).count();

      System.out.println("count joined=" +hotels_weather_joined);


        spark.stop();
    }
}
