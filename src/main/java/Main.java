import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("yarn").appName("Application").enableHiveSupport().getOrCreate();
        Dataset<Row> hotels = spark.read().option("header", "true")
                .csv("hdfs://sandbox-hdp.hortonworks.com:8020/201_hotels");
        Dataset<Row> weather = spark.read()
                .parquet("hdfs://sandbox-hdp.hortonworks.com:8020/201_weather/*");
        Dataset<Row> expedia = spark.read().format("com.databricks.spark.avro").option("header", "true")
                .load("hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/expedia");

//        long counth= hotels_weather_joined.count();
//        long counte= expedia.count();
        System.out.println(" hotels=");
        hotels.printSchema();
        System.out.println(" expedia=");
        expedia.printSchema();
        System.out.println(" weather=");
        weather.printSchema();

//        Dataset<Row> expedia_hotels_weather_joined = hotels_weather_joined
//                .join(expedia, hotels_weather_joined.col("id").equalTo(expedia.col("id")));
//
//        long countexp= expedia_hotels_weather_joined.count();
//        System.out.println("count joined=" +countexp);


        spark.stop();
    }
}
