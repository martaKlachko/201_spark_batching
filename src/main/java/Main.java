import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("yarn").appName("Application").enableHiveSupport().getOrCreate();
        Dataset<Row> hotels_weather_joined = spark.read().option("header", "false")
                .csv("hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/hotels_weather_joined");

        Dataset<Row> expedia = spark.read().option("header", "false")
                .csv("hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/expedia");
        long counth= hotels_weather_joined.count();
        long counte= expedia.count();
                System.out.println("count hotels_weather_joined=" +counth);
                System.out.println("count expedia=" +counte);

        spark.stop();
    }
}
