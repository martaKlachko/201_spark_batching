import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("yarn").appName("Application").enableHiveSupport().getOrCreate();
        Dataset<Row> ds = spark.read()
                .parquet("hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/hotels_weather_joined");

        int count= ds.count()
                System.out.println("count=" +count);

        spark.stop();
    }
}
