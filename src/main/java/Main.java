import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkConfig.getSession();
        String path = "hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output";

        Dataset<Row> hotels = TaskUtil.readCSV(spark, "hdfs://sandbox-hdp.hortonworks.com:8020/hotels");
        Dataset<Row> expedia = TaskUtil.readAvro(spark, "hdfs://sandbox-hdp.hortonworks.com:8020/expedia");


        Dataset<Row> expedia_windowed_data = TaskUtil.window_data(expedia).persist();
        Dataset<Row> invalid_expedia_data = TaskUtil.invalid_data(expedia_windowed_data, hotels);
        Dataset<Row> valid_expedia_data = TaskUtil.valid_data(expedia_windowed_data);

        invalid_expedia_data.show();

        Dataset<Row> grouped_by_country = TaskUtil.group_by_column_and_join_hotels(valid_expedia_data, hotels, "country");
        Dataset<Row> grouped_by_city = TaskUtil.group_by_column_and_join_hotels(valid_expedia_data, hotels, "city");

        System.out.println("Valid data, grouped by country");
        grouped_by_country.show();

        System.out.println("Valid data, grouped by city");
        grouped_by_city.show();


        TaskUtil.write(valid_expedia_data,path);


        spark.stop();
}
}
