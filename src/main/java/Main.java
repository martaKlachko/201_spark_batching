import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import javax.xml.crypto.Data;


public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkConfig.getSession();

        Dataset<Row> hotels = TaskUtil.readCSV(spark, "hdfs://sandbox-hdp.hortonworks.com:8020/201_hotels");

        Dataset<Row> weather = TaskUtil.readParquet(spark, "hdfs://sandbox-hdp.hortonworks.com:8020/201_weather");

        Dataset<Row> expedia = TaskUtil.readAvro(spark, "hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia");


        Dataset<Row> expedia_windoved_data = TaskUtil.expedia_windoved_data(expedia).persist();
        Dataset<Row> invalid_expedia_data = TaskUtil.invalid_expedia_data(expedia_windoved_data, hotels);
        Dataset<Row> valid_expedia_data = TaskUtil.valid_expedia_data(expedia_windoved_data, hotels);

        invalid_expedia_data.show();

        Dataset<Row> grouped_by_country = TaskUtil.group_by_column(valid_expedia_data, hotels, "country");
        Dataset<Row> grouped_by_city = TaskUtil.group_by_column(valid_expedia_data, hotels, "city");

        System.out.println("Valid data, grouped by country");
        grouped_by_country.show();

        System.out.println("Valid data, grouped by city");
        grouped_by_city.show();


        TaskUtil.write_to_hdfs(valid_expedia_data);


//        WindowSpec window = Window.partitionBy("hotel_id").orderBy("hotel_id");
//
//
//        Dataset<Row> df = expedia
//                .orderBy("hotel_id", "srch_ci")
//                .withColumn("lag_day", functions.lag("srch_ci", 1)
//                        .over(window));
//
//        Dataset<Row> df2 = df
//                .withColumn("diff", functions.datediff(df.col("srch_ci"), df.col("lag_day")))
//                .select("*").persist();
//
//
//        Dataset<Row> incorrect_data = df2.select("id", "hotel_id", "srch_ci", "srch_co", "lag_day", "diff")
//                .where(df2.col("diff").isNotNull()
//                        .and(df2.col("diff").$greater(2)
//                                .and(df2.col("diff").$less(30)))).persist();
//
//
//        incorrect_data.join(hotels, incorrect_data.col("hotel_id").equalTo(hotels.col("id")))
//                .select("Name", "country", "city", "address").distinct().show(20);

//        Dataset<Row> correct_data = df2.select("id", "hotel_id", "srch_ci", "srch_co", "lag_day", "diff")
//                .where(df2.col("diff").isNull()
//                        .or(df2.col("diff").$less(2)
//                                .or(df2.col("diff").$greater(30)))).persist();
//
//        Dataset<Row> correct_group_by_country = correct_data.withColumnRenamed("id", "data_id")
//                .join(hotels, correct_data.col("hotel_id").equalTo(hotels.col("id")))
//                .groupBy("country").agg(functions.count("id").as("count"));

      //  correct_group_by_country.show();

//        Dataset<Row> correct_group_by_city = correct_data.withColumnRenamed("id", "data_id")
//                .join(hotels, correct_data.col("hotel_id").equalTo(hotels.col("id")))
//                .groupBy("city").agg(functions.count("id").as("count"));

       // correct_group_by_city.show();

//        correct_data.withColumn("ci_year", correct_data.col("srch_ci").substr(0, 4)).coalesce(5)
//                .write().partitionBy("ci_year")
//                .parquet("hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output");


        spark.stop();
    }
}
