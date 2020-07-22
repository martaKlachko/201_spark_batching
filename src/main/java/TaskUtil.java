import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

public class TaskUtil {

    static Dataset<Row> readCSV(SparkSession ss, String path) {

        return ss.read().option("header", "true")
                .csv(path);
    }

    static Dataset<Row> readParquet(SparkSession ss, String path) {
        return ss.read()
                .parquet(path);

    }

    static Dataset<Row> readAvro(SparkSession ss, String path) {
        return ss.read()
                .format("com.databricks.spark.avro")
                .load(path);


    }

    static Dataset<Row> expedia_windoved_data(Dataset<Row> expedia) {
        WindowSpec window = Window.partitionBy("hotel_id").orderBy("hotel_id");
        Dataset<Row> df = expedia
                .orderBy("hotel_id", "srch_ci")
                .withColumn("lag_day", functions.lag("srch_ci", 1)
                        .over(window));

        Dataset<Row> result = df
                .withColumn("diff", functions.datediff(df.col("srch_ci"), df.col("lag_day")));


        return result;

    }

    static Dataset<Row> invalid_expedia_data(Dataset<Row> expedia, Dataset<Row> hotels) {
        Dataset<Row> df = expedia.select("id", "hotel_id", "srch_ci", "srch_co", "lag_day", "diff")
                .where(expedia.col("diff").isNotNull()
                        .and(expedia.col("diff").$greater(2)
                                .and(expedia.col("diff").$less(30)))).persist();


        return df.join(hotels, df.col("hotel_id").equalTo(hotels.col("id")))
                .select("Name", "country", "city", "address").distinct();
        
    }

    static Dataset<Row> valid_expedia_data(Dataset<Row> expedia, Dataset<Row> hotels) {
        return expedia.select("id", "hotel_id", "srch_ci", "srch_co", "lag_day", "diff")
                .where(expedia.col("diff").isNull()
                        .or(expedia.col("diff").$less(2)
                                .or(expedia.col("diff").$greater(30))));

    }


    static Dataset<Row> group_by_column(Dataset<Row> dataset, Dataset<Row> hotels, String column) {
        return  dataset.withColumnRenamed("id", "data_id")
                .join(hotels, dataset.col("hotel_id").equalTo(hotels.col("id")))
                .groupBy(column).agg(functions.count("id").as("count"));

    }

    static void write_to_hdfs(Dataset<Row> dataset) {

        dataset.withColumn("ci_year", dataset.col("srch_ci").substr(0, 4)).coalesce(5)
                .write().partitionBy("ci_year")
                .parquet("hdfs://sandbox-hdp.hortonworks.com:8020/201_expedia_output");

    }


}
