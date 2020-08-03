import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

public class TaskUtil {

    static Dataset<Row> readCSV(SparkSession ss, String path) {

        return ss.read().option("header", "true").option("inferSchema", true)
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

    static Dataset<Row> window_data(Dataset<Row> dataset) {
        WindowSpec window = Window.partitionBy("hotel_id").orderBy("hotel_id");
        Dataset<Row> df = dataset
                .orderBy("hotel_id", "srch_ci")
                .withColumn("lag_day", functions.lag("srch_ci", 1)
                        .over(window));

        return df
                .withColumn("diff", functions.datediff(df.col("srch_ci"), df.col("lag_day")));


    }

    static Dataset<Row> invalid_data(Dataset<Row> dataset, Dataset<Row> hotels) {
        Dataset<Row> df = dataset.select("id", "hotel_id", "srch_ci", "srch_co", "lag_day", "diff")
                .where(dataset.col("diff").isNotNull()
                        .and(dataset.col("diff").$greater(2)
                                .and(dataset.col("diff").$less(30)))).persist();


        return df.join(hotels, df.col("hotel_id").equalTo(hotels.col("id")))
                .select("Name", "country", "city", "address").distinct();

    }

    static Dataset<Row> valid_data(Dataset<Row> dataset) {
        Dataset<Row> result =  dataset.select("id", "hotel_id", "srch_ci", "srch_co", "lag_day", "diff")
                .withColumn("value",
                        functions.array("id", "hotel_id", "srch_ci", "srch_co", "lag_day", "diff")
                                .cast(DataTypes.StringType))
                .where(dataset.col("diff").isNull()
                        .or(dataset.col("diff").$less(2)
                                .or(dataset.col("diff").$greater(30))));
        result.show();
        return result;

    }


    static Dataset<Row> group_by_column_and_join_hotels(Dataset<Row> dataset, Dataset<Row> hotels, String column) {
        return dataset.withColumnRenamed("id", "data_id")
                .join(hotels, dataset.col("hotel_id").equalTo(hotels.col("id")))
                .groupBy(column).agg(functions.count("id").as("count"));

    }

    static void write(Dataset<Row> dataset, String path) {

        dataset.withColumn("ci_year", dataset.col("srch_ci").substr(0, 4)).coalesce(5)
                .write().partitionBy("ci_year")
                .parquet(path);

//        dataset.filter(dataset.col("srch_ci").substr(0, 4).equalTo("2017"))
//                .withColumnRenamed("id", "value")
//                .write()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
//                .option("topic", "201_output")
//                .save();


    }


}
