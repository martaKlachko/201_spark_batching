import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

public class Test {
    private static Dataset<Row> df;
    private static SparkSession spark;

    @BeforeClass
    public static void beforeClass() {
        spark = SparkSession.builder().master("local[*]").config(new SparkConf().set("fs.defaultFS", "file:///"))
                .getOrCreate();

    }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }

    @org.junit.Test
    public void readCSV() {
        Dataset<Row> df = TaskUtil.readCSV(spark, "src/test/resources/1.csv");
        Assert.assertNotNull(df);
    }

    @org.junit.Test
    public void readParquet() {
        Dataset<Row> df = TaskUtil.readParquet(spark, "src/test/resources/2.snappy.parquet");
        Assert.assertNotNull(df);
    }

    @org.junit.Test
    public void readAvro() {
        Dataset<Row> df = TaskUtil.readAvro(spark, "src/test/resources/3.avro");
        Assert.assertNotNull(df);
    }

    @org.junit.Test
    public void testWindowData() {

        Dataset<Row> df = TaskUtil.readAvro(spark, "src/test/resources/3.avro");
        Row[] actual = TaskUtil.window_data(df).collect();

        WindowSpec window = Window.partitionBy("hotel_id").orderBy("hotel_id");
        Dataset<Row> df2 = df
                .orderBy("hotel_id", "srch_ci")
                .withColumn("lag_day", functions.lag("srch_ci", 1)
                        .over(window));
        Row[] expected = df2
                .withColumn("diff", functions.datediff(df.col("srch_ci"), df.col("lag_day"))).collect();


        Assert.assertEquals(actual, expected);
    }


    @org.junit.Test
    public void testInvalidData() {

        Dataset<Row> df = TaskUtil.readAvro(spark, "src/test/resources/3.avro");
        Dataset<Row> df1 = TaskUtil.readCSV(spark, "src/test/resources/1.csv");

        Row[] actual = TaskUtil.invalid_data(df, df1).collect();

        Dataset<Row> df2 = df.select("id", "hotel_id", "srch_ci", "srch_co", "lag_day", "diff")
                .where(df.col("diff").isNotNull()
                        .and(df.col("diff").$greater(2)
                                .and(df.col("diff").$less(30)))).persist();


        Row[] expected =  df.join(df1, df.col("hotel_id").equalTo(df2.col("id")))
                .select("Name", "country", "city", "address").distinct().collect();

        Assert.assertEquals(actual, expected);
    }

    @org.junit.Test
    public void testValidData() {

        Dataset<Row> df = TaskUtil.readAvro(spark, "src/test/resources/3.avro");

        Row[] actual = TaskUtil.valid_data(df).collect();


        Row[] expected =  df.select("id", "hotel_id", "srch_ci", "srch_co", "lag_day", "diff")
                .where(df.col("diff").isNull()
                        .or(df.col("diff").$less(2)
                                .or(df.col("diff").$greater(30)))).collect();

        Assert.assertEquals(actual, expected);
    }


    @org.junit.Test
    public void testGroup_by_column_and_join_hotels() {

        Dataset<Row> df = TaskUtil.readAvro(spark, "src/test/resources/3.avro");
        Dataset<Row> df1 = TaskUtil.readCSV(spark, "src/test/resources/1.csv");

        Row[] actual = TaskUtil.group_by_column_and_join_hotels(df, df1, "country").collect();


        Row[] expected =  df.withColumnRenamed("id", "data_id")
                .join(df1, df.col("hotel_id").equalTo(df1.col("id")))
                .groupBy("country").agg(functions.count("id").as("count")).collect();

        Assert.assertEquals(actual, expected);
    }



}
