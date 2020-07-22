import org.apache.spark.sql.SparkSession;

public class SparkConfig {
    static SparkSession getSession() {
        return SparkSession.builder()
               // .config("spark.jars", "/home/maria_dev/201_spark_batching/target/201_project_batching-1.0-SNAPSHOT.jar")
                .enableHiveSupport().getOrCreate();

    }
}
