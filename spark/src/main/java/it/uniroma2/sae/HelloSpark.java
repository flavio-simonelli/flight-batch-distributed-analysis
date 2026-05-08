package it.uniroma2.sae;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class HelloSpark {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("HelloSpark")
                .master("spark://spark-master:7077")
                .config("spark.hadoop.fs.defaultFS", "hdfs://master:54310")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Read data from HDFS. Parquet files are self-describing, so Spark can infer the schema.
        Dataset<Row> rawFlights = spark.read().parquet("hdfs://master:54310/data/conv/202501_T_ONTIME_REPORTING.parquet");

        // Adapt the raw data to the schema expected by the analysis logic
        Dataset<Row> flights = rawFlights
                .withColumn("flight_id", concat(col("OP_UNIQUE_CARRIER"), lit("_"), col("OP_CARRIER_FL_NUM")))
                .withColumn("origin", col("ORIGIN_AIRPORT_ID").cast(DataTypes.StringType))
                .withColumn("dest", col("DEST_AIRPORT_ID").cast(DataTypes.StringType))
                .withColumn("delay_min", col("ARR_DELAY").cast(DataTypes.IntegerType))
                .select("flight_id", "origin", "dest", "delay_min");


        System.out.println("=== Dataset completo ===");
        flights.show();

        System.out.println("=== Voli in ritardo (delay > 0) ===");
        flights.filter(col("delay_min").gt(0))
               .orderBy(col("delay_min").desc())
               .show();

        System.out.println("=== Ritardo medio per origine ===");
        flights.groupBy("origin")
               .agg(avg("delay_min").alias("avg_delay"))
               .orderBy(col("avg_delay").desc())
               .show();

        spark.stop();
    }
}
