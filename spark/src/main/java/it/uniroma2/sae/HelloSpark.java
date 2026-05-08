package it.uniroma2.sae;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class HelloSpark {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("HelloSpark")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("flight_id", DataTypes.StringType, false),
                DataTypes.createStructField("origin",    DataTypes.StringType, false),
                DataTypes.createStructField("dest",      DataTypes.StringType, false),
                DataTypes.createStructField("delay_min", DataTypes.IntegerType, false),
        });

        List<Row> rows = Arrays.asList(
                org.apache.spark.sql.RowFactory.create("AA001", "FCO", "JFK", 12),
                org.apache.spark.sql.RowFactory.create("AZ202", "MXP", "LAX", 0),
                org.apache.spark.sql.RowFactory.create("FR033", "CIA", "BCN", 45),
                org.apache.spark.sql.RowFactory.create("LH044", "FRA", "ORD", 3),
                org.apache.spark.sql.RowFactory.create("VY055", "BCN", "FCO", 120)
        );

        Dataset<Row> flights = spark.createDataFrame(rows, schema);

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
