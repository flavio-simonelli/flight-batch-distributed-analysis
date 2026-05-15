package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Implementation of Query 1: Monthly Performance Analyzer.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 * This query computes the average, minimum, and maximum departure delay, along with 
 * the cancellation rate for each airline and month.
 */
public class MonthlyPerformanceAnalyzer extends BaseQuery {

    @Override
    protected Dataset<Row> loadData(FlightRepository repository, ApplicationConfig config) {
        String datasetFilename = config.getInput().getDatasetFilename();
        // Return raw Dataset<Row> with only needed columns
        return repository.getFlightsOfAirlines(datasetFilename, "AA", "DL")
                .select("MONTH", "OP_UNIQUE_CARRIER", "DEP_DELAY", "CANCELLED");
    }

    private static final int MONTH_IDX = 0;
    private static final int OP_UNIQUE_CARRIER_IDX = 1;
    private static final int DEP_DELAY_IDX = 2;
    private static final int CANCELLED_IDX = 3;

    /**
     * Executes the query using the Spark RDD API.
     * Computes monthly statistics including average, min, max delays, and cancellation rates.
     *
     * @param dataset the dataset to query
     * @param config the application configuration containing input/output paths
     * @return a list containing a single RDD with the formatted results and its schema
     */
    @Override
    protected List<Tuple2<JavaRDD<Row>, StructType>> runQueryRDD(Dataset<Row> dataset, ApplicationConfig config) {

        // Load the dataset for specific airlines
        JavaRDD<Row> flights = dataset.javaRDD();

        // PHASE 1 & 2: Map-Side Aggregation (Combiner) and Reduction
        // Transforms each flight into a key-value pair where the key is (Airline, Month)
        // and uses aggregateByKey to aggregate statistics efficiently without creating
        // millions of temporary arrays.
        JavaPairRDD<Tuple2<String, Integer>, Row> pairs = flights.mapToPair(flight ->
                new Tuple2<>(new Tuple2<>(flight.getString(OP_UNIQUE_CARRIER_IDX), flight.getInt(MONTH_IDX)), flight)
        );

        // Zero value for the accumulator: [0: SumDelay, 1: MaxDelay, 2: MinDelay, 3: NotCancelledCount, 4: TotalCount]
        double[] zeroValue = {0.0, -Double.MAX_VALUE, Double.MAX_VALUE, 0.0, 0.0};

        JavaPairRDD<Tuple2<String, Integer>, double[]> reducedRDD = pairs.aggregateByKey(
                zeroValue,
                (acc, flight) -> {
                    // SEQ OP: Aggregation within a partition (The "Combiner")
                    boolean isCancelled = getDoubleSafe(flight, CANCELLED_IDX) > 0.0;
                    acc[4] += 1.0; // Increment TotalCount for every flight

                    if (!isCancelled) {
                        double delay = getDoubleSafe(flight, DEP_DELAY_IDX);
                        acc[0] += delay; // Sum delay
                        acc[1] = Math.max(acc[1], delay); // Max delay
                        acc[2] = Math.min(acc[2], delay); // Min delay
                        acc[3] += 1.0; // Increment NotCancelledCount
                    }
                    return acc;
                },
                (acc1, acc2) -> {
                    // COMB OP: Merging results between partitions
                    acc1[0] += acc2[0]; // Sum of delays
                    acc1[1] = Math.max(acc1[1], acc2[1]); // Maximum delay
                    acc1[2] = Math.min(acc1[2], acc2[2]); // Minimum delay
                    acc1[3] += acc2[3]; // Sum of not cancelled flights
                    acc1[4] += acc2[4]; // Sum of total flights
                    return acc1;
                }
        );

        // PHASE 3: Final Map operation
        // Converts the aggregated statistics into Spark SQL Rows, computing the final averages and rates.
        JavaRDD<Row> rowRDD = reducedRDD.map(tuple -> {
            Tuple2<String, Integer> key = tuple._1;
            double[] stats = tuple._2;

            String carrier = key._1;
            Integer month = key._2;

            double totalFlights = stats[4];
            double notCancelledFlights = stats[3];
            double cancelledFlights = totalFlights - notCancelledFlights;

            // Round calculations to 2 decimal places
            double avgDelay = Math.round(((notCancelledFlights > 0) ? (stats[0] / notCancelledFlights) : 0.0) * 100.0) / 100.0;
            double maxDelay = Math.round(((notCancelledFlights > 0) ? stats[1] : 0.0) * 100.0) / 100.0;
            double minDelay = Math.round(((notCancelledFlights > 0) ? stats[2] : 0.0) * 100.0) / 100.0;
            double cancellationRate = Math.round(((totalFlights > 0) ? (cancelledFlights / totalFlights) * 100 : 0.0) * 100.0) / 100.0;

            return org.apache.spark.sql.RowFactory.create(month, carrier, avgDelay, minDelay, maxDelay, cancellationRate);
        });

        // Sort the RDD by month and then by airline
        JavaRDD<Row> sortedRDD = rowRDD.sortBy(row -> (row.getInt(0) < 10 ? "0" : "") + row.getInt(0) + "-" + row.getString(1), true, rowRDD.getNumPartitions());
        
        // Define the schema for the RDD
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("month", DataTypes.IntegerType, false),
                DataTypes.createStructField("carrier", DataTypes.StringType, false),
                DataTypes.createStructField("avg_delay", DataTypes.DoubleType, false),
                DataTypes.createStructField("min_delay", DataTypes.DoubleType, false),
                DataTypes.createStructField("max_delay", DataTypes.DoubleType, false),
                DataTypes.createStructField("cancellation_rate", DataTypes.DoubleType, false)
        });

        return Collections.singletonList(new Tuple2<>(sortedRDD, schema));
    }

    /**
     * Executes the query using the Spark DataFrame API.
     * Computes monthly statistics including average, min, max delays, and cancellation rates.
     *
     * @param flights the dataset to query
     * @param config the application configuration containing input/output paths
     * @return a list containing a single Dataset with the query results
     */
    @Override
    protected List<Dataset<Row>> runQueryDataFrame(Dataset<Row> flights, ApplicationConfig config) {

        // Use raw column names from Parquet to avoid any unnecessary mapping
        Dataset<Row> result = flights
                .groupBy("MONTH", "OP_UNIQUE_CARRIER")
                .agg(
                        round(avg(when(col("CANCELLED").equalTo(0.0), col("DEP_DELAY"))), 2).as("dep-delay-mean"),
                        round(min(when(col("CANCELLED").equalTo(0.0), col("DEP_DELAY"))), 2).as("dep-delay-min"),
                        round(max(when(col("CANCELLED").equalTo(0.0), col("DEP_DELAY"))), 2).as("dep-delay-max"),
                        round(sum(col("CANCELLED")).divide(count("*")).multiply(100), 2).as("cancellation-rate")
                )
                .withColumnRenamed("MONTH", "month")
                .withColumnRenamed("OP_UNIQUE_CARRIER", "carrier")
                .orderBy("month", "carrier");

        return Collections.singletonList(result);
    }

    /**
     * Executes the query using the Spark SQL API.
     * Computes monthly statistics including average, min, max delays, and cancellation rates.
     *
     * @param flights the dataset to query
     * @param config the application configuration containing input/output paths
     * @param spark the active SparkSession to run SQL commands
     * @return a list containing a single Dataset with the query results
     */
    @Override
    protected List<Dataset<Row>> runQuerySQL(Dataset<Row> flights, ApplicationConfig config, SparkSession spark) {
        // Create a temporary view to run SQL queries against it
        flights.createOrReplaceTempView("flights");

        String sqlQuery = "SELECT MONTH as month, OP_UNIQUE_CARRIER as carrier, " +
                "ROUND(AVG(CASE WHEN CANCELLED = 0.0 THEN DEP_DELAY END), 2) AS `dep-delay-mean`, " +
                "ROUND(MIN(CASE WHEN CANCELLED = 0.0 THEN DEP_DELAY END), 2) AS `dep-delay-min`, " +
                "ROUND(MAX(CASE WHEN CANCELLED = 0.0 THEN DEP_DELAY END), 2) AS `dep-delay-max`, " +
                "ROUND((SUM(CANCELLED) / COUNT(*)) * 100, 2) AS `cancellation-rate` " +
                "FROM flights " +
                "GROUP BY MONTH, OP_UNIQUE_CARRIER " +
                "ORDER BY month, carrier";

        Dataset<Row> result = spark.sql(sqlQuery);
        return Collections.singletonList(result);
    }
}
