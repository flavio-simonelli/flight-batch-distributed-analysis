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
        // Return raw Dataset<Row> to avoid early Bean Encoder overhead
        return repository.getFlightsOfAirlines(datasetFilename, "AA", "DL");
    }

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
        
        // Convert to RawFlight only when needed for RDD
        Dataset<RawFlight> flightDataset = dataset.select(
                col("YEAR").as("year"),
                col("MONTH").as("month"),
                col("DAY_OF_MONTH").as("dayOfMonth"),
                col("OP_UNIQUE_CARRIER").as("opUniqueCarrier"),
                col("OP_CARRIER_FL_NUM").as("opCarrierFlNum"),
                col("ORIGIN_AIRPORT_ID").as("originAirportId"),
                col("ORIGIN_CITY_MARKET_ID").as("originCityMarketId"),
                col("ORIGIN_STATE_ABR").as("originStateAbr"),
                col("DEST_AIRPORT_ID").as("destAirportId"),
                col("DEST_CITY_MARKET_ID").as("destCityMarketId"),
                col("DEST_STATE_ABR").as("destStateAbr"),
                col("CRS_DEP_TIME").as("crsDepTime"),
                col("DEP_TIME").as("depTime"),
                col("DEP_DELAY").as("depDelay"),
                col("CRS_ARR_TIME").as("crsArrTime"),
                col("ARR_TIME").as("arrTime"),
                col("ARR_DELAY").as("arrDelay"),
                col("CANCELLED").cast(DataTypes.DoubleType).as("cancelled"),
                col("CANCELLATION_CODE").as("cancellationCode"),
                col("DIVERTED").cast(DataTypes.DoubleType).as("diverted"),
                col("ACTUAL_ELAPSED_TIME").as("actualElapsedTime"),
                col("DISTANCE").as("distance"),
                col("CARRIER_DELAY").as("carrierDelay"),
                col("WEATHER_DELAY").as("weatherDelay"),
                col("NAS_DELAY").as("nasDelay"),
                col("SECURITY_DELAY").as("securityDelay"),
                col("LATE_AIRCRAFT_DELAY").as("lateAircraftDelay")
        ).as(Encoders.bean(RawFlight.class));

        // Load the dataset for specific airlines
        JavaRDD<RawFlight> flights = flightDataset.javaRDD();

        // PHASE 1: Map operation
        // Transforms each flight into a key-value pair where the key is (Airline, Month)
        // and the value is an array of statistics needed for aggregation.
        JavaPairRDD<Tuple2<String, Integer>, double[]> mappedRDD = flights.mapToPair(flight -> {
            // Key: (Airline, Month)
            Tuple2<String, Integer> key = new Tuple2<>(flight.getOpUniqueCarrier(), flight.getMonth());
            
            // Values array holds statistics:
            // [0: SumDelay, 1: MaxDelay, 2: MinDelay, 3: NotCancelledCount, 4: TotalCount]
            double[] values = new double[5];
            boolean isCancelled = (flight.getCancelled() != null && flight.getCancelled() > 0.0);
            
            values[4] = 1.0; // Increment TotalCount for every flight
            
            if (isCancelled) {
                values[0] = 0.0; // SumDelay is 0 if the flight was cancelled
                values[1] = -Double.MAX_VALUE; // Neutral element for MAX calculation
                values[2] = Double.MAX_VALUE;  // Neutral element for MIN calculation
                values[3] = 0.0; // NotCancelledCount is 0
            } else {
                double delay = (flight.getDepDelay() != null) ? flight.getDepDelay() : 0.0;
                values[0] = delay;
                values[1] = delay;
                values[2] = delay;
                values[3] = 1.0; // Increment NotCancelledCount
            }

            return new Tuple2<>(key, values);
        });

        // PHASE 2: Reduce operation
        // Aggregates the statistics for each (Airline, Month) combination by summing up counts and delays,
        // and finding the absolute min and max delays.
        JavaPairRDD<Tuple2<String, Integer>, double[]> reducedRDD = mappedRDD.reduceByKey((a, b) -> {
            double[] res = new double[5];
            res[0] = a[0] + b[0]; // Sum of delays
            res[1] = Math.max(a[1], b[1]); // Maximum delay
            res[2] = Math.min(a[2], b[2]); // Minimum delay
            res[3] = a[3] + b[3]; // Sum of not cancelled flights
            res[4] = a[4] + b[4]; // Sum of total flights

            return res;
        });

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
        JavaRDD<Row> sortedRDD = rowRDD.sortBy(row -> String.format("%02d-%s", row.getInt(0), row.getString(1)), true, rowRDD.getNumPartitions());
        
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
