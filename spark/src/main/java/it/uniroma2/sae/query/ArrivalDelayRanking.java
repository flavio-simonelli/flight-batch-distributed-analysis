package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Implementation of Query 2: Arrival Delay Ranking.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 * This query ranks airlines based on their average arrival delay, considering only
 * those with more than 500 valid flights, and provides a breakdown of various delay causes.
 */
public class ArrivalDelayRanking extends BaseQuery {

    @Override
    protected Dataset<Row> loadData(FlightRepository repository, ApplicationConfig config) {
        String datasetFilename = config.getInput().getDatasetFilename();
        // Return raw Dataset<Row> with only needed columns
        return repository.getFlights(datasetFilename)
                .select("OP_UNIQUE_CARRIER", "ARR_DELAY", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "CANCELLED", "DIVERTED");
    }

    private static final int OP_UNIQUE_CARRIER_IDX = 0;
    private static final int ARR_DELAY_IDX = 1;
    private static final int CARRIER_DELAY_IDX = 2;
    private static final int WEATHER_DELAY_IDX = 3;
    private static final int NAS_DELAY_IDX = 4;
    private static final int SECURITY_DELAY_IDX = 5;
    private static final int LATE_AIRCRAFT_DELAY_IDX = 6;
    private static final int CANCELLED_IDX = 7;
    private static final int DIVERTED_IDX = 8;

    /**
     * Executes the query using the Spark RDD API.
     * Maps and reduces flight data to compute average delays and filters top offenders.
     *
     * @param dataset the dataset to query
     * @param config the application configuration containing input/output paths
     * @return a list containing a single RDD with the formatted results and its schema
     */
    @Override
    protected List<Tuple2<JavaRDD<Row>, StructType>> runQueryRDD(Dataset<Row> dataset, ApplicationConfig config) {

        // Load the dataset for specific airlines
        JavaRDD<Row> flights = dataset.javaRDD();

        // Discard flights that were either cancelled or diverted
        JavaRDD<Row> validFlights = flights.filter(flight -> {
            boolean isCancelled = getDoubleSafe(flight, CANCELLED_IDX) != 0.0;
            boolean isDiverted = getDoubleSafe(flight, DIVERTED_IDX) != 0.0;
            return !isCancelled && !isDiverted;
        });

        final int totalCountIdx = 0;
        final int avgArrDelayIdx = 1;
        final int avgCarrierIdx = 2;
        final int avgWeatherIdx = 3;
        final int avgNasIdx = 4;
        final int avgSecurityIdx = 5;
        final int avgLateAircraftIdx = 6;

        // PHASE 1 & 2: Map-Side Aggregation (with Combiner) and Reduction
        // Using aggregateByKey to avoid creating millions of temporary arrays.
        // It performs an initial aggregation locally in each partition (Combiner phase)
        // and then merges results across the cluster (Reduce phase).
        JavaPairRDD<String, double[]> reducedRDD = validFlights
                .mapToPair(flight -> new Tuple2<>(flight.getString(OP_UNIQUE_CARRIER_IDX), flight))
                .aggregateByKey(
                        new double[7], // [0: Count, 1: Arrive, 2: Carrier, 3: Weather, 4: NAS, 5: Security, 6: LateAircraft]
                        (acc, flight) -> {
                            // SEQ OP: Aggregation within a partition
                            acc[totalCountIdx]      += 1.0;                                             // Total count of valid flights
                            acc[avgArrDelayIdx]     += getDoubleSafe(flight, ARR_DELAY_IDX);            // Sum of arrival delays
                            acc[avgCarrierIdx]      += getDoubleSafe(flight, CARRIER_DELAY_IDX);        // Sum of carrier delays
                            acc[avgWeatherIdx]      += getDoubleSafe(flight, WEATHER_DELAY_IDX);        // Sum of weather delays
                            acc[avgNasIdx]          += getDoubleSafe(flight, NAS_DELAY_IDX);            // Sum of NAS delays
                            acc[avgSecurityIdx]     += getDoubleSafe(flight, SECURITY_DELAY_IDX);       // Sum of security delays
                            acc[avgLateAircraftIdx] += getDoubleSafe(flight, LATE_AIRCRAFT_DELAY_IDX);  // Sum of late aircraft delays
                            return acc;
                        },
                        (acc1, acc2) -> {
                            // COMB OP: Merging results between partitions
                            for (int i = 0; i < 7; i++) {
                                acc1[i] += acc2[i];
                            }
                            return acc1;
                        }
                );

        // PHASE 3: Filter and Map to Rows
        // Retains airlines with at least 500 flights and computes the averages.
        JavaRDD<Row> processedRDD = reducedRDD
                .filter(tuple -> tuple._2[totalCountIdx] >= 500.0) // Filter airlines with >= 500 valid flights
                .map(tuple -> {
                    String carrier = tuple._1;
                    double[] stats = tuple._2;

                    double count            = stats[totalCountIdx];                                     // Total count of valid flights
                    double avgArrDelay      = safeDivideRounded(stats[avgArrDelayIdx], count);          // Average arrival delay
                    double avgCarrier       = safeDivideRounded(stats[avgCarrierIdx], count);           // Average carrier delay
                    double avgWeather       = safeDivideRounded(stats[avgWeatherIdx], count);           // Average weather delay
                    double avgNas           = safeDivideRounded(stats[avgNasIdx], count);               // Average NAS delay
                    double avgSecurity      = safeDivideRounded(stats[avgSecurityIdx], count);          // Average security delay
                    double avgLateAircraft  = safeDivideRounded(stats[avgLateAircraftIdx], count);      // Average late aircraft delay

                    return RowFactory.create(
                            carrier,
                            (long) count,
                            avgArrDelay,
                            avgCarrier,
                            avgWeather,
                            avgNas,
                            avgSecurity,
                            avgLateAircraft
                    );
                });

        // Diagnostic print
        // SparkDiagnostics.profilePartitions(processedRDD, "Airlines after 500 filter");
        // SparkDiagnostics.checkSkew(processedRDD, "Airlines", 3.0);

        // Sort the result by average arrival delay in descending order
        //
        // We use a simple global sortBy followed by a filter on index because we know that
        // the number of airlines remaining after the >= 500 flights filter is very small (approx. 14).
        // In this specific case, implementing a complex distributed top-N (local sorts followed by
        // global sort) would add unnecessary complexity without providing measurable performance gains,
        // as the shuffle volume for 14 records is negligible.
        JavaRDD<Row> sortedRDD = processedRDD.sortBy(r -> r.getDouble(avgArrDelayIdx + 1), false, processedRDD.getNumPartitions());
        JavaRDD<Row> top10RDD = sortedRDD.zipWithIndex()
                .filter(tuple -> tuple._2 < 10)
                .map(tuple -> tuple._1)
                .coalesce(1);

        // Define the schema for the RDD
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("carrier", DataTypes.StringType, false),
                DataTypes.createStructField("num_flights", DataTypes.LongType, false),
                DataTypes.createStructField("arrdelay_mean", DataTypes.DoubleType, false),
                DataTypes.createStructField("carrier_delay_mean", DataTypes.DoubleType, false),
                DataTypes.createStructField("weather_delay_mean", DataTypes.DoubleType, false),
                DataTypes.createStructField("nas_delay_mean", DataTypes.DoubleType, false),
                DataTypes.createStructField("security_delay_mean", DataTypes.DoubleType, false),
                DataTypes.createStructField("late_aircraft_delay_mean", DataTypes.DoubleType, false)
        });

        return Collections.singletonList(new Tuple2<>(top10RDD, schema));
    }
    
    /**
     * Executes the query using the Spark DataFrame API.
     * Groups data by airline and aggregates the various types of delays.
     *
     * @param flights the dataset to query
     * @param config the application configuration containing input/output paths
     * @return a list containing a single Dataset with the top 10 airlines ranked by arrival delay
     */
    @Override
    protected List<Dataset<Row>> runQueryDataFrame(Dataset<Row> flights, ApplicationConfig config) {

        Dataset<Row> result = flights
                .filter(col("CANCELLED").equalTo(0).and(col("DIVERTED").equalTo(0)))
                .groupBy("OP_UNIQUE_CARRIER")
                .agg(
                    count("*").as("num_flights"),
                    round(avg(coalesce(col("ARR_DELAY"), lit(0))), 2).as("arrdelay_mean"),
                    round(avg(coalesce(col("CARRIER_DELAY"), lit(0))), 2).as("carrier_delay_mean"),
                    round(avg(coalesce(col("WEATHER_DELAY"), lit(0))), 2).as("weather_delay_mean"),
                    round(avg(coalesce(col("NAS_DELAY"), lit(0))), 2).as("nas_delay_mean"),
                    round(avg(coalesce(col("SECURITY_DELAY"), lit(0))), 2).as("security_delay_mean"),
                    round(avg(coalesce(col("LATE_AIRCRAFT_DELAY"), lit(0))), 2).as("late_aircraft_delay_mean")
                )
                .withColumnRenamed("OP_UNIQUE_CARRIER", "carrier")
                .filter(col("num_flights").geq(500))
                .orderBy(col("arrdelay_mean").desc())
                .limit(10);

        return Collections.singletonList(result);
    }

    /**
     * Executes the query using the Spark SQL API.
     * Groups data by airline and aggregates the various types of delays.
     *
     * @param flights the dataset to query
     * @param config the application configuration containing input/output paths
     * @param spark the active SparkSession to run SQL commands
     * @return a list containing a single Dataset with the top 10 airlines ranked by arrival delay
     */
    @Override
    protected List<Dataset<Row>> runQuerySQL(Dataset<Row> flights, ApplicationConfig config, SparkSession spark) {

        flights.createOrReplaceTempView("flights");

        String sqlQuery =   "SELECT OP_UNIQUE_CARRIER as carrier, " +
                            "COUNT(*) AS num_flights, " +
                            "ROUND(AVG(COALESCE(ARR_DELAY, 0)), 2) AS arrdelay_mean, " +
                            "ROUND(AVG(COALESCE(CARRIER_DELAY, 0)), 2) AS carrier_delay_mean, " +
                            "ROUND(AVG(COALESCE(WEATHER_DELAY, 0)), 2) AS weather_delay_mean, " +
                            "ROUND(AVG(COALESCE(NAS_DELAY, 0)), 2) AS nas_delay_mean, " +
                            "ROUND(AVG(COALESCE(SECURITY_DELAY, 0)), 2) AS security_delay_mean, " +
                            "ROUND(AVG(COALESCE(LATE_AIRCRAFT_DELAY, 0)), 2) AS late_aircraft_delay_mean " +
                            "FROM flights " +
                            "WHERE CANCELLED = 0 AND DIVERTED = 0 " +
                            "GROUP BY OP_UNIQUE_CARRIER " +
                            "HAVING num_flights >= 500 " +
                            "ORDER BY arrdelay_mean DESC " +
                            "LIMIT 10";

        Dataset<Row> result = spark.sql(sqlQuery);
        return Collections.singletonList(result);
    }

}
