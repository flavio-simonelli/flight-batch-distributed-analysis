package it.uniroma2.sae.query;

import it.uniroma2.sae.config.AppBackendType;
import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.PercentileAlgorithm;
import it.uniroma2.sae.repository.FlightRepository;
import it.uniroma2.sae.util.percentile.PercentileSketch;
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

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Implementation of Query 3: Hourly Delay Percentiles.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 * This query computes the 25th, 50th, 75th, and 90th percentiles of departure delays 
 * grouped by airline and hour of the day, alongside global min and max delays for each airline.
 */
public class HourlyDelayPercentiles extends BaseQuery {

    /**
     * Enable caching of intermediate datasets shared by multiple pipelines.
     */
    private static final boolean CACHE_ENABLED = true;

    @Override
    protected Dataset<Row> loadData(FlightRepository repository, ApplicationConfig config) {
        // Return raw Dataset<Row> with only needed columns
        String datasetFilename = config.getInput().getDatasetFilename();
        return repository.getFlightsOfAirlines(datasetFilename, "AA", "DL", "UA", "WN")
                .select("OP_UNIQUE_CARRIER", "CRS_DEP_TIME", "DEP_DELAY", "CANCELLED");
    }

    private static final int OP_UNIQUE_CARRIER_IDX = 0;
    private static final int CRS_DEP_TIME_IDX = 1;
    private static final int DEP_DELAY_IDX = 2;
    private static final int CANCELLED_IDX = 3;

    /**
     * Executes the query using the Spark RDD API.
     * For each (airline, hour-of-day), estimates p25/p50/p75/p90 of departure delay using a
     * mergeable quantile sketch (KLL or t-digest, selectable via config). For each airline,
     * also computes the exact global min/max departure delay. Both pipelines operate on the
     * non-cancelled subset.
     *
     * @param dataset the dataset to query
     * @param config the application configuration containing input/output paths and the chosen sketch
     * @return a list with two tuples: hourly percentiles RDD+schema, then global min/max RDD+schema
     */
    @Override
    protected List<Tuple2<JavaRDD<Row>, StructType>> runQueryRDD(Dataset<Row> dataset, ApplicationConfig config) {

        // Initialize the chosen percentile sketch based on configuration
        PercentileSketch sketch = PercentileSketch.from(config.getPercentileAlgorithm());

        // Load the dataset for specific airlines
        JavaRDD<Row> flights = dataset.javaRDD();

        // Single non-cancelled filter shared by both pipelines.
        JavaRDD<Row> validFlights = flights.filter(f ->
                f.getDouble(CANCELLED_IDX) == 0.0 && !f.isNullAt(DEP_DELAY_IDX)
        );

        if (CACHE_ENABLED) {
            validFlights.cache();
        }

        // --------------------------------------
        // --- Pipeline 1: hourly percentiles ---
        // --------------------------------------

        // Map each flight to a pair of ((airline, hour), delay)
        // and use combineByKey to build quantile sketches
        JavaPairRDD<Tuple2<String, Integer>, Double> hourlyPairs = validFlights
                .filter(f -> !f.isNullAt(CRS_DEP_TIME_IDX))
                .mapToPair(f -> {
                    int hour = (f.getInt(CRS_DEP_TIME_IDX) / 100) % 24; // BTS uses 2400 for end-of-day midnight
                    return new Tuple2<>(new Tuple2<>(f.getString(OP_UNIQUE_CARRIER_IDX), hour), f.getDouble(DEP_DELAY_IDX));
                });

        // Using combineByKey to build quantile sketches per (airline, hour)
        // without materializing large intermediate collections.
        JavaPairRDD<Tuple2<String, Integer>, byte[]> hourlySketches = hourlyPairs.combineByKey(
                sketch::init,
                sketch::update,
                sketch::merge
        );

        final int p25Idx = 0;
        final int p50Idx = 1;
        final int p75Idx = 2;
        final int p90Idx = 3;

        // Extract quantiles from sketches and format as Rows
        JavaRDD<Row> hourlyRows = hourlySketches.map(t -> {
            double[] q = sketch.getQuantiles(t._2, 0.25, 0.50, 0.75, 0.90);
            return RowFactory.create(
                    t._1._1,
                    t._1._2,
                    roundDecimals(q[p25Idx]),   // 25th percentile
                    roundDecimals(q[p50Idx]),   // 50th percentile
                    roundDecimals(q[p75Idx]),   // 75th percentile
                    roundDecimals(q[p90Idx])    // 90th percentile
            );
        });

        StructType hourlySchema = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("airline",         DataTypes.StringType, false),
            DataTypes.createStructField("hour",            DataTypes.IntegerType, false),
            DataTypes.createStructField("p25",             DataTypes.DoubleType, false),
            DataTypes.createStructField("p50",             DataTypes.DoubleType, false),
            DataTypes.createStructField("p75",             DataTypes.DoubleType, false),
            DataTypes.createStructField("p90",             DataTypes.DoubleType, false)
        });

        // ----------------------------------------------
        // --- Pipeline 2: global min/max per airline ---
        // ----------------------------------------------

        final int minDelayIdx = 0;
        final int maxDelayIdx = 1;

        // Using aggregateByKey to avoid creating millions of double arrays.
        JavaRDD<Row> globalRows = validFlights
                .mapToPair(f -> new Tuple2<>(f.getString(OP_UNIQUE_CARRIER_IDX), f))
                .aggregateByKey(
                    new double[]{Double.MAX_VALUE, -Double.MAX_VALUE}, // [0: min, 1: max]
                    (acc, flight) -> {
                        double delay = getDoubleSafe(flight, DEP_DELAY_IDX);
                        acc[minDelayIdx] = Math.min(acc[minDelayIdx], delay);
                        acc[maxDelayIdx] = Math.max(acc[maxDelayIdx], delay);
                        return acc;
                    },
                    (acc1, acc2) -> {
                        acc1[minDelayIdx] = Math.min(acc1[minDelayIdx], acc2[minDelayIdx]);
                        acc1[maxDelayIdx] = Math.max(acc1[maxDelayIdx], acc2[maxDelayIdx]);
                        return acc1;
                    }
                )
                .map(t -> RowFactory.create(t._1, t._2[minDelayIdx], t._2[maxDelayIdx]));

        StructType globalSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("airline",          DataTypes.StringType, false),
                DataTypes.createStructField("min_delay",        DataTypes.DoubleType, false),
                DataTypes.createStructField("max_delay",        DataTypes.DoubleType, false)
        });

        return Arrays.asList(
            new Tuple2<>(hourlyRows, hourlySchema),
            new Tuple2<>(globalRows, globalSchema)
        );
    }

    /**
     * Appends the chosen percentile algorithm (kll/tdigest) to the base target name, so RDD
     * runs with different sketches land in distinct output tables/files instead of overwriting.
     * DataFrame and SQL backends always use Spark's percentile_approx, so no qualifier is added.
     */
    @Override
    protected String buildBaseTargetName(ApplicationConfig config) {
        String base = super.buildBaseTargetName(config);
        if (config.getAppBackend() == AppBackendType.RDD) {
            PercentileAlgorithm algo = config.getPercentileAlgorithm();
            if (algo != null) {
                return base + "_" + algo.toString();
            }
        }
        return base;
    }

    /**
     * Executes the query using the Spark DataFrame API.
     * Computes percentiles of delays by hour and overall min/max delays per airline.
     *
     * @param flights the dataset to query
     * @param config the application configuration containing input/output paths
     * @return a list containing two Datasets: the first with hourly stats, the second with global min/max delays
     */
    @Override
    protected List<Dataset<Row>> runQueryDataFrame(Dataset<Row> flights, ApplicationConfig config) {

        Dataset<Row> validFlights = flights.filter(col("CANCELLED").equalTo(0));

        if (CACHE_ENABLED) {
            validFlights.cache();
        }

        Dataset<Row> hourlyStats = validFlights
                .withColumn("hour", col("CRS_DEP_TIME").divide(100).cast("int").mod(24))
                .groupBy("OP_UNIQUE_CARRIER", "hour")
                .agg(
                    round(expr("percentile_approx(DEP_DELAY, 0.25)"), 2).as("p25"),
                    round(expr("percentile_approx(DEP_DELAY, 0.50)"), 2).as("p50"),
                    round(expr("percentile_approx(DEP_DELAY, 0.75)"), 2).as("p75"),
                    round(expr("percentile_approx(DEP_DELAY, 0.90)"), 2).as("p90")
                )
                .withColumnRenamed("OP_UNIQUE_CARRIER", "airline");

        Dataset<Row> globalMinMax = validFlights
                .groupBy("OP_UNIQUE_CARRIER")
                .agg(
                    min("DEP_DELAY").as("min_delay"),
                    max("DEP_DELAY").as("max_delay")
                )
                .withColumnRenamed("OP_UNIQUE_CARRIER", "airline");

        return Arrays.asList(hourlyStats, globalMinMax);
    }

    /**
     * Executes the query using the Spark SQL API.
     * Computes percentiles of delays by hour and overall min/max delays per airline.
     *
     * @param flights the dataset to query
     * @param config the application configuration containing input/output paths
     * @param spark the active SparkSession to run SQL commands
     * @return a list containing two Datasets: the first with hourly stats, the second with global min/max delays
     */
    @Override
    protected List<Dataset<Row>> runQuerySQL(Dataset<Row> flights, ApplicationConfig config, SparkSession spark) {

        Dataset<Row> validFlights = flights.filter("CANCELLED = 0");

        if (CACHE_ENABLED) {
            validFlights.cache();
        }
        
        validFlights.createOrReplaceTempView("flights");

        String hourlyStatsSql = "SELECT OP_UNIQUE_CARRIER as airline, CAST(CRS_DEP_TIME / 100 AS INT) % 24 AS hour, " +
                                "ROUND(percentile_approx(DEP_DELAY, 0.25), 2) AS p25, " +
                                "ROUND(percentile_approx(DEP_DELAY, 0.50), 2) AS p50, " +
                                "ROUND(percentile_approx(DEP_DELAY, 0.75), 2) AS p75, " +
                                "ROUND(percentile_approx(DEP_DELAY, 0.90), 2) AS p90 " +
                                "FROM flights " +
                                "GROUP BY OP_UNIQUE_CARRIER, hour";
        
        Dataset<Row> hourlyStats = spark.sql(hourlyStatsSql);

        String globalMinMaxSql =    "SELECT OP_UNIQUE_CARRIER as airline, MIN(DEP_DELAY) AS min_delay, MAX(DEP_DELAY) AS max_delay " +
                                    "FROM flights " +
                                    "GROUP BY OP_UNIQUE_CARRIER";
        
        Dataset<Row> globalMinMax = spark.sql(globalMinMaxSql);

        return Arrays.asList(hourlyStats, globalMinMax);
    }
}
