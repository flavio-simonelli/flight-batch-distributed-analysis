package it.uniroma2.sae.query;

import it.uniroma2.sae.config.AppBackendType;
import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.PercentileAlgorithm;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.repository.FlightRepository;
import it.uniroma2.sae.util.SparkDiagnostics;
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

    @Override
    protected Dataset<RawFlight> loadData(FlightRepository repository, ApplicationConfig config) {
        String datasetFilename = config.getInput().getDatasetFilename();
        return repository.getFlightsOfAirlines(datasetFilename, "AA", "DL", "UA", "WN");
    }

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
    protected List<Tuple2<JavaRDD<Row>, StructType>> runQueryRDD(Dataset<RawFlight> dataset, ApplicationConfig config) {
        PercentileSketch sketch = PercentileSketch.from(config.getPercentileAlgorithm());
        JavaRDD<RawFlight> flights = dataset.javaRDD();

        // Single non-cancelled filter shared by both pipelines (cached to avoid double scan).
        JavaRDD<RawFlight> validFlights = flights.filter(f ->
                f.getCancelled() != null && f.getCancelled() == 0.0 && f.getDepDelay() != null
        ).cache();

        // Pipeline 1: hourly percentiles
        JavaPairRDD<Tuple2<String, Integer>, Double> hourlyPairs = validFlights
                .filter(f -> f.getCrsDepTime() != null)
                .mapToPair(f -> {
                    int hour = (f.getCrsDepTime() / 100) % 24; // BTS uses 2400 for end-of-day midnight
                    return new Tuple2<>(new Tuple2<>(f.getOpUniqueCarrier(), hour), f.getDepDelay());
                });

        JavaPairRDD<Tuple2<String, Integer>, byte[]> hourlySketches = hourlyPairs.combineByKey(
                sketch::init,
                sketch::update,
                sketch::merge
        );

        JavaRDD<Row> hourlyRows = hourlySketches.map(t -> {
            double[] q = sketch.getQuantiles(t._2, 0.25, 0.50, 0.75, 0.90);
            return RowFactory.create(
                    t._1._1,
                    t._1._2,
                    round2(q[0]),
                    round2(q[1]),
                    round2(q[2]),
                    round2(q[3])
            );
        });

        SparkDiagnostics.profilePartitions(hourlyRows, "Q3 hourly percentiles");
        SparkDiagnostics.checkSkew(hourlyRows, "Q3 hourly percentiles", 3.0);

        JavaRDD<Row> hourlySorted = hourlyRows
                .sortBy(r -> r.getString(0) + String.format("_%02d", r.getInt(1)),
                        true, hourlyRows.getNumPartitions())
                .coalesce(1);

        StructType hourlySchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("opUniqueCarrier", DataTypes.StringType, false),
                DataTypes.createStructField("hour",            DataTypes.IntegerType, false),
                DataTypes.createStructField("p25",             DataTypes.DoubleType, false),
                DataTypes.createStructField("p50",             DataTypes.DoubleType, false),
                DataTypes.createStructField("p75",             DataTypes.DoubleType, false),
                DataTypes.createStructField("p90",             DataTypes.DoubleType, false)
        });

        // Pipeline 2: global min/max per airline
        JavaRDD<Row> globalRows = validFlights
                .mapToPair(f -> new Tuple2<>(
                        f.getOpUniqueCarrier(),
                        new double[]{f.getDepDelay(), f.getDepDelay()}
                ))
                .reduceByKey((a, b) -> new double[]{
                        Math.min(a[0], b[0]),
                        Math.max(a[1], b[1])
                })
                .map(t -> RowFactory.create(t._1, t._2[0], t._2[1]))
                .sortBy(r -> r.getString(0), true, 1)
                .coalesce(1);

        StructType globalSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("opUniqueCarrier",  DataTypes.StringType, false),
                DataTypes.createStructField("min_delay_global", DataTypes.DoubleType, false),
                DataTypes.createStructField("max_delay_global", DataTypes.DoubleType, false)
        });

        validFlights.unpersist();

        return Arrays.asList(
                new Tuple2<>(hourlySorted, hourlySchema),
                new Tuple2<>(globalRows,   globalSchema)
        );
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
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
    protected List<Dataset<Row>> runQueryDataFrame(Dataset<RawFlight> flights, ApplicationConfig config) {
        flights.cache();

        Dataset<Row> hourlyStats = flights
                .filter(col("cancelled").equalTo(0))
                .withColumn("hour", col("crsDepTime").divide(100).cast("int"))
                .groupBy("opUniqueCarrier", "hour")
                .agg(
                        round(expr("percentile_approx(depDelay, 0.25)"), 2).as("p25"),
                        round(expr("percentile_approx(depDelay, 0.50)"), 2).as("p50"),
                        round(expr("percentile_approx(depDelay, 0.75)"), 2).as("p75"),
                        round(expr("percentile_approx(depDelay, 0.90)"), 2).as("p90")
                )
                .orderBy("opUniqueCarrier", "hour");

        Dataset<Row> globalMinMax = flights
                .groupBy("opUniqueCarrier")
                .agg(
                    min("depDelay").as("min_delay_global"),
                    max("depDelay").as("max_delay_global")
                )
                .orderBy("opUniqueCarrier");

        flights.unpersist();

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
    protected List<Dataset<Row>> runQuerySQL(Dataset<RawFlight> flights, ApplicationConfig config, SparkSession spark) {
        flights.cache();
        
        flights.createOrReplaceTempView("flights");

        String hourlyStatsSql = "SELECT opUniqueCarrier, CAST(crsDepTime / 100 AS INT) AS hour, " +
                "ROUND(percentile_approx(depDelay, 0.25), 2) AS p25, " +
                "ROUND(percentile_approx(depDelay, 0.50), 2) AS p50, " +
                "ROUND(percentile_approx(depDelay, 0.75), 2) AS p75, " +
                "ROUND(percentile_approx(depDelay, 0.90), 2) AS p90 " +
                "FROM flights " +
                "WHERE cancelled = 0 " +
                "GROUP BY opUniqueCarrier, hour " +
                "ORDER BY opUniqueCarrier, hour";
        
        Dataset<Row> hourlyStats = spark.sql(hourlyStatsSql);

        String globalMinMaxSql = "SELECT opUniqueCarrier, MIN(depDelay) AS min_delay_global, MAX(depDelay) AS max_delay_global " +
                "FROM flights " +
                "GROUP BY opUniqueCarrier " +
                "ORDER BY opUniqueCarrier";
        
        Dataset<Row> globalMinMax = spark.sql(globalMinMaxSql);

        flights.unpersist();

        return Arrays.asList(hourlyStats, globalMinMax);
    }
}
