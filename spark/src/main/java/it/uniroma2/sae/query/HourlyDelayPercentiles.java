package it.uniroma2.sae.query;

import it.uniroma2.sae.config.AppBackendType;
import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.PercentileAlgorithm;
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

        PercentileSketch sketch = PercentileSketch.from(config.getPercentileAlgorithm());
        JavaRDD<Row> flights = dataset.javaRDD();

        // Single non-cancelled filter shared by both pipelines (cached to avoid double scan).
        JavaRDD<Row> validFlights = flights.filter(f ->
                f.getDouble(CANCELLED_IDX) == 0.0 && !f.isNullAt(DEP_DELAY_IDX)
        ).cache();

        // Pipeline 1: hourly percentiles
        JavaPairRDD<Tuple2<String, Integer>, Double> hourlyPairs = validFlights
                .filter(f -> !f.isNullAt(CRS_DEP_TIME_IDX))
                .mapToPair(f -> {
                    int hour = (f.getInt(CRS_DEP_TIME_IDX) / 100) % 24; // BTS uses 2400 for end-of-day midnight
                    return new Tuple2<>(new Tuple2<>(f.getString(OP_UNIQUE_CARRIER_IDX), hour), f.getDouble(DEP_DELAY_IDX));
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
                .sortBy(r -> r.getString(0) + (r.getInt(1) < 10 ? "_0" : "_") + r.getInt(1),
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
        // Using aggregateByKey to avoid creating millions of double[2] arrays.
        JavaRDD<Row> globalRows = validFlights
                .mapToPair(f -> new Tuple2<>(f.getString(OP_UNIQUE_CARRIER_IDX), f))
                .aggregateByKey(
                    new double[]{Double.MAX_VALUE, -Double.MAX_VALUE}, // [0: min, 1: max]
                    (acc, flight) -> {
                        double delay = getDoubleSafe(flight, DEP_DELAY_IDX);
                        acc[0] = Math.min(acc[0], delay);
                        acc[1] = Math.max(acc[1], delay);
                        return acc;
                    },
                    (acc1, acc2) -> {
                        acc1[0] = Math.min(acc1[0], acc2[0]);
                        acc1[1] = Math.max(acc1[1], acc2[1]);
                        return acc1;
                    }
                )
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
    protected List<Dataset<Row>> runQueryDataFrame(Dataset<Row> flights, ApplicationConfig config) {
        flights.cache();

        Dataset<Row> hourlyStats = flights
                .filter(col("CANCELLED").equalTo(0))
                .withColumn("hour", col("CRS_DEP_TIME").divide(100).cast("int"))
                .groupBy("OP_UNIQUE_CARRIER", "hour")
                .agg(
                        round(expr("percentile_approx(DEP_DELAY, 0.25)"), 2).as("p25"),
                        round(expr("percentile_approx(DEP_DELAY, 0.50)"), 2).as("p50"),
                        round(expr("percentile_approx(DEP_DELAY, 0.75)"), 2).as("p75"),
                        round(expr("percentile_approx(DEP_DELAY, 0.90)"), 2).as("p90")
                )
                .withColumnRenamed("OP_UNIQUE_CARRIER", "opUniqueCarrier")
                .orderBy("opUniqueCarrier", "hour");

        Dataset<Row> globalMinMax = flights
                .groupBy("OP_UNIQUE_CARRIER")
                .agg(
                    min("DEP_DELAY").as("min_delay_global"),
                    max("DEP_DELAY").as("max_delay_global")
                )
                .withColumnRenamed("OP_UNIQUE_CARRIER", "opUniqueCarrier")
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
    protected List<Dataset<Row>> runQuerySQL(Dataset<Row> flights, ApplicationConfig config, SparkSession spark) {
        flights.cache();
        
        flights.createOrReplaceTempView("flights");

        String hourlyStatsSql = "SELECT OP_UNIQUE_CARRIER as opUniqueCarrier, CAST(CRS_DEP_TIME / 100 AS INT) AS hour, " +
                "ROUND(percentile_approx(DEP_DELAY, 0.25), 2) AS p25, " +
                "ROUND(percentile_approx(DEP_DELAY, 0.50), 2) AS p50, " +
                "ROUND(percentile_approx(DEP_DELAY, 0.75), 2) AS p75, " +
                "ROUND(percentile_approx(DEP_DELAY, 0.90), 2) AS p90 " +
                "FROM flights " +
                "WHERE CANCELLED = 0 " +
                "GROUP BY OP_UNIQUE_CARRIER, hour " +
                "ORDER BY opUniqueCarrier, hour";
        
        Dataset<Row> hourlyStats = spark.sql(hourlyStatsSql);

        String globalMinMaxSql = "SELECT OP_UNIQUE_CARRIER as opUniqueCarrier, MIN(DEP_DELAY) AS min_delay_global, MAX(DEP_DELAY) AS max_delay_global " +
                "FROM flights " +
                "GROUP BY OP_UNIQUE_CARRIER " +
                "ORDER BY opUniqueCarrier";
        
        Dataset<Row> globalMinMax = spark.sql(globalMinMaxSql);

        flights.unpersist();

        return Arrays.asList(hourlyStats, globalMinMax);
    }
}
