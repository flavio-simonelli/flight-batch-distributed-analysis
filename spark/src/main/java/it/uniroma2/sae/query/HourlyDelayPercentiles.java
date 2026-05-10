package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
    protected List<Tuple2<JavaRDD<Row>, StructType>> runQueryRDD(FlightRepository repository, ApplicationConfig config) {
        throw new UnsupportedOperationException("RDD backend is not implemented for this query.");
    }

    /**
     * Executes the query using the Spark DataFrame API.
     * Computes percentiles of delays by hour and overall min/max delays per airline.
     *
     * @param repository the repository used to load flight data
     * @param config the application configuration containing input/output paths
     * @return a list containing two Datasets: the first with hourly stats, the second with global min/max delays
     */
    @Override
    protected List<Dataset<Row>> runQueryDataFrame(FlightRepository repository, ApplicationConfig config) {
        String datasetFilename = config.getInput().getDatasetFilename();

        Dataset<RawFlight> flights = repository.getFlightsOfAirlines(datasetFilename, "AA", "DL", "UA", "WN");
        flights.cache();
        System.out.println("=== Dataset loaded (and cached) ===");
        flights.show(5);

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

        hourlyStats.show(24);
        globalMinMax.show();

        flights.unpersist();

        return Arrays.asList(hourlyStats, globalMinMax);
    }

    /**
     * Executes the query using the Spark SQL API.
     * Computes percentiles of delays by hour and overall min/max delays per airline.
     *
     * @param repository the repository used to load flight data
     * @param config the application configuration containing input/output paths
     * @param spark the active SparkSession to run SQL commands
     * @return a list containing two Datasets: the first with hourly stats, the second with global min/max delays
     */
    @Override
    protected List<Dataset<Row>> runQuerySQL(FlightRepository repository, ApplicationConfig config, SparkSession spark) {
        String datasetFilename = config.getInput().getDatasetFilename();
        Dataset<RawFlight> flights = repository.getFlightsOfAirlines(datasetFilename, "AA", "DL", "UA", "WN");
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

        hourlyStats.show(24);
        globalMinMax.show();

        flights.unpersist();

        return Arrays.asList(hourlyStats, globalMinMax);
    }
}
