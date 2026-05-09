package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.round;

/**
 * Implementation of Query 3: Hourly Delay Percentiles.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 */
public class HourlyDelayPercentiles extends BaseQuery {

    /**
     * Executes the specific logic for Query 3 using the provided dataset.
     *
     * @param repository the input repository
     * @param config the application configuration
     * @return a Dataset<Row> containing the hourly delay percentiles
     */
    @Override
    protected Dataset<Row> runQuery(FlightRepository repository, ApplicationConfig config) {
        String datasetFilename = config.getInput().getDatasetFilename();

        Dataset<RawFlight> flights = repository.getFlightsOfAirlines(datasetFilename, "AA", "DL", "UA", "WN");
        flights.cache();
        System.out.println("=== Dataset caricato (e messo in cache) ===");
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

        return hourlyStats;
    }
}
