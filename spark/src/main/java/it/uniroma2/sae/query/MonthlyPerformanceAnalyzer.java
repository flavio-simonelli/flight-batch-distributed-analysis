package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

/**
 * Implementation of Query 1: Monthly Performance Analyzer.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 */
public class MonthlyPerformanceAnalyzer extends BaseQuery {

    /**
     * Executes the specific logic for Query 1 using the provided dataset.
     *
     * @param repository the input repository
     * @param config the application configuration
     * @return a Dataset<Row> containing the average delay by origin
     */
    @Override
    protected Dataset<Row> runQuery(FlightRepository repository, ApplicationConfig config) {

        String datasetFilename = config.getInput().getDatasetFilename();
        if (datasetFilename == null || datasetFilename.isEmpty()) {
            throw new IllegalArgumentException("datasetFilename is not defined in config.yml");
        }
        Dataset<RawFlight> flights = repository.getFlightsOfAirlines(datasetFilename, "AA", "DL");
        System.out.println("=== Dataset caricato ===");
        flights.show(5);

        Dataset<Row> result = flights
                .groupBy("month", "opUniqueCarrier")
                .agg(
                        round(avg(when(col("cancelled").equalTo(0), col("depDelay"))), 2).as("dep-delay-mean"),
                        round(min(when(col("cancelled").equalTo(0), col("depDelay"))), 2).as("dep-delay-min"),
                        round(max(when(col("cancelled").equalTo(0), col("depDelay"))), 2).as("dep-delay-max"),
                        round(sum(col("cancelled")).divide(count("*")).multiply(100), 2).as("cancellation-rate")
                )
                .orderBy("month", "opUniqueCarrier");
        result.show();
        
        return result;
    }
}
