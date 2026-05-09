package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

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
        // TODO: Implement Query 3 logic here based on the project requirements
        System.out.println("Executing Query 3 logic...");
        
        return null;
    }
}
