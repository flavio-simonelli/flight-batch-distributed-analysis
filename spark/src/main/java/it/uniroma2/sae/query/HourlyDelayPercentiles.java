package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.RawFlight;
import org.apache.spark.sql.Dataset;

/**
 * Implementation of Query 3: Hourly Delay Percentiles.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 */
public class HourlyDelayPercentiles extends BaseQuery {

    /**
     * Executes the specific logic for Query 3 using the provided dataset.
     *
     * @param flights the raw flight dataset
     * @param config the application configuration
     */
    @Override
    protected void runQuery(Dataset<RawFlight> flights, ApplicationConfig config) {
        // TODO: Implement Query 3 logic here based on the project requirements
        System.out.println("Executing Query 3 logic...");
    }
}
