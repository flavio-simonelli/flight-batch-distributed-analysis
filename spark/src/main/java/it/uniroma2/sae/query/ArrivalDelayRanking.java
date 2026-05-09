package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.RawFlight;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Implementation of Query 2: Arrival Delay Ranking.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 */
public class ArrivalDelayRanking extends BaseQuery {

    /**
     * Executes the specific logic for Query 2 using the provided dataset.
     *
     * @param flights the raw flight dataset
     * @param config the application configuration
     * @return a Dataset<Row> containing the arrival delay ranking
     */
    @Override
    protected Dataset<Row> runQuery(Dataset<RawFlight> flights, ApplicationConfig config) {
        // TODO: Implement Query 2 logic here based on the project requirements
        System.out.println("Executing Query 2 logic...");
        
        return null;
    }
}
