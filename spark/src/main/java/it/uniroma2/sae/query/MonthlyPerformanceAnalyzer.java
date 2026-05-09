package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.service.FlightService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Implementation of Query 1: Monthly Performance Analyzer.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 */
public class MonthlyPerformanceAnalyzer extends BaseQuery {

    /**
     * Executes the specific logic for Query 1 using the provided dataset.
     *
     * @param flights the raw flight dataset
     * @param config the application configuration
     * @return a Dataset<Row> containing the average delay by origin
     */
    @Override
    protected Dataset<Row> runQuery(Dataset<RawFlight> flights, ApplicationConfig config) {
        FlightService service = new FlightService();

        System.out.println("=== Dataset caricato ===");
        flights.show(5);

        System.out.println("=== Voli in ritardo (delay > 0) ===");
        service.getDelayedFlights(flights)
               .show();

        System.out.println("=== Ritardo medio per origine ===");
        Dataset<Row> avgDelayByOrigin = service.getAverageDelayByOrigin(flights);
        avgDelayByOrigin.show();
        
        return avgDelayByOrigin;
    }
}
