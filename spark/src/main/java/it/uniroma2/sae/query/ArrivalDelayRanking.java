package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

/**
 * Implementation of Query 2: Arrival Delay Ranking.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 */
public class ArrivalDelayRanking extends BaseQuery {

    /**
     * Executes the specific logic for Query 2 using the provided dataset.
     *
     * @param repository the input repository
     * @param config the application configuration
     * @return a Dataset<Row> containing the arrival delay ranking
     */
    @Override
    protected Dataset<Row> runQuery(FlightRepository repository, ApplicationConfig config) {
        String datasetFilename = config.getInput().getDatasetFilename();

        Dataset<RawFlight> flights = repository.getFlights(datasetFilename);
        System.out.println("=== Dataset caricato ===");
        flights.show(5);

        Dataset<Row> result = flights
                .groupBy("opUniqueCarrier")
                .agg(
                        count(when(col("cancelled").equalTo(0).and(col("diverted").equalTo(0)), 1)).as("num_flights"),
                        round(avg(col("arrDelay")), 2).as("arrdelay_mean"),
                        round(avg(col("carrierDelay")), 2).as("carrier_delay_mean"),
                        round(avg(col("weatherDelay")), 2).as("weather_delay_mean"),
                        round(avg(col("nasDelay")), 2).as("nas_delay_mean"),
                        round(avg(col("securityDelay")), 2).as("security_delay_mean"),
                        round(avg(col("lateAircraftDelay")), 2).as("late_aircraft_delay_mean")
                )
                .filter(col("num_flights").gt(500))
                .orderBy(col("arrdelay_mean").desc())
                .limit(10);
        result.show();

        return result;
    }
}
