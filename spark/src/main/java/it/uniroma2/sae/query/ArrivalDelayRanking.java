package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Implementation of Query 2: Arrival Delay Ranking.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 */
public class ArrivalDelayRanking extends BaseQuery {

    @Override
    protected Dataset<Row> runQueryDataFrame(FlightRepository repository, ApplicationConfig config) {
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

    @Override
    protected Dataset<Row> runQuerySQL(FlightRepository repository, ApplicationConfig config, SparkSession spark) {
        String datasetFilename = config.getInput().getDatasetFilename();
        Dataset<RawFlight> flights = repository.getFlights(datasetFilename);

        flights.createOrReplaceTempView("flights");

        String sqlQuery = "SELECT opUniqueCarrier, " +
                "COUNT(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 END) AS num_flights, " +
                "ROUND(AVG(arrDelay), 2) AS arrdelay_mean, " +
                "ROUND(AVG(carrierDelay), 2) AS carrier_delay_mean, " +
                "ROUND(AVG(weatherDelay), 2) AS weather_delay_mean, " +
                "ROUND(AVG(nasDelay), 2) AS nas_delay_mean, " +
                "ROUND(AVG(securityDelay), 2) AS security_delay_mean, " +
                "ROUND(AVG(lateAircraftDelay), 2) AS late_aircraft_delay_mean " +
                "FROM flights " +
                "GROUP BY opUniqueCarrier " +
                "HAVING num_flights > 500 " +
                "ORDER BY arrdelay_mean DESC " +
                "LIMIT 10";

        Dataset<Row> result = spark.sql(sqlQuery);
        result.show();
        return result;
    }
}
