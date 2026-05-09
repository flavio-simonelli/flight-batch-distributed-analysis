package it.uniroma2.sae.service;

import it.uniroma2.sae.model.RawFlight;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

public class FlightService {

    public Dataset<RawFlight> getDelayedFlights(Dataset<RawFlight> flights) {
        return flights
                .filter(col("arrDelay").gt(0))
                .orderBy(col("arrDelay").desc());
    }

    public Dataset<Row> getAverageDelayByOrigin(Dataset<RawFlight> flights) {
        return flights
                .groupBy("originAirportId")
                .agg(avg("arrDelay").alias("avgDelay"))
                .orderBy(col("avgDelay").desc());
    }
}