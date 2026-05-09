package it.uniroma2.sae.repository;

import it.uniroma2.sae.model.RawFlight;
import org.apache.spark.sql.Dataset;

public interface FlightRepository {
    Dataset<RawFlight> getFlights(String filename);
}
