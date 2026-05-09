package it.uniroma2.sae.repository;

import org.apache.spark.sql.SparkSession;

/**
 * An implementation of {@link FlightRepository} that reads flight data from the local file system.
 */
public class LocalFlightRepository extends FlightRepository {

    private final String dataPath;

    /**
     * Constructs a new LocalFlightRepository.
     *
     * @param spark the SparkSession
     * @param dataPath the absolute path on the local file system where data is stored
     */
    public LocalFlightRepository(SparkSession spark, String dataPath) {
        super(spark);
        this.dataPath = dataPath;
    }

    /**
     * Constructs the full local path to a specific flight data file.
     * Prepends the "file://" scheme required by Spark for local file access.
     *
     * @param filename the name of the file
     * @return the full local file path (e.g., "file:///opt/spark/data/flights.parquet")
     */
    @Override
    protected String getFullPath(String filename) {
        return "file://" + dataPath + filename;
    }
}
