package it.uniroma2.sae.repository;

import it.uniroma2.sae.config.DbStorageConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Base abstract class for database-based repositories.
 * It centralizes the database configuration and disables methods that are not 
 * applicable to database sources.
 *
 * @param <T> the type of database configuration
 */
public abstract class DbFlightRepository<T extends DbStorageConfig> extends FlightRepository {

    protected final T config;

    /**
     * Constructs a new DbFlightRepository.
     *
     * @param spark the SparkSession to be used for data operations
     * @param config the database configuration
     */
    public DbFlightRepository(SparkSession spark, T config) {
        super(spark);
        this.config = config;
    }

    /**
     * Reading from database sources is currently not supported.
     */
    @Override
    public final Dataset<Row> getFlights(String datasetFilename) {
        throw new UnsupportedOperationException("Reading flight data from this database source is not supported.");
    }

    /**
     * File paths are not applicable for database connections.
     */
    @Override
    protected final String getFullPath(String filename) {
        throw new UnsupportedOperationException("getFullPath is not applicable for database connections.");
    }
}
