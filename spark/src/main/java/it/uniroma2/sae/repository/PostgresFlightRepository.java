package it.uniroma2.sae.repository;

import it.uniroma2.sae.config.JdbcStorageConfig;
import org.apache.spark.sql.SparkSession;

/**
 * An implementation of {@link JdbcFlightRepository} for PostgreSQL.
 */
public class PostgresFlightRepository extends JdbcFlightRepository {

    /**
     * Constructs a new PostgresFlightRepository with the given SparkSession and JDBC configuration.
     *
     * @param spark the SparkSession to be used for data operations
     * @param config the JDBC storage configuration
     */
    public PostgresFlightRepository(SparkSession spark, JdbcStorageConfig config) {
        super(spark, config);
    }
}
