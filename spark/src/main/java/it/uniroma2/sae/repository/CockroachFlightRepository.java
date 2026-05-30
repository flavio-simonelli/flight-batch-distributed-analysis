package it.uniroma2.sae.repository;

import it.uniroma2.sae.config.JdbcStorageConfig;
import org.apache.spark.sql.SparkSession;

/**
 * An implementation of {@link JdbcFlightRepository} for CockroachDB.
 * Since CockroachDB is wire-compatible with PostgreSQL, it reuses the standard PostgreSQL JDBC driver.
 */
public class CockroachFlightRepository extends JdbcFlightRepository {

    /**
     * Constructs a new CockroachFlightRepository with the given SparkSession and JDBC configuration.
     *
     * @param spark the SparkSession to be used for data operations
     * @param config the JDBC storage configuration
     */
    public CockroachFlightRepository(SparkSession spark, JdbcStorageConfig config) {
        super(spark, config);
    }
}
