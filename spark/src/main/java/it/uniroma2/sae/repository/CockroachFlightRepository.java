package it.uniroma2.sae.repository;

import it.uniroma2.sae.config.JdbcStorageConfig;
import org.apache.spark.sql.SparkSession;

/**
 * An implementation of {@link JdbcFlightRepository} for CockroachDB.
 * Since CockroachDB is wire-compatible with PostgreSQL, it reuses the standard PostgreSQL JDBC driver.
 */
public class CockroachFlightRepository extends JdbcFlightRepository {

    public CockroachFlightRepository(SparkSession spark, JdbcStorageConfig config) {
        super(spark, config);
    }
}
