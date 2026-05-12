package it.uniroma2.sae.repository;

import it.uniroma2.sae.config.JdbcStorageConfig;
import org.apache.spark.sql.SparkSession;

/**
 * An implementation of {@link JdbcFlightRepository} for PostgreSQL.
 */
public class PostgresFlightRepository extends JdbcFlightRepository {

    public PostgresFlightRepository(SparkSession spark, JdbcStorageConfig config) {
        super(spark, config);
    }
}
