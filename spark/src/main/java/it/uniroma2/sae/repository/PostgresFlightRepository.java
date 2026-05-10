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

    @Override
    protected String getDriver(JdbcStorageConfig config) {
        return config.getDriver() != null ? config.getDriver() : "org.postgresql.Driver";
    }

    @Override
    protected String getUrl(JdbcStorageConfig config) {
        return config.getUrl() != null ? config.getUrl() : "jdbc:postgresql://" + config.getHostname() + ":" + config.getPort() + "/" + config.getDatabase();
    }
}
