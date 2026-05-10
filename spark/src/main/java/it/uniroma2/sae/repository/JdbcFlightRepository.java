package it.uniroma2.sae.repository;

import it.uniroma2.sae.config.JdbcStorageConfig;
import it.uniroma2.sae.model.RawFlight;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import java.util.Properties;

/**
 * An abstract repository for JDBC-based data sources.
 * It centralizes the logic for saving Datasets to a JDBC-compliant database.
 */
public abstract class JdbcFlightRepository extends FlightRepository {

    protected final JdbcStorageConfig config;

    public JdbcFlightRepository(SparkSession spark, JdbcStorageConfig config) {
        super(spark);
        this.config = config;
        checkConnectionDetails();
    }

    private void checkConnectionDetails() {
        if (config.getHostname() == null || config.getHostname().isEmpty()) throw new IllegalArgumentException("Invalid JDBC URL.");
        if (config.getPort() == null || config.getPort() <= 0) throw new IllegalArgumentException("Invalid JDBC port.");
        if (config.getDatabase() == null || config.getDatabase().isEmpty()) throw new IllegalArgumentException("JDBC database name cannot be empty.");
        if (config.getUser() == null || config.getUser().isEmpty()) throw new IllegalArgumentException("JDBC user cannot be empty.");
        if (config.getPassword() == null || config.getPassword().isEmpty()) throw new IllegalArgumentException("JDBC password cannot be empty.");
        if (getDriver(config) == null || getDriver(config).isEmpty()) throw new IllegalArgumentException("JDBC driver must be provided.");
        if (getUrl(config) == null || getUrl(config).isEmpty() || !getUrl(config).startsWith("jdbc:")) throw new IllegalArgumentException("JDBC URL must be provided and start with 'jdbc:'.");
    }

    /**
     * Returns the specific JDBC driver class name.
     * @param config the configuration object
     * @return the driver class name
     */
    protected abstract String getDriver(JdbcStorageConfig config);

    /**
     * Return the specific JDBC URL.
     * @param config the configuration object
     * @return the JDBC URL
     */
    protected abstract String getUrl(JdbcStorageConfig config);

    @Override
    public Dataset<RawFlight> getFlights(String datasetFilename) {
        throw new UnsupportedOperationException("Reading flight data from a JDBC source is not supported in this repository.");
    }

    @Override
    protected String getFullPath(String filename) {
        throw new UnsupportedOperationException("getFullPath is not applicable for JDBC connections.");
    }

    @Override
    public final void saveResults(Dataset<Row> results, String table) {
        if (results == null) throw new IllegalArgumentException("Results dataset cannot be null.");
        if (table == null || table.isEmpty()) throw new IllegalArgumentException("Target table name must be provided for JDBC output.");

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", config.getUser());
        connectionProperties.put("password", config.getPassword());
        connectionProperties.put("driver", getDriver(config));

        results.write()
                .mode(SaveMode.Overwrite)
                .jdbc(getUrl(config), table, connectionProperties);
    }

    @Override
    public final void saveResults(JavaRDD<Row> results, StructType schema, String table) {
        if (results == null) throw new IllegalArgumentException("Results RDD cannot be null.");
        if (schema == null) throw new IllegalArgumentException("Schema cannot be null when saving JavaRDD<Row>.");
        
        Dataset<Row> convertedResults = spark.createDataFrame(results, schema);
        saveResults(convertedResults, table);
    }
}
