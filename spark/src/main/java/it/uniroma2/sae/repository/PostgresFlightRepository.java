package it.uniroma2.sae.repository;

import it.uniroma2.sae.model.RawFlight;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import java.util.Properties;

/**
 * An implementation of {@link FlightRepository} that writes results to a PostgreSQL database.
 * Reading from Postgres is not supported in this architecture.
 */
public class PostgresFlightRepository extends FlightRepository {

    private final String url;
    private final String user;
    private final String password;

    public PostgresFlightRepository(SparkSession spark, String url, String user, String password) {
        super(spark);
        this.url = url;
        this.user = user;
        this.password = password;
        checkConnectionDetails();
    }

    private void checkConnectionDetails() {
        if (url == null || url.isEmpty() || !url.startsWith("jdbc:postgresql://")) {
            throw new IllegalArgumentException("Invalid Postgres URL.");
        }
        if (user == null || user.isEmpty()) {
            throw new IllegalArgumentException("Postgres user cannot be empty.");
        }
    }

    /**
     * Reading from Postgres is not supported. This method will always throw an exception.
     */
    @Override
    public Dataset<RawFlight> getFlights(String datasetFilename) {
        throw new UnsupportedOperationException("Reading flight data from PostgreSQL is not supported in this repository.");
    }

    /**
     * This method is not applicable for JDBC connections.
     */
    @Override
    protected String getFullPath(String filename) {
        throw new UnsupportedOperationException("getFullPath is not applicable for JDBC connections.");
    }

    /**
     * Saves the given Dataset to a table in the PostgreSQL database.
     *
     * @param results the Dataset to save
     * @param dbtable the name of the target database table
     */
    @Override
    public final void saveResults(Dataset<Row> results, String dbtable) {
        if (results == null) {
            throw new IllegalArgumentException("Results dataset cannot be null.");
        }
        if (dbtable == null || dbtable.isEmpty()) {
             throw new IllegalArgumentException("Target table name must be provided for PostgreSQL output.");
        }

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", this.user);
        connectionProperties.put("password", this.password);
        connectionProperties.put("driver", "org.postgresql.Driver");

        results.write()
                .mode(SaveMode.Overwrite)
                .jdbc(this.url, dbtable, connectionProperties);
    }
    
    /**
     * Saves the given JavaRDD to a table in the PostgreSQL database.
     *
     * @param results the JavaRDD to save
     * @param dbtable the name of the target database table
     */
    @Override
    public final void saveResults(JavaRDD<Row> results, String dbtable) {
        if (results == null) {
            throw new IllegalArgumentException("Results RDD cannot be null.");
        }
        if (results.isEmpty()) return;
        
        StructType schema = results.first().schema();
        Dataset<Row> convertedResults = spark.createDataFrame(results, schema);
        saveResults(convertedResults, dbtable);
    }
}
