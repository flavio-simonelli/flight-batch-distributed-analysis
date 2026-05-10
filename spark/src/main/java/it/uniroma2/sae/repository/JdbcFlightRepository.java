package it.uniroma2.sae.repository;

import it.uniroma2.sae.config.JdbcStorageConfig;
import it.uniroma2.sae.model.RawFlight;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.*;
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

    /**
     * Saves the given Dataset as a table to the specified output database.
     *
     * @param results the Dataset to save
     * @param table the name of the output table
     */
    @Override
    public final void saveResults(Dataset<Row> results, String table) {
        if (results == null) throw new IllegalArgumentException("Results dataset cannot be null.");
        if (table == null || table.isEmpty()) throw new IllegalArgumentException("Target table name must be provided for JDBC output.");

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", config.getUser());
        connectionProperties.put("password", config.getPassword());
        connectionProperties.put("driver", getDriver(config));
        connectionProperties.put("batchsize", "10000");
        connectionProperties.put("rewriteBatchedStatements", "true");

        results.write()
                .mode(SaveMode.Overwrite)
                .jdbc(getUrl(config), table, connectionProperties);
    }

    /**
     * Saves the given JavaRDD as a table to the specified output database.
     *
     * @param results the JavaRDD to save
     * @param schema the schema of the RDD
     * @param table the name of the output table
     */
    @Override
    public final void saveResults(JavaRDD<Row> results, StructType schema, String table) {
        if (results == null) throw new IllegalArgumentException("Results RDD cannot be null.");
        if (schema == null) throw new IllegalArgumentException("Schema cannot be null when saving JavaRDD<Row>.");
        // Avoid to use .empty() for performance reasons
        // if (results.isEmpty()) return;
        
        Dataset<Row> convertedResults = spark.createDataFrame(results, schema);
        saveResults(convertedResults, table);
    }

    /**
     * @deprecated This method is deprecated in favor of the version that accepts a Dataset<Row> for better performance and simplicity.
     * Saves the given JavaRDD as a table to the specified output database.
     *
     * @param jsc the SparkContext to use
     * @param results the JavaRDD to save
     * @param schema the schema of the RDD
     * @param table the name of the output table
     */
    @Override
    @Deprecated
    public final void saveResults(JavaSparkContext jsc, JavaRDD<Row> results, StructType schema, String table) {
        if (results == null) throw new IllegalArgumentException("Results RDD cannot be null.");
        if (schema == null) throw new IllegalArgumentException("Schema is required.");
        // Avoid to use .empty() for performance reasons
        // if (results.isEmpty()) return;

        System.out.println("Saving results to JDBC database. This may take a while for large datasets...");

        final String url = getUrl(config);
        final String user = config.getUser();
        final String password = config.getPassword();
        final String driver = getDriver(config);

        // Ensure the table exists
        ensureTableExists(url, user, password, driver, schema, table);

        // Prepare the SQL template
        final String insertSql = buildInsertStatement(table, schema);

        // Perform distributed data insertion
        final int columnCount = schema.fieldNames().length;
        results.foreachPartition(partition -> {
            // Load the JDBC driver within the executor context
            Class.forName(driver);

            try (Connection conn = DriverManager.getConnection(url, user, password)) {
                // Disable Auto-Commit to handle transactions manually and allow batching
                conn.setAutoCommit(false);

                try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                    int batchSize = 0;

                    while (partition.hasNext()) {
                        Row row = partition.next();

                        // Map Spark Row values to PreparedStatement placeholders (1-based index)
                        for (int i = 0; i < columnCount; i++) {
                            ps.setObject(i + 1, row.get(i));
                        }

                        ps.addBatch(); // Add record to the local memory buffer

                        // Flush the batch to the database every 1000 records to avoid memory overflow
                        if (++batchSize % 1000 == 0) {
                            ps.executeBatch();
                        }
                    }

                    // Final flush for remaining records in the partition
                    ps.executeBatch();

                    // Commit all changes for the partition
                    conn.commit();
                } catch (SQLException e) {
                    // Rollback transaction in case of failure to maintain data integrity
                    conn.rollback();
                    throw e;
                }
            }
        });
    }

    /**
     * Generates a parameterized INSERT INTO SQL statement dynamically based on the schema.
     * This template is used by executors to perform batch inserts.
     *
     * @param table  The name of the target database table.
     * @param schema The Spark StructType defining the columns.
     * @return A formatted SQL string: INSERT INTO table (col1, col2) VALUES (?, ?).
     */
    private String buildInsertStatement(String table, StructType schema) {
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        String[] fieldNames = schema.fieldNames();

        for (int i = 0; i < fieldNames.length; i++) {
            columns.append(fieldNames[i]);
            placeholders.append("?"); // Add placeholder for PreparedStatement

            // Append separators if it's not the last column
            if (i < fieldNames.length - 1) {
                columns.append(", ");
                placeholders.append(", ");
            }
        }
        return String.format("INSERT INTO %s (%s) VALUES (%s)", table, columns, placeholders);
    }

    /**
     * Verifies if the target table exists and creates it if missing.
     * This logic runs on the Driver to ensure schema consistency before workers start.
     *
     * @param url      The JDBC connection URL.
     * @param user     Database username.
     * @param password Database password.
     * @param driver   The JDBC driver class name.
     * @param schema   The Spark StructType used to derive the SQL schema.
     * @param table    The name of the table to check/create.
     */
    private void ensureTableExists(String url, String user, String password, String driver, StructType schema, String table) {
        try {
            Class.forName(driver);
            try (Connection conn = DriverManager.getConnection(url, user, password);
                 Statement stmt = conn.createStatement()) {

                // Start building the DDL statement
                StringBuilder ddl = new StringBuilder("CREATE TABLE IF NOT EXISTS " + table + " (");
                StructField[] fields = schema.fields();

                for (int i = 0; i < fields.length; i++) {
                    // Map Spark field name and convert DataType to SQL type
                    ddl.append(fields[i].name()).append(" ").append(mapSparkTypeToSql(fields[i].dataType()));

                    if (i < fields.length - 1) {
                        ddl.append(", ");
                    }
                }
                ddl.append(")");

                // Execute the table creation on the database
                stmt.executeUpdate(ddl.toString());
            }
        } catch (Exception e) {
            throw new RuntimeException("Error ensuring table existence for: " + table, e);
        }
    }

    /**
     * Converts Spark DataType objects into equivalent standard SQL type strings.
     *
     * @param type The Spark DataType to convert.
     * @return A string representing the SQL data type (e.g., INT, TEXT, BIGINT).
     */
    private String mapSparkTypeToSql(DataType type) {
        String typeName = type.typeName();

        // Switch based on Spark's internal type name
        switch (typeName) {
            case "integer":     return "INT";
            case "long":        return "BIGINT";
            case "double":      return "DOUBLE PRECISION";
            case "boolean":     return "BOOLEAN";
            case "string":      return "TEXT";
            default:            return "TEXT";
        }
    }
}
