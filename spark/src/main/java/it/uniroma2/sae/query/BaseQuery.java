package it.uniroma2.sae.query;

import it.uniroma2.sae.config.AppBackendType;
import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.PostgresStorageConfig;
import it.uniroma2.sae.factory.FlightRepositoryFactory;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Base abstract class for all Spark queries in the project.
 * It encapsulates the common boilerplate code required to bootstrap a Spark job:
 * 1. Initializing the SparkSession with cluster settings.
 * 2. Instantiating the appropriate input repository via the Factory.
 * 3. Managing the SparkSession lifecycle (stopping it safely).
 *
 * Subclasses must implement the execution methods (runQueryDataFrame, runQueryRDD, runQuerySQL)
 * depending on which backend APIs they support.
 */
public abstract class BaseQuery {

    /**
     * The main execution flow for the query.
     * It handles setup, executes the specific query logic based on the configured backend, and performs cleanup.
     *
     * @param config the application configuration loaded at startup
     */
    public void execute(ApplicationConfig config) {
        SparkSession spark = null;
        try {
            spark = SparkSession.builder()
                    .appName(config.getAppName())
                    .master(config.getSparkCluster().getMasterUri())
                    .getOrCreate();

            spark.sparkContext().setLogLevel("WARN");

            // Instantiate repositories
            FlightRepository inputRepository = FlightRepositoryFactory.createInputRepository(config, spark);
            FlightRepository outputRepository = FlightRepositoryFactory.createOutputRepository(config, spark);

            // Determine output target (table name for Postgres, directory for HDFS/S3/Local)
            String target = "";
            if (config.getOutput() instanceof PostgresStorageConfig) {
                target = ((PostgresStorageConfig) config.getOutput()).getDbtable();
            } else {
                target = config.getOutput().getResultDirectory();
            }

            AppBackendType backend = config.getAppBackend();
            if (backend == null) {
                throw new IllegalArgumentException("appBackend is not defined in config.yml. Please choose rdd, dataframe, or sql.");
            }

            // Execute the query using the configured backend API
            switch (backend) {
                case DATAFRAME:
                    Dataset<Row> dfResults = runQueryDataFrame(inputRepository, config);
                    if (dfResults != null) {
                        outputRepository.saveResults(dfResults, target);
                    }
                    break;

                case SQL:
                    Dataset<Row> sqlResults = runQuerySQL(inputRepository, config, spark);
                    if (sqlResults != null) {
                        outputRepository.saveResults(sqlResults, target);
                    }
                    break;

                case RDD:
                    JavaRDD<Row> rddResults = runQueryRDD(inputRepository, config);
                    if (rddResults != null && !rddResults.isEmpty()) {
                        // To save results using the existing repository logic (which expects Dataset<Row>),
                        // we convert the RDD back to a Dataset. We need the schema from the first row.
                        // Dataset<Row> convertedResults = spark.createDataFrame(rddResults, rddResults.first().schema());
                        // outputRepository.saveResults(convertedResults, target);
                    }
                    break;

                default:
                    throw new UnsupportedOperationException("Backend " + backend + " is not supported.");
            }

        } catch (Exception e) {
            System.err.println("Fatal error during query execution:");
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }

    /**
     * Executes the query using the Spark DataFrame API.
     * By default, throws an exception. Must be overridden by subclasses supporting this backend.
     */
    protected Dataset<Row> runQueryDataFrame(FlightRepository repository, ApplicationConfig config) {
        throw new UnsupportedOperationException("DataFrame backend is not implemented for this query.");
    }

    /**
     * Executes the query using Spark SQL.
     * By default, throws an exception. Must be overridden by subclasses supporting this backend.
     * Note: The SparkSession is provided here because SQL queries typically require registering temporary views.
     */
    protected Dataset<Row> runQuerySQL(FlightRepository repository, ApplicationConfig config, SparkSession spark) {
        throw new UnsupportedOperationException("SQL backend is not implemented for this query.");
    }

    /**
     * Executes the query using the Spark RDD API.
     * By default, throws an exception. Must be overridden by subclasses supporting this backend.
     */
    protected JavaRDD<Row> runQueryRDD(FlightRepository repository, ApplicationConfig config) {
        throw new UnsupportedOperationException("RDD backend is not implemented for this query.");
    }
}
