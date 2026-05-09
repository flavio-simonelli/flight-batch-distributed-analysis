package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.PostgresStorageConfig;
import it.uniroma2.sae.factory.FlightRepositoryFactory;
import it.uniroma2.sae.repository.FlightRepository;
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
 * Subclasses only need to implement the {@link #runQuery(FlightRepository, ApplicationConfig)} method
 * to focus strictly on the business logic and data transformations.
 */
public abstract class BaseQuery {

    /**
     * The main execution flow for the query.
     * It handles setup, data loading, executes the specific query logic, and performs cleanup.
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

            // Run query on the loaded dataset
            FlightRepository inputRepository = FlightRepositoryFactory.createInputRepository(config, spark);
            Dataset<Row> results = runQuery(inputRepository, config);

            // Save the results
            if (results != null) {
                FlightRepository outputRepository = FlightRepositoryFactory.createOutputRepository(config, spark);
                
                String target = "";
                if(config.getOutput() instanceof PostgresStorageConfig) {
                    target = ((PostgresStorageConfig) config.getOutput()).getDbtable();
                } else {
                    target = config.getOutput().getResultDirectory(); 
                }
                 
                outputRepository.saveResults(results, target);
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
     * Abstract method that must be implemented by concrete Query classes.
     * Contains the specific Dataset transformations required to answer the query.
     *
     * @param repository the input repository
     * @param config the application configuration
     * @return a Dataset<Row> containing the results of the query
     */
    protected abstract Dataset<Row> runQuery(FlightRepository repository, ApplicationConfig config);
}