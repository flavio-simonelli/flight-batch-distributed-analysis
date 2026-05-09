package it.uniroma2.sae.query;

import it.uniroma2.sae.config.AppConfig;
import it.uniroma2.sae.factory.FlightRepositoryFactory;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Base abstract class for all Spark queries in the project.
 * It encapsulates the common boilerplate code required to bootstrap a Spark job:
 * 1. Initializing the SparkSession with cluster settings.
 * 2. Instantiating the appropriate input repository via the Factory.
 * 3. Loading the initial raw dataset using the target filename from configuration.
 * 4. Managing the SparkSession lifecycle (stopping it safely).
 * 
 * Subclasses only need to implement the {@link #runQuery(Dataset, AppConfig)} method
 * to focus strictly on the business logic and data transformations.
 */
public abstract class BaseQuery {

    /**
     * The main execution flow for the query.
     * It handles setup, data loading, executes the specific query logic, and performs cleanup.
     *
     * @param config the application configuration loaded at startup
     */
    public void execute(AppConfig config) {
        SparkSession spark = null;
        try {
            spark = SparkSession.builder()
                    .appName(config.getAppName())
                    .master(config.getSparkCluster().getMasterUri())
                    .getOrCreate();

            spark.sparkContext().setLogLevel("WARN");

            FlightRepository repository = FlightRepositoryFactory.createInputRepository(config);
            
            String datasetFilename = config.getDatasetFilename();
            if (datasetFilename == null || datasetFilename.isEmpty()) {
                throw new IllegalArgumentException("datasetFilename is not defined in config.yml");
            }
            
            Dataset<RawFlight> flights = repository.getFlights(datasetFilename);
            runQuery(flights, config);

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
     * @param flights the raw dataset loaded from the configured storage
     * @param config the application configuration
     */
    protected abstract void runQuery(Dataset<RawFlight> flights, AppConfig config);
}
