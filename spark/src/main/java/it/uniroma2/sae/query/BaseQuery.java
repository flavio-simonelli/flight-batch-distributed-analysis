package it.uniroma2.sae.query;

import it.uniroma2.sae.config.AppBackendType;
import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.QueryType;
import it.uniroma2.sae.factory.FlightRepositoryFactory;
import it.uniroma2.sae.repository.FlightRepository;
import it.uniroma2.sae.util.JobTimerListener; // Import the new listener
import it.uniroma2.sae.util.PerformanceMetrics;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.List;

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

        PerformanceMetrics metrics = PerformanceMetrics.getInstance();
        metrics.reset();

        QueryType query = config.getQueryToRun();
        if (query == null) throw new IllegalArgumentException("queryToRun is not defined. Please choose monthly_performance, arrival_delay_ranking, or hourly_delay_percentiles via config or CLI.");
        String queryName = query.name().toLowerCase();

        try {
            SparkSession.Builder builder = SparkSession.builder()
                    .appName(config.getAppName());

            // If master is provided in config, use it (typically for local/docker).
            // If not, Spark will expect it from spark-submit --master (typical for EMR/YARN).
            if (config.getSparkCluster() != null && config.getSparkCluster().getMaster() != null) {
                builder.master(config.getSparkCluster().getMasterUri());
            }

            spark = builder.getOrCreate();

            spark.sparkContext().setLogLevel("WARN");

            // OPTIMIZATION: Reduce shuffle partitions for small/medium datasets.
            // The default is 200, which is overkill for this project and causes massive task overhead.
            spark.conf().set("spark.sql.shuffle.partitions", "8"); 
            // Enable Adaptive Query Execution to further optimize shuffles at runtime.
            spark.conf().set("spark.sql.adaptive.enabled", "true");

            // Add the custom SparkListener for job timing
            JobTimerListener timer = new JobTimerListener();
            spark.sparkContext().addSparkListener(timer);

            // Instantiate repositories
            FlightRepository inputRepository = FlightRepositoryFactory.createInputRepository(config, spark);
            FlightRepository outputRepository = FlightRepositoryFactory.createOutputRepository(config, spark);

            AppBackendType backend = config.getAppBackend();
            if (backend == null) throw new IllegalArgumentException("appBackend is not defined. Please choose rdd, dataframe, or sql via config or CLI.");

            // Determine output target name based on query and backend (subclasses may extend it).
            String baseTargetName = buildBaseTargetName(config);
            String fullTargetName = config.getOutput().getResultDirectory() + baseTargetName;

            List<Dataset<Row>> dfResults = null;
            List<Tuple2<JavaRDD<Row>, StructType>> rddResultsWithSchema = null;

            // Load dataset
            metrics.startPhase("PLANNING");
            Dataset<Row> flights = loadData(inputRepository, config);
            metrics.stopPhase();

            // Execute the query using the configured backend API
            metrics.startPhase("EXECUTION");
            switch (backend) {
                case DATAFRAME:
                    dfResults = runQueryDataFrame(flights, config);
                    break;

                case SQL:
                    dfResults = runQuerySQL(flights, config, spark);
                    break;

                case RDD:
                    rddResultsWithSchema = runQueryRDD(flights, config);
                    break;

                default:
                    throw new UnsupportedOperationException("Backend " + backend + " is not supported.");
            }

            if(dfResults != null) {
                for (int i = 0; i < dfResults.size(); i++) {
                    String currentTarget = dfResults.size() > 1 ? fullTargetName + "_" + (i + 1) : fullTargetName;
                    outputRepository.saveResults(dfResults.get(i), currentTarget);
                }
            } else if(rddResultsWithSchema != null) {
                for (int i = 0; i < rddResultsWithSchema.size(); i++) {
                    Tuple2<JavaRDD<Row>, StructType> rddTuple = rddResultsWithSchema.get(i);
                    JavaRDD<Row> rdd = rddTuple._1();
                    StructType schema = rddTuple._2();

                    // Avoid to use .empty() for performance reasons
                    // if (rdd.isEmpty()) continue;

                    String currentTarget = rddResultsWithSchema.size() > 1 ? fullTargetName + "_" + (i + 1) : fullTargetName;
                    // Don't use deprecated method
                    // outputRepository.saveResults(JavaSparkContext.fromSparkContext(spark.sparkContext()), rdd, schema, currentTarget);
                    //
                    // Use version with internal conversion
                    outputRepository.saveResults(rdd, schema, currentTarget);
                }
            }
            metrics.stopPhase();


            metrics.printReport(queryName);

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
     * Load the specific dataset using the Spark DataFrame API.
     * By default, throws an exception. Must be overridden by subclasses supporting this backend.
     *
     * @param repository the repository used to load flight data
     * @param config the application configuration containing input/output paths
     * @return a Dataset with the query results
     */
    protected abstract Dataset<Row> loadData(FlightRepository repository, ApplicationConfig config);

    /**
     * Executes the query using the Spark DataFrame API.
     * By default, throws an exception. Must be overridden by subclasses supporting this backend.
     *
     * @param dataset the dataset to query
     * @param config the application configuration containing input/output paths
     * @return a list containing Datasets with the query results
     */
    protected abstract List<Dataset<Row>> runQueryDataFrame(Dataset<Row> dataset, ApplicationConfig config);

    /**
     * Executes the query using Spark SQL.
     * By default, throws an exception. Must be overridden by subclasses supporting this backend.
     * Note: The SparkSession is provided here because SQL queries typically require registering temporary views.
     *
     * @param dataset the dataset to query
     * @param config the application configuration containing input/output paths
     * @param spark the active SparkSession to run SQL commands
     * @return a list containing Datasets with the query results
     */
    protected abstract List<Dataset<Row>> runQuerySQL(Dataset<Row> dataset, ApplicationConfig config, SparkSession spark);

    /**
     * Executes the query using the Spark RDD API.
     * By default, throws an exception. Must be overridden by subclasses supporting this backend.
     *
     * @param dataset the dataset to query
     * @param config the application configuration containing input/output paths
     * @return a list containing RDDs with their corresponding schemas
     */
    protected abstract List<Tuple2<JavaRDD<Row>, StructType>> runQueryRDD(Dataset<Row> dataset, ApplicationConfig config);

    /**
     * Builds the base name used to derive output target identifiers (table names, file names).
     * Default form: {@code <queryName>_<backendName>}. Subclasses can override to append further
     * qualifiers (e.g., the chosen percentile algorithm) so that runs with different parameters
     * produce distinct outputs and don't overwrite each other.
     */
    protected String buildBaseTargetName(ApplicationConfig config) {
        String queryName = config.getQueryToRun().name().toLowerCase();
        String backendName = config.getAppBackend().name().toLowerCase();
        return String.format("%s_%s", queryName, backendName);
    }
}
