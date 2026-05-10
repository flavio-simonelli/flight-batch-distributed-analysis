package it.uniroma2.sae.query;

import it.uniroma2.sae.config.AppBackendType;
import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.QueryType;
import it.uniroma2.sae.factory.FlightRepositoryFactory;
import it.uniroma2.sae.repository.FlightRepository;
import it.uniroma2.sae.util.JobTimerListener; // Import the new listener
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

        QueryType query = config.getQueryToRun();
        if (query == null) throw new IllegalArgumentException("queryToRun is not defined in config.yml. Please choose monthly_performance, arrival_delay_ranking, or hourly_delay_percentiles");
        String queryName = query.name().toLowerCase();

        try {
            spark = SparkSession.builder()
                    .appName(config.getAppName())
                    .master(config.getSparkCluster().getMasterUri())
                    .getOrCreate();

            spark.sparkContext().setLogLevel("WARN");

            // Add the custom SparkListener for job timing
            JobTimerListener timer = new JobTimerListener();
            spark.sparkContext().addSparkListener(timer);

            // Instantiate repositories
            FlightRepository inputRepository = FlightRepositoryFactory.createInputRepository(config, spark);
            FlightRepository outputRepository = FlightRepositoryFactory.createOutputRepository(config, spark);

            AppBackendType backend = config.getAppBackend();
            if (backend == null) throw new IllegalArgumentException("appBackend is not defined in config.yml. Please choose rdd, dataframe, or sql.");

            // Determine output target name based on query and backend (subclasses may extend it).
            String baseTargetName = buildBaseTargetName(config);
            String fullTargetName = config.getOutput().getResultDirectory() + baseTargetName;


            long endLoad = 0;
            long loadWallTime = 0;
            long loadMaxJob = 0;

            long startSave = 0;
            long endSave = 0;
            long saveWallTime = 0;
            long saveMaxJob = 0;

            long startTime = System.currentTimeMillis();
            long startProcess = System.currentTimeMillis();

            // Execute the query using the configured backend API
            switch (backend) {
                case DATAFRAME:
                    List<Dataset<Row>> dfResults = runQueryDataFrame(inputRepository, config);

                    endLoad = System.currentTimeMillis();
                    loadMaxJob = timer.getMaxJobDuration();
                    timer.reset();

                    if (dfResults == null) break;

                    startSave = System.currentTimeMillis();

                    for (int i = 0; i < dfResults.size(); i++) {
                        String currentTarget = dfResults.size() > 1 ? fullTargetName + "_" + (i + 1) : fullTargetName;
                        outputRepository.saveResults(dfResults.get(i), currentTarget);
                    }

                    endSave = System.currentTimeMillis();
                    saveMaxJob = timer.getMaxJobDuration();

                    break;

                case SQL:
                    List<Dataset<Row>> sqlResults = runQuerySQL(inputRepository, config, spark);

                    endLoad = System.currentTimeMillis();
                    loadMaxJob = timer.getMaxJobDuration();
                    timer.reset();

                    if (sqlResults == null) break;

                    startSave = System.currentTimeMillis();

                    for (int i = 0; i < sqlResults.size(); i++) {
                        String currentTarget = sqlResults.size() > 1 ? fullTargetName + "_" + (i + 1) : fullTargetName;
                        outputRepository.saveResults(sqlResults.get(i), currentTarget);
                    }

                    endSave = System.currentTimeMillis();
                    saveMaxJob = timer.getMaxJobDuration();

                    break;

                case RDD:
                    List<Tuple2<JavaRDD<Row>, StructType>> rddResultsWithSchema = runQueryRDD(inputRepository, config);

                    endLoad = System.currentTimeMillis();
                    loadMaxJob = timer.getMaxJobDuration();
                    timer.reset();

                    if (rddResultsWithSchema == null) break;

                    startSave = System.currentTimeMillis();

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

                    endSave = System.currentTimeMillis();
                    saveMaxJob = timer.getMaxJobDuration();

                    break;

                default:
                    throw new UnsupportedOperationException("Backend " + backend + " is not supported.");
            }

            loadWallTime = endLoad - startProcess;
            saveWallTime = endSave - startSave;

            long endTime = System.currentTimeMillis();

            System.out.println("--- PERFORMANCE REPORT: " + queryName + " ---");
            printPhase("LOADING", loadWallTime, loadMaxJob);
            printPhase("PROCESSING & SAVING", saveWallTime, saveMaxJob);
            System.out.println("TOTAL WALL TIME: " + (endTime - startTime) + " ms");

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
     * Print time metrics for a specific phase.
     *
     * @param phaseName name of the phase
     * @param wallTime total wall time for the phase
     * @param maxJobTime max time for any job in the phase
     */
    private void printPhase(String phaseName, long wallTime, long maxJobTime) {
        System.out.printf("Phase: %s%n", phaseName);
        System.out.printf("  - Wall Clock Time: %d ms%n", wallTime);
        System.out.printf("  - Longest Spark Job in this phase: %d ms%n", maxJobTime);
    }

    /**
     * Executes the query using the Spark DataFrame API.
     * By default, throws an exception. Must be overridden by subclasses supporting this backend.
     *
     * @param repository the repository used to load flight data
     * @param config the application configuration containing input/output paths
     * @return a list containing Datasets with the query results
     */
    protected abstract List<Dataset<Row>> runQueryDataFrame(FlightRepository repository, ApplicationConfig config);

    /**
     * Executes the query using Spark SQL.
     * By default, throws an exception. Must be overridden by subclasses supporting this backend.
     * Note: The SparkSession is provided here because SQL queries typically require registering temporary views.
     *
     * @param repository the repository used to load flight data
     * @param config the application configuration containing input/output paths
     * @param spark the active SparkSession to run SQL commands
     * @return a list containing Datasets with the query results
     */
    protected abstract List<Dataset<Row>> runQuerySQL(FlightRepository repository, ApplicationConfig config, SparkSession spark);

    /**
     * Executes the query using the Spark RDD API.
     * By default, throws an exception. Must be overridden by subclasses supporting this backend.
     *
     * @param repository the repository used to load flight data
     * @param config the application configuration containing input/output paths
     * @return a list containing RDDs with their corresponding schemas
     */
    protected abstract List<Tuple2<JavaRDD<Row>, StructType>> runQueryRDD(FlightRepository repository, ApplicationConfig config);

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
