package it.uniroma2.sae.query;

import it.uniroma2.sae.config.AppBackendType;
import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.factory.FlightRepositoryFactory;
import it.uniroma2.sae.repository.FlightRepository;
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
        try {
            spark = SparkSession.builder()
                    .appName(config.getAppName())
                    .master(config.getSparkCluster().getMasterUri())
                    .getOrCreate();

            spark.sparkContext().setLogLevel("WARN");

            // Instantiate repositories
            FlightRepository inputRepository = FlightRepositoryFactory.createInputRepository(config, spark);
            FlightRepository outputRepository = FlightRepositoryFactory.createOutputRepository(config, spark);

            AppBackendType backend = config.getAppBackend();
            if (backend == null) {
                throw new IllegalArgumentException("appBackend is not defined in config.yml. Please choose rdd, dataframe, or sql.");
            }

            // Determine output target name based on query and backend
            String queryName = this.getClass().getSimpleName();
            String backendName = backend.name().toLowerCase();
            String baseTargetName = String.format("%s_%s", queryName, backendName);
            String fullTargetName = config.getOutput().getResultDirectory() + baseTargetName;

            // Execute the query using the configured backend API
            switch (backend) {
                case DATAFRAME:
                    List<Dataset<Row>> dfResults = runQueryDataFrame(inputRepository, config);
                    if (dfResults == null) break;

                    for (int i = 0; i < dfResults.size(); i++) {
                        String currentTarget = dfResults.size() > 1 ? fullTargetName + "_" + (i + 1) : fullTargetName;
                        outputRepository.saveResults(dfResults.get(i), currentTarget);
                    }
                    break;

                case SQL:
                    List<Dataset<Row>> sqlResults = runQuerySQL(inputRepository, config, spark);
                    if (sqlResults == null) break;

                    for (int i = 0; i < sqlResults.size(); i++) {
                        String currentTarget = sqlResults.size() > 1 ? fullTargetName + "_" + (i + 1) : fullTargetName;
                        outputRepository.saveResults(sqlResults.get(i), currentTarget);
                    }
                    break;

                case RDD:
                    List<Tuple2<JavaRDD<Row>, StructType>> rddResultsWithSchema = runQueryRDD(inputRepository, config);
                    if (rddResultsWithSchema == null) break;

                    for (int i = 0; i < rddResultsWithSchema.size(); i++) {
                        Tuple2<JavaRDD<Row>, StructType> rddTuple = rddResultsWithSchema.get(i);
                        JavaRDD<Row> rdd = rddTuple._1();
                        StructType schema = rddTuple._2();

                        // Avoid to use .empty() for performance reasons
                        // if (rdd.isEmpty()) continue;

                        String currentTarget = rddResultsWithSchema.size() > 1 ? fullTargetName + "_" + (i + 1) : fullTargetName;
                        // Don't use depreciated method
                        // outputRepository.saveResults(JavaSparkContext.fromSparkContext(spark.sparkContext()), rdd, schema, currentTarget);
                        //
                        // Use version with internal conversion
                        outputRepository.saveResults(rdd, schema, currentTarget);
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
}
