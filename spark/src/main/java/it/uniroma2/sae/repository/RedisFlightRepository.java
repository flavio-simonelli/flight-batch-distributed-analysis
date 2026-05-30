package it.uniroma2.sae.repository;

import it.uniroma2.sae.config.RedisStorageConfig;
import it.uniroma2.sae.util.PerformanceMetrics;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link DbFlightRepository} for Redis.
 * Supports saving results and performance metrics.
 */
public class RedisFlightRepository extends DbFlightRepository<RedisStorageConfig> {
    private static final Logger logger = LoggerFactory.getLogger(RedisFlightRepository.class);

    /**
     * Constructs a new RedisFlightRepository with the given SparkSession and Redis configuration.
     *
     * @param spark the SparkSession to be used for data operations
     * @param config the Redis storage configuration
     */
    public RedisFlightRepository(SparkSession spark, RedisStorageConfig config) {
        super(spark, config);
    }

    /**
     * Saves the given results to Redis under the specified table/key.
     *
     * @param results the dataset containing the results to save
     * @param table the name of the Redis table/key where results will be stored
     * @param saveMode the mode for saving
     * @throws IllegalArgumentException if results is null or if the table name is null/empty
     */
    public void saveResults(Dataset<Row> results, String table, SaveMode saveMode) {
        if (results == null) throw new IllegalArgumentException("Results dataset cannot be null.");
        if (table == null || table.isEmpty()) throw new IllegalArgumentException("Target table/key name must be provided for Redis output.");

        logger.debug("Writing results to Redis | table={} | mode={} | host={}:{}", table, saveMode, config.getHostname(), config.getPort());

        DataFrameWriter<Row> writer = results.write()
                .format("org.apache.spark.sql.redis")
                .mode(saveMode)
                .option("host", config.getHostname())
                .option("port", String.valueOf(config.getPort()))
                .option("dbNum", (config.getDatabase() != null && !config.getDatabase().isEmpty()) ? config.getDatabase() : "0")
                .option("table", table);

        // Only set auth if a password is actually provided
        if (config.getPassword() != null && !config.getPassword().isEmpty()) {
            writer.option("auth", config.getPassword());
        }

        writer.save();
    }

    @Override
    public void saveResults(Dataset<Row> results, String table) {
        saveResults(results, table, SaveMode.Overwrite);
    }

    @Override
    public void saveResults(JavaRDD<Row> results, StructType schema, String table) {
        if (results == null) throw new IllegalArgumentException("Results RDD cannot be null.");
        if (schema == null) throw new IllegalArgumentException("Schema cannot be null.");

        Dataset<Row> df = spark.createDataFrame(results, schema);
        saveResults(df, table);
    }

    /**
     * Saves performance metrics to Redis, including the approach used and the query name, along with a timestamp.
     *
     * @param queryName The name of the query (e.g., "query1_monthly_performance")
     * @param approach The Spark API used (e.g., "RDD", "DataFrame", "SQL")
     */
    public void saveMetrics(String queryName, String approach) {
        PerformanceMetrics pm = PerformanceMetrics.getInstance();
        long timestamp = System.currentTimeMillis();

        logger.info("Persisting metrics to Redis | query={} | approach={}", queryName, approach);

        // Save phase-level metrics
        List<Row> phaseRows = new ArrayList<>();
        pm.getPhases().forEach((name, m) -> {
            phaseRows.add(RowFactory.create(queryName, approach, name, m.getWallClockTime(), m.getSparkTime(), timestamp));
        });

        // Define the schema for phase metrics
        StructType phaseSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("query", DataTypes.StringType, false),
                DataTypes.createStructField("approach", DataTypes.StringType, false),
                DataTypes.createStructField("phase", DataTypes.StringType, false),
                DataTypes.createStructField("wallclock-duration", DataTypes.LongType, false),
                DataTypes.createStructField("spark-duration", DataTypes.LongType, false),
                DataTypes.createStructField("timestamp", DataTypes.LongType, false)
        });

        // Save phase metrics to Redis
        String phaseTable = "metrics:phases:" + queryName + ":" + approach;
        if (!phaseRows.isEmpty()) {
            logger.debug("Saving phase metrics | count={}", phaseRows.size());
            saveResults(spark.createDataFrame(phaseRows, phaseSchema), phaseTable, SaveMode.Append);
        }

        // Save stage-level metrics
        List<Row> stageRows = new ArrayList<>();
        pm.getPhases().forEach((phaseName, m) -> {
            m.sparkStages.forEach(s -> {
                stageRows.add(RowFactory.create(queryName, approach, phaseName, s.stageId, s.duration, s.gcTime,
                        s.bytesRead, s.bytesWritten, s.shuffleRead, s.shuffleWrite, timestamp));
            });
        });

        // Define the schema for stage metrics
        if (!stageRows.isEmpty()) {
            StructType stageSchema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("query", DataTypes.StringType, false),
                    DataTypes.createStructField("approach", DataTypes.StringType, false),
                    DataTypes.createStructField("phase", DataTypes.StringType, false),
                    DataTypes.createStructField("stageId", DataTypes.IntegerType, false),
                    DataTypes.createStructField("duration", DataTypes.LongType, false),
                    DataTypes.createStructField("gcTime", DataTypes.LongType, false),
                    DataTypes.createStructField("read", DataTypes.LongType, false),
                    DataTypes.createStructField("written", DataTypes.LongType, false),
                    DataTypes.createStructField("shuffleRead", DataTypes.LongType, false),
                    DataTypes.createStructField("shuffleWrite", DataTypes.LongType, false),
                    DataTypes.createStructField("timestamp", DataTypes.LongType, false)
            });

            // Save stage metrics to Redis
            String stageTable = "metrics:stages:" + queryName + ":" + approach;
            logger.debug("Saving stage metrics | count={}", stageRows.size());
            saveResults(spark.createDataFrame(stageRows, stageSchema), stageTable, SaveMode.Append);
        }

        // Save executor-level metrics
        List<Row> execRows = new ArrayList<>();
        pm.getPhases().forEach((phaseName, m) -> {
            m.executorMetrics.values().forEach(e -> {
                execRows.add(RowFactory.create(queryName, approach, phaseName, e.executorId, e.host, e.taskCount,
                        e.totalRunTime, e.totalCpuTime, e.totalGcTime,
                        e.totalBytesRead, e.totalBytesWritten,
                        e.totalShuffleRead, e.totalShuffleWritten, timestamp));
            });
        });

        // Define the schema for executor metrics
        if (!execRows.isEmpty()) {
            StructType execSchema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("query", DataTypes.StringType, false),
                    DataTypes.createStructField("approach", DataTypes.StringType, false),
                    DataTypes.createStructField("phase", DataTypes.StringType, false),
                    DataTypes.createStructField("execId", DataTypes.StringType, false),
                    DataTypes.createStructField("host", DataTypes.StringType, false),
                    DataTypes.createStructField("tasks", DataTypes.IntegerType, false),
                    DataTypes.createStructField("runTime", DataTypes.LongType, false),
                    DataTypes.createStructField("cpuTime", DataTypes.LongType, false),
                    DataTypes.createStructField("gcTime", DataTypes.LongType, false),
                    DataTypes.createStructField("read", DataTypes.LongType, false),
                    DataTypes.createStructField("written", DataTypes.LongType, false),
                    DataTypes.createStructField("shuffleRead", DataTypes.LongType, false),
                    DataTypes.createStructField("shuffleWrite", DataTypes.LongType, false),
                    DataTypes.createStructField("timestamp", DataTypes.LongType, false)
            });

            // Save executor metrics to Redis
            String execTable = "metrics:executors:" + queryName + ":" + approach;
            logger.debug("Saving executor metrics | count={}", execRows.size());
            saveResults(spark.createDataFrame(execRows, execSchema), execTable, SaveMode.Append);
        }
        logger.info("Metrics successfully persisted to Redis.");
    }
}
