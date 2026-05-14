package it.uniroma2.sae.repository;

import it.uniroma2.sae.config.RedisStorageConfig;
import it.uniroma2.sae.util.PerformanceMetrics;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link DbFlightRepository} for Redis.
 * Supports saving results and performance metrics.
 */
public class RedisFlightRepository extends DbFlightRepository<RedisStorageConfig> {

    public RedisFlightRepository(SparkSession spark, RedisStorageConfig config) {
        super(spark, config);
    }

    public void saveResults(Dataset<Row> results, String table, SaveMode saveMode) {
        if (results == null) throw new IllegalArgumentException("Results dataset cannot be null.");
        if (table == null || table.isEmpty()) throw new IllegalArgumentException("Target table/key name must be provided for Redis output.");

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
     * Salva le metriche di performance su Redis, includendo l'approccio utilizzato (es. RDD, DF, SQL)
     * per permettere il confronto su Grafana.
     *
     * @param queryName Il nome della query (es. "query1_monthly_performance")
     * @param approach L'API di Spark utilizzata (es. "RDD", "DataFrame", "SQL")
     */
    public void saveMetrics(String queryName, String approach) {
        PerformanceMetrics pm = PerformanceMetrics.getInstance();
        long timestamp = System.currentTimeMillis();

        // 1. SAVE PHASE METRICS
        List<Row> phaseRows = new ArrayList<>();
        pm.getPhases().forEach((name, m) -> {
            phaseRows.add(RowFactory.create(queryName, approach, name, m.getWallClockTime(), m.getSparkTime(), timestamp));
        });

        StructType phaseSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("query", DataTypes.StringType, false),
                DataTypes.createStructField("approach", DataTypes.StringType, false), // NUOVO CAMPO
                DataTypes.createStructField("phase", DataTypes.StringType, false),
                DataTypes.createStructField("wallclock-duration", DataTypes.LongType, false),
                DataTypes.createStructField("spark-duration", DataTypes.LongType, false),
                DataTypes.createStructField("timestamp", DataTypes.LongType, false)
        });
        String phaseTable = "metrics:phases:" + queryName + ":" + approach;
        saveResults(spark.createDataFrame(phaseRows, phaseSchema), phaseTable, SaveMode.Append);

        // 2. SAVE STAGE METRICS
        List<Row> stageRows = new ArrayList<>();
        pm.getPhases().forEach((phaseName, m) -> {
            m.sparkStages.forEach(s -> {
                stageRows.add(RowFactory.create(queryName, approach, phaseName, s.stageId, s.duration, s.gcTime,
                        s.bytesRead, s.bytesWritten, s.shuffleRead, s.shuffleWrite, timestamp));
            });
        });

        if (!stageRows.isEmpty()) {
            StructType stageSchema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("query", DataTypes.StringType, false),
                    DataTypes.createStructField("approach", DataTypes.StringType, false), // NUOVO CAMPO
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
            String stageTable = "metrics:stages:" + queryName + ":" + approach;
            saveResults(spark.createDataFrame(stageRows, stageSchema), stageTable, SaveMode.Append);
        }

        // 3. SAVE EXECUTOR METRICS
        List<Row> execRows = new ArrayList<>();
        pm.getPhases().forEach((phaseName, m) -> {
            m.executorMetrics.values().forEach(e -> {
                execRows.add(RowFactory.create(queryName, approach, phaseName, e.executorId, e.host, e.taskCount,
                        e.totalRunTime, e.totalCpuTime, e.totalGcTime,
                        e.totalBytesRead, e.totalBytesWritten,
                        e.totalShuffleRead, e.totalShuffleWritten, timestamp));
            });
        });

        if (!execRows.isEmpty()) {
            StructType execSchema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("query", DataTypes.StringType, false),
                    DataTypes.createStructField("approach", DataTypes.StringType, false), // NUOVO CAMPO
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
            String execTable = "metrics:executors:" + queryName + ":" + approach;
            saveResults(spark.createDataFrame(execRows, execSchema), execTable, SaveMode.Append);
        }
    }
}
