package it.uniroma2.sae.repository;

import it.uniroma2.sae.config.RedisStorageConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * An implementation of {@link DbFlightRepository} for Redis.
 * Supports only saving results.
 */
public class RedisFlightRepository extends DbFlightRepository<RedisStorageConfig> {

    public RedisFlightRepository(SparkSession spark, RedisStorageConfig config) {
        super(spark, config);
    }

    @Override
    public void saveResults(Dataset<Row> results, String table) {
        if (results == null) throw new IllegalArgumentException("Results dataset cannot be null.");
        if (table == null || table.isEmpty()) throw new IllegalArgumentException("Target table/key name must be provided for Redis output.");

        DataFrameWriter<Row> writer = results.write()
                .format("org.apache.spark.sql.redis")
                .mode(SaveMode.Overwrite)
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
    public void saveResults(JavaRDD<Row> results, StructType schema, String table) {
        if (results == null) throw new IllegalArgumentException("Results RDD cannot be null.");
        if (schema == null) throw new IllegalArgumentException("Schema cannot be null.");

        Dataset<Row> df = spark.createDataFrame(results, schema);
        saveResults(df, table);
    }
}
