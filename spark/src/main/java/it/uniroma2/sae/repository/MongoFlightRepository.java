package it.uniroma2.sae.repository;

import it.uniroma2.sae.config.MongoStorageConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * An implementation of {@link DbFlightRepository} for MongoDB.
 * Supports only saving results.
 */
public class MongoFlightRepository extends DbFlightRepository<MongoStorageConfig> {

    public MongoFlightRepository(SparkSession spark, MongoStorageConfig config) {
        super(spark, config);
    }

    @Override
    public void saveResults(Dataset<Row> results, String collection) {
        if (results == null) throw new IllegalArgumentException("Results dataset cannot be null.");
        
        String targetCollection = (collection != null && !collection.isEmpty()) ? collection : config.getCollection();
        if (targetCollection == null || targetCollection.isEmpty()) {
            throw new IllegalArgumentException("Target collection must be provided for MongoDB output.");
        }

        // Removed explicit serverApi.version to avoid casing-related duplication warnings.
        // If needed, it can be passed via the connection URI in MongoStorageConfig.
        results.write()
                .format("mongodb")
                .mode(SaveMode.Overwrite)
                .option("connection.uri", config.getConnectionUri())
                .option("database", config.getDatabase())
                .option("collection", targetCollection)
                .save();
    }

    @Override
    public void saveResults(JavaRDD<Row> results, StructType schema, String collection) {
        if (results == null) throw new IllegalArgumentException("Results RDD cannot be null.");
        if (schema == null) throw new IllegalArgumentException("Schema cannot be null.");

        Dataset<Row> df = spark.createDataFrame(results, schema);
        saveResults(df, collection);
    }
}
