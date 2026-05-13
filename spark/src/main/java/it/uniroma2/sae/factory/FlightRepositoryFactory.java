package it.uniroma2.sae.factory;

import it.uniroma2.sae.config.*;
import it.uniroma2.sae.repository.*;
import org.apache.spark.sql.SparkSession;

/**
 * Factory class to create instances of FlightRepository.
 * It uses the ApplicationConfig to decide which implementation to instantiate.
 */
public class FlightRepositoryFactory {

    /**
     * Creates the input repository based on the provided configuration.
     *
     * @param config the ApplicationConfig object
     * @param spark the initialized SparkSession
     * @return an instance of FlightRepository
     * @throws IllegalArgumentException if the storage type is not supported or configuration is invalid
     */
    public static FlightRepository createInputRepository(ApplicationConfig config, SparkSession spark) {
        StorageConfig inputConfig = config.getInput();
        return createRepository(inputConfig, spark);
    }

    /**
     * Creates the output repository based on the provided configuration.
     *
     * @param config the ApplicationConfig object
     * @param spark the initialized SparkSession
     * @return an instance of FlightRepository
     * @throws IllegalArgumentException if the storage type is not supported or configuration is invalid
     */
    public static FlightRepository createOutputRepository(ApplicationConfig config, SparkSession spark) {
        StorageConfig outputConfig = config.getOutput();
        return createRepository(outputConfig, spark);
    }

    private static FlightRepository createRepository(StorageConfig storageConfig, SparkSession spark) {
        StorageType storageType = storageConfig.getType();
        String storagePath = storageConfig.getPath();

        switch (storageType) {
            case HDFS:
                if (!(storageConfig instanceof RemoteStorageConfig)) {
                    throw new IllegalArgumentException("Configuration mismatch: Expected RemoteStorageConfig for HDFS output type.");
                }
                RemoteStorageConfig hdfsConfig = (RemoteStorageConfig) storageConfig;
                String hdfsUri = hdfsConfig.getUri();
                if (hdfsUri == null || hdfsUri.isEmpty()) {
                    throw new IllegalArgumentException("HDFS URI is not defined in compose-config.yml for HDFS input type.");
                }
                return new HdfsFlightRepository(spark, hdfsUri, storagePath);

            case S3:
                if(!(storageConfig instanceof RemoteStorageConfig)) {
                    throw new IllegalArgumentException("Configuration mismatch: Expected RemoteStorageConfig for S3 output type.");
                }
                RemoteStorageConfig s3Config = (RemoteStorageConfig) storageConfig;
                String s3Uri = s3Config.getUri();
                if (s3Uri == null || s3Uri.isEmpty()) {
                    throw new IllegalArgumentException("S3 URI is not defined in compose-config.yml for S3 input type.");
                }
                return new S3FlightRepository(spark, s3Uri, storagePath);

            case LOCAL:
                return new LocalFlightRepository(spark, storagePath);

            case POSTGRES:
                if (!(storageConfig instanceof JdbcStorageConfig)) {
                     throw new IllegalArgumentException("Configuration mismatch: Expected JdbcStorageConfig for POSTGRES output type.");
                }
                JdbcStorageConfig jdbcConfig = (JdbcStorageConfig) storageConfig;
                return new PostgresFlightRepository(spark, jdbcConfig);

            case MONGODB:
                if (!(storageConfig instanceof MongoStorageConfig)) {
                    throw new IllegalArgumentException("Configuration mismatch: Expected MongoStorageConfig for MONGODB output type.");
                }
                return new MongoFlightRepository(spark, (MongoStorageConfig) storageConfig);

            case REDIS:
                if (!(storageConfig instanceof RedisStorageConfig)) {
                    throw new IllegalArgumentException("Configuration mismatch: Expected RedisStorageConfig for REDIS output type.");
                }
                return new RedisFlightRepository(spark, (RedisStorageConfig) storageConfig);

            case HBASE:
                if (!(storageConfig instanceof HBaseStorageConfig)) {
                    throw new IllegalArgumentException("Configuration mismatch: Expected HBaseStorageConfig for HBASE output type.");
                }
                return new HBaseFlightRepository(spark, (HBaseStorageConfig) storageConfig);

            default:
                throw new IllegalArgumentException("Unsupported storage type: " + storageType);
        }
    }
}
