package it.uniroma2.sae.factory;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.RemoteStorageConfig;
import it.uniroma2.sae.config.StorageConfig;
import it.uniroma2.sae.config.StorageType;
import it.uniroma2.sae.repository.FlightRepository;
import it.uniroma2.sae.repository.HdfsFlightRepository;
import it.uniroma2.sae.repository.LocalFlightRepository;
import it.uniroma2.sae.repository.S3FlightRepository;
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
        return FlightRepositoryFactory.createRepository(inputConfig, spark);
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
        return FlightRepositoryFactory.createRepository(outputConfig, spark);
    }

    /**
     * Creates the repository based on the provided configuration.
     *
     * @param storageConfig the StorageConfig object
     * @param spark the initialized SparkSession
     * @return an instance of FlightRepository
     * @throws IllegalArgumentException if the storage type is not supported or configuration is invalid
     */
    private static FlightRepository createRepository(StorageConfig storageConfig, SparkSession spark) {
        StorageType storageType = storageConfig.getType();
        String storagePath = storageConfig.getPath();

        switch (storageType) {
            case HDFS:

                RemoteStorageConfig hdfsConfig = (RemoteStorageConfig) storageConfig;
                String hdfsUri = hdfsConfig.getUri();
                if (hdfsUri == null || hdfsUri.isEmpty()) {
                    throw new IllegalArgumentException("HDFS URI is not defined in config.yml for HDFS input type.");
                }
                return new HdfsFlightRepository(spark, hdfsUri, storagePath);

            case S3:

                RemoteStorageConfig s3Config = (RemoteStorageConfig) storageConfig;
                String s3Uri = s3Config.getUri();
                if (s3Uri == null || s3Uri.isEmpty()) {
                    throw new IllegalArgumentException("S3 URI is not defined in config.yml for S3 input type.");
                }
                return new S3FlightRepository(spark, s3Uri, storagePath);

            case LOCAL:
                return new LocalFlightRepository(spark, storagePath);

            default:
                throw new IllegalArgumentException("Unsupported input type: " + storagePath);
        }
    }
}
