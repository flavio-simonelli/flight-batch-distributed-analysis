package it.uniroma2.sae.factory;

import it.uniroma2.sae.config.AppConfig;
import it.uniroma2.sae.config.StorageType;
import it.uniroma2.sae.repository.FlightRepository;
import it.uniroma2.sae.repository.HdfsFlightRepository;
import it.uniroma2.sae.repository.LocalFlightRepository;
import it.uniroma2.sae.repository.S3FlightRepository;
import org.apache.spark.sql.SparkSession;

/**
 * Factory class to create instances of FlightRepository.
 * It uses the AppConfig to decide which implementation to instantiate.
 */
public class FlightRepositoryFactory {

    /**
     * Creates the input repository based on the provided configuration.
     *
     * @param config the AppConfig object
     * @return an instance of FlightRepository
     * @throws IllegalArgumentException if the storage type is not supported
     */
    public static FlightRepository createInputRepository(AppConfig config) {
        AppConfig.StorageConfig inputConfig = config.getInput();
        StorageType inputType = inputConfig.getType();
        String inputPath = inputConfig.getPath();

        SparkSession spark = SparkSession.builder().getOrCreate();

        switch (inputType) {
            case HDFS:

                String hdfsUri = System.getenv("HDFS_URI");
                if (hdfsUri == null) {
                    System.err.println("Input type is HDFS, but HDFS_URI environment variable is not set!");
                    System.exit(1);
                }

                return new HdfsFlightRepository(spark, hdfsUri, inputPath);

            case S3:

                String s3Uri = System.getenv("S3_URI");
                if (s3Uri == null) {
                    System.err.println("Input type is S3, but S3_URI environment variable is not set!");
                    System.exit(1);
                }
                
                return new S3FlightRepository(spark, s3Uri, inputPath);

            case LOCAL:
                return new LocalFlightRepository(spark, inputPath);

            default:
                throw new IllegalArgumentException("Unsupported input type: " + inputType);
        }
    }
}
