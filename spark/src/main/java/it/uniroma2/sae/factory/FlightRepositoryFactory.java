package it.uniroma2.sae.factory;

import it.uniroma2.sae.config.AppConfig;
import it.uniroma2.sae.config.StorageType;
import it.uniroma2.sae.repository.FlightRepository;
import it.uniroma2.sae.repository.HdfsFlightRepository;
import org.apache.spark.sql.SparkSession;


public class FlightRepositoryFactory {

    public static FlightRepository createRepository(AppConfig config) {
        AppConfig.StorageConfig inputConfig = config.getInput();
        StorageType inputType = inputConfig.getType();
        String inputPath = inputConfig.getPath();

        switch (inputType) {
            case HDFS:

                String hdfsUri = System.getenv("HDFS_URI");
                if (hdfsUri == null) {
                    System.err.println("Input type is HDFS, but HDFS_URI environment variable is not set!");
                    System.exit(1);
                }

                SparkSession spark = SparkSession.builder()
                        // .config("spark.hadoop.fs.defaultFS", hdfsUri)
                        .getOrCreate();

                return new HdfsFlightRepository(spark, hdfsUri, inputPath);

            default:
                throw new IllegalArgumentException("Unsupported input type: " + inputType);
        }
    }
}
