package it.uniroma2.sae.repository;

import org.apache.spark.sql.SparkSession;

/**
 * An implementation of {@link RemoteFlightRepository} that reads flight data from an AWS S3 bucket.
 * This repository requires Spark to be configured with S3 credentials and the necessary hadoop-aws dependencies.
 */
public class S3FlightRepository extends RemoteFlightRepository {

    /**
     * Constructs a new S3FlightRepository.
     *
     * @param spark the SparkSession
     * @param s3Uri the base URI of the S3 bucket (e.g., "s3a://my-bucket")
     * @param dataPath the sub-path within the bucket where data is stored
     */
    public S3FlightRepository(SparkSession spark, String s3Uri, String dataPath) {
        super(spark, s3Uri, dataPath);
    }

    /**
     * Checks if the provided URI is a valid S3 URI.
     * It must start with "s3://", "s3a://", or "s3n://".
     *
     * @param uri the S3 URI to validate
     * @throws IllegalArgumentException if the URI is null, empty, or invalid
     */
    @Override
    protected void checkUri(String uri) {
        if (uri == null || uri.isEmpty()) {
            throw new IllegalArgumentException("S3 URI cannot be null or empty");
        }
        
        if (!uri.startsWith("s3://") && !uri.startsWith("s3a://") && !uri.startsWith("s3n://")) {
            throw new IllegalArgumentException("Invalid S3 URI: must start with 's3://', 's3a://' or 's3n://'. Provided: " + uri);
        }
    }
}
