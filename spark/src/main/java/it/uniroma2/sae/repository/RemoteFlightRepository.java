package it.uniroma2.sae.repository;

import org.apache.spark.sql.SparkSession;

/**
 * An abstract base class for repositories that read data from remote storage systems (e.g., HDFS, S3).
 * It introduces a mandatory URI validation step during instantiation and provides a common
 * implementation for building the full remote path.
 */
public abstract class RemoteFlightRepository extends FlightRepository {

    protected final String baseUri;
    protected final String dataPath;

    /**
     * Constructs a new RemoteFlightRepository and validates the provided base URI.
     *
     * @param spark the SparkSession
     * @param baseUri the base URI of the remote storage
     * @param dataPath the path within the remote storage where data is located
     */
    public RemoteFlightRepository(SparkSession spark, String baseUri, String dataPath) {
        super(spark);
        this.baseUri = baseUri;
        this.dataPath = dataPath;
        checkUri(this.baseUri);
    }

    /**
     * Validates that the provided URI is correctly formatted for the specific remote storage implementation.
     * This method is called during object construction.
     *
     * @param uri the URI to validate
     * @throws IllegalArgumentException if the URI is invalid
     */
    protected abstract void checkUri(String uri);

    /**
     * Constructs the full remote path to a specific flight data file.
     * Safely joins the base URI, data path, and filename avoiding double slashes.
     *
     * @param filename the name of the file
     * @return the full remote file path
     */
    @Override
    protected String getFullPath(String filename) {
        // Ensure no double slashes in the path
        String path = dataPath.endsWith("/") ? dataPath.substring(0, dataPath.length() - 1) : dataPath;
        String uri = baseUri.endsWith("/") ? baseUri.substring(0, baseUri.length() - 1) : baseUri;

        return uri + path + filename;
    }
}
