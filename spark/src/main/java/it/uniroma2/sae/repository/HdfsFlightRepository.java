package it.uniroma2.sae.repository;

import org.apache.spark.sql.SparkSession;

/**
 * An implementation of {@link RemoteFlightRepository} that reads flight data from a Hadoop Distributed File System.
 */
public class HdfsFlightRepository extends RemoteFlightRepository {

    /**
     * Constructs a new HdfsFlightRepository.
     *
     * @param spark the SparkSession
     * @param hdfsUri the base URI of the HDFS cluster (e.g., "hdfs://namenode:8020")
     * @param dataPath the path within HDFS where data is stored
     */
    public HdfsFlightRepository(SparkSession spark, String hdfsUri, String dataPath) {
        super(spark, hdfsUri, dataPath);
    }

    /**
     * Checks if the provided URI is a valid HDFS URI.
     * It must start with "hdfs://".
     *
     * @param uri the HDFS URI to validate
     * @throws IllegalArgumentException if the URI is null, empty, or invalid
     */
    @Override
    protected void checkUri(String uri) {
        if (uri == null || uri.isEmpty()) {
            throw new IllegalArgumentException("HDFS URI cannot be null or empty");
        }
        if (!uri.startsWith("hdfs://")) {
            throw new IllegalArgumentException("Invalid HDFS URI: must start with 'hdfs://'. Provided: " + uri);
        }
    }
}
