package it.uniroma2.sae.config;

/**
 * Configuration class for remote storage systems (e.g., HDFS, S3).
 * Extends {@link StorageConfig} by adding a 'uri' field for the base URI of the remote system.
 */
public class RemoteStorageConfig extends StorageConfig {
    private String uri;


    public String getUri() {
        return uri;
    }
    public void setUri(String uri) {
        this.uri = uri;
    }
}
