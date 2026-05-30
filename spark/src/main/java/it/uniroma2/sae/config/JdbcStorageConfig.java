package it.uniroma2.sae.config;

/**
 * Configuration class for JDBC-based storage systems.
 * Extends {@link DbStorageConfig} by adding JDBC-specific details like driver and URL.
 */
public class JdbcStorageConfig extends DbStorageConfig {
    private String driver;
    private String url;

    public String getDriver() {
        if (driver != null) return driver;
        if (getType() == null) return null;
        
        // Default drivers based on storage type
        switch (getType()) {
            case POSTGRES:
            case COCKROACH:
                return "org.postgresql.Driver";
            default:
                return null;
        }
    }
    
    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        if (url != null) return url;
        if (getType() == null) return null;

        // Construct default URL based on storage type and parameters
        switch (getType()) {
            case POSTGRES:
            case COCKROACH:
                return String.format("jdbc:postgresql://%s:%d/%s", getHostname(), getPort(), getDatabase());
            default:
                return null;
        }
    }
    
    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String getConnectionUri() {
        return getUrl();
    }
}
