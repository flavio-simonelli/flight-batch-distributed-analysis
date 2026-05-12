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
        
        // Default drivers based on storage type
        if (getType() != null) {
            switch (getType()) {
                case POSTGRES:
                    return "org.postgresql.Driver";
            }
        }
        return null;
    }
    
    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        if (url != null) return url;

        // Construct default URL based on storage type and parameters
        if (getType() != null) {
            switch (getType()) {
                case POSTGRES:
                    return String.format("jdbc:postgresql://%s:%d/%s", getHostname(), getPort(), getDatabase());
            }
        }
        return null;
    }
    
    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String getConnectionUri() {
        return getUrl();
    }
}
