package it.uniroma2.sae.config;

/**
 * Configuration class for JDBC-based storage systems.
 * Extends {@link StorageConfig} by adding connection details required for JDBC.
 */
public class JdbcStorageConfig extends StorageConfig {
    private String driver;
    private String url;
    private String hostname;
    private Integer port;
    private String database;
    private String user;
    private String password;

    public String getDriver() {
        return driver;
    }
    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }
    public void setUrl(String url) {
        this.url = url;
    }

    public String getHostname() {
        return hostname;
    }
    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public Integer getPort() {
        return port;
    }
    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }
    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUser() {
        return user;
    }
    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String getResultDirectory() {
        return "";
    }
}
