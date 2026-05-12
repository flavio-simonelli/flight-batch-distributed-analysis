package it.uniroma2.sae.config;

import java.io.Serializable;

/**
 * Base configuration class for database-based storage systems.
 * Consolidates common connection parameters such as hostname, port, database name, and credentials.
 */
public abstract class DbStorageConfig extends StorageConfig {
    private String hostname;
    private Integer port;
    private String database;
    private String user;
    private String password;

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

    /**
     * Returns the connection URI for the specific database.
     * Must be implemented by subclasses to provide the correct format.
     *
     * @return the connection URI string
     */
    public abstract String getConnectionUri();

    @Override
    public String getResultDirectory() {
        return "";
    }
}
