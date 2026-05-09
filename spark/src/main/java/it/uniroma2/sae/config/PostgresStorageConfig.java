package it.uniroma2.sae.config;

/**
 * Configuration class for PostgreSQL storage systems.
 * Extends {@link StorageConfig} by adding connection details required for JDBC.
 */
public class PostgresStorageConfig extends StorageConfig {
    private String url;
    private String user;
    private String password;
    private String dbtable;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
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

    public String getDbtable() {
        return dbtable;
    }

    public void setDbtable(String dbtable) {
        this.dbtable = dbtable;
    }
}
