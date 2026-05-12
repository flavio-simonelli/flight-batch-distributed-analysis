package it.uniroma2.sae.config;

/**
 * Configuration class for Redis storage.
 * Extends {@link DbStorageConfig}.
 */
public class RedisStorageConfig extends DbStorageConfig {
    
    @Override
    public String getConnectionUri() {
        // Standard Redis URI format: redis://[:password@]host:port[/db-number]
        String credentials = (getPassword() != null && !getPassword().isEmpty()) ? ":" + getPassword() + "@" : "";
        String dbNum = (getDatabase() != null && !getDatabase().isEmpty()) ? "/" + getDatabase() : "";
        return String.format("redis://%s%s:%d%s", credentials, getHostname(), getPort(), dbNum);
    }
}
