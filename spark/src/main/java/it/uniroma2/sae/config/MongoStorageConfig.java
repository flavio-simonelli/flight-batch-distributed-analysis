package it.uniroma2.sae.config;

/**
 * Configuration class for MongoDB storage.
 * Extends {@link DbStorageConfig} and adds a collection field.
 */
public class MongoStorageConfig extends DbStorageConfig {
    private String collection;
    private String authSource = "admin"; // Default to admin for root users

    public String getCollection() {
        return collection;
    }
    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getAuthSource() {
        return authSource;
    }
    public void setAuthSource(String authSource) {
        this.authSource = authSource;
    }

    @Override
    public String getConnectionUri() {
        String credentials = "";
        if (getUser() != null && !getUser().isEmpty()) {
            credentials = getUser() + ":" + (getPassword() != null ? getPassword() : "") + "@";
        }
        
        // Added authSource to handle root users created in the 'admin' database
        String uri = String.format("mongodb://%s%s:%d/%s", 
                credentials, getHostname(), getPort(), getDatabase());
        
        if (authSource != null && !authSource.isEmpty()) {
            uri += "?authSource=" + authSource;
        }
        
        return uri;
    }
}
