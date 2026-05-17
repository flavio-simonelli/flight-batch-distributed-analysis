package it.uniroma2.sae.config;

/**
 * Configuration class for HBase storage.
 * Extends {@link DbStorageConfig}.
 * Map:
 * - hostname -> zookeeperQuorum
 * - port -> zookeeperClientPort
 * - database -> tableName
 */
public class HBaseStorageConfig extends DbStorageConfig {
    
    public String getZookeeperQuorum() {
        return getHostname();
    }
    public void setZookeeperQuorum(String zookeeperQuorum) {
        setHostname(zookeeperQuorum);
    }

    public Integer getZookeeperClientPort() {
        return getPort();
    }
    public void setZookeeperClientPort(Integer zookeeperClientPort) {
        setPort(zookeeperClientPort);
    }

    public String getTableName() {
        return getDatabase();
    }
    public void setTableName(String tableName) {
        setDatabase(tableName);
    }

    @Override
    public String getConnectionUri() {
        // HBase doesn't use a single connection URI, 
        // but we can return the Zookeeper quorum string as its identifier.
        return getZookeeperQuorum() + ":" + getZookeeperClientPort();
    }
}
