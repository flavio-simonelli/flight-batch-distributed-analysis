package it.uniroma2.sae.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;

/**
 * POJO class representing the application configuration loaded from a YAML file.
 * It contains all the necessary settings to initialize the Spark job, including:
 * - cluster configuration
 * - input data sources
 * - output destinations
 * - query to run
 */
public class AppConfig {

    public static final String CONFIG_FILE = "config.yml";

    /**
     * Loads and parses a YAML configuration file from the classpath into an AppConfig object.
     *
     * @param resourceName the name of the resource file to load (e.g., "/config.yml")
     * @return an instance of AppConfig populated with the data from the YAML file
     * @throws Exception if the resource is not found or cannot be parsed
     */
    public static AppConfig load(String resourceName) throws Exception {
        if (resourceName == null) throw new IllegalArgumentException("Resource name cannot be null");
        if (!resourceName.startsWith("/")) resourceName = "/" + resourceName;

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try (InputStream is = AppConfig.class.getResourceAsStream(resourceName)) {
            if (is == null) throw new IllegalArgumentException("Resource not found: " + resourceName);
            return mapper.readValue(is, AppConfig.class);
        }
    }


    private String appName;
    private QueryType queryToRun;
    private String datasetFilename;

    private SparkCluster sparkCluster;

    private StorageConfig input;
    private StorageConfig output;

    
    public String getAppName() {
        return appName;
    }
    public void setAppName(String appName) {
        this.appName = appName;
    }

    public QueryType getQueryToRun() {
        return queryToRun;
    }
    public void setQueryToRun(QueryType queryToRun) {
        this.queryToRun = queryToRun;
    }
    public String getStringQueryToRun() {
        return queryToRun != null ? queryToRun.toString() : null;
    }
    public void setStringQueryToRun(String queryToRun) {
        this.queryToRun = QueryType.fromString(queryToRun);
    }
    
    public String getDatasetFilename() {
        return datasetFilename;
    }
    public void setDatasetFilename(String datasetFilename) {
        this.datasetFilename = datasetFilename;
    }

    public SparkCluster getSparkCluster() {
        return sparkCluster;
    }
    public void setSparkCluster(SparkCluster sparkCluster) {
        this.sparkCluster = sparkCluster;
    }

    
    public StorageConfig getInput() {
        return input;
    }
    public void setInput(StorageConfig input) {
        this.input = input;
    }

    public StorageConfig getOutput() {
        return output;
    }
    public void setOutput(StorageConfig output) {
        this.output = output;
    }

    /**
     * Inner class modeling the configuration for a specific storage component (input or output).
     */
    public static class StorageConfig {
        private StorageType type;
        private String path;

        public StorageType getType() {
            return type;
        }
        public void setType(StorageType type) {
            this.type = type;
        }
        public String getStringType() {
            return type.toString();
        }
        public void setStringType(String type) {
            this.type = StorageType.fromString(type);
        }

        public String getPath() {
            return path;
        }
        public void setPath(String path) {
            this.path = path;
        }
    }

    /**
     * Inner class modeling the configuration for the Spark cluster environment.
     */
    public static class SparkCluster {
        private SparkNode master;

        
        public SparkNode getMaster() {
            return master;
        }
        public void setMaster(SparkNode master) {
            this.master = master;
        }

        /**
         * Computes the full connection URI for the Spark master node.
         *
         * @return the formatted Spark master URI (e.g., "spark://hostname:port")
         */
        public String getMasterUri() {
            return "spark://" + master.getHostname() + ":" + master.getPort();
        }

        /**
         * Inner class modeling a single node within the Spark cluster.
         */
        public static class SparkNode {
            private String hostname;
            private String port;

            public String getHostname() {
                return hostname;
            }
            public void setHostname(String hostname) {
                this.hostname = hostname;
            }

            public String getPort() {
                return port;
            }
            public void setPort(String port) {
                this.port = port;
            }
        }

    }

}
