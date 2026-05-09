package it.uniroma2.sae.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;

public class AppConfig {

    public static final String CONFIG_FILE = "config.yml";

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

    private SparkCluster sparkCluster;

    private StorageConfig input;
    private StorageConfig output;

    public String getAppName() {
        return appName;
    }
    public void setAppName(String appName) {
        this.appName = appName;
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

    public static class SparkCluster {
        private SparkNode master;

        public SparkNode getMaster() {
            return master;
        }
        public void setMaster(SparkNode master) {
            this.master = master;
        }

        public String getMasterUri() {
            return "spark://" + master.getHostname() + ":" + master.getPort();
        }

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
