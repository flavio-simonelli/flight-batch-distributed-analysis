package it.uniroma2.sae.config;

import java.io.Serializable;

/**
 * Configuration class for the Spark cluster environment.
 * This class maps the 'sparkCluster' section of the application's YAML configuration.
 */
public class SparkConfig implements Serializable {
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

    public static class SparkNode implements Serializable {
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
