package it.uniroma2.sae.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;

/**
 * Main class representing the application's overall configuration loaded from a YAML file.
 * It orchestrates various configuration sections like
 * - Spark cluster settings
 * - input/output storage details
 * - query to be executed
 * - execution backend API
 */
public class ApplicationConfig {

    public static final String CONFIG_FILE = "compose-config.yml";


    /**
     * Loads and parses a YAML configuration file from the classpath into an ApplicationConfig object.
     *
     * @param resourceName the name of the resource file to load (e.g., "/compose-config.yml")
     * @return an instance of ApplicationConfig populated with the data from the YAML file
     * @throws Exception if the resource is not found or cannot be parsed
     */
    public static ApplicationConfig load(String resourceName) throws Exception {
        if (resourceName == null) throw new IllegalArgumentException("Resource name cannot be null");
        if (!resourceName.startsWith("/")) resourceName = "/" + resourceName;

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        try (InputStream is = ApplicationConfig.class.getResourceAsStream(resourceName)) {
            if (is == null) throw new IllegalArgumentException("Resource not found: " + resourceName);
            return mapper.readValue(is, ApplicationConfig.class);
        }
    }

    // --- Application Specific Configuration ---
    private String appName;
    private QueryType queryToRun;
    private AppBackendType appBackend;
    private PercentileAlgorithm percentileAlgorithm = PercentileAlgorithm.KLL;

    // --- Spark Cluster Configuration ---
    private SparkConfig sparkCluster;

    // --- Storage Configurations ---
    private StorageConfig input;
    private StorageConfig output;

    // --- Getters and Setters ---

    public String getAppName() {
        return appName + " - " + queryToRun + " (" + appBackend + ")";
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
    
    public AppBackendType getAppBackend() {
        return appBackend;
    }
    public void setAppBackend(AppBackendType appBackend) {
        this.appBackend = appBackend;
    }
    public String getStringAppBackend() {
        return appBackend != null ? appBackend.toString() : null;
    }
    public void setStringAppBackend(String appBackend) {
        this.appBackend = AppBackendType.fromString(appBackend);
    }

    public PercentileAlgorithm getPercentileAlgorithm() {
        return percentileAlgorithm;
    }
    public void setPercentileAlgorithm(PercentileAlgorithm percentileAlgorithm) {
        this.percentileAlgorithm = percentileAlgorithm;
    }
    public String getStringPercentileAlgorithm() {
        return percentileAlgorithm != null ? percentileAlgorithm.toString() : null;
    }
    public void setStringPercentileAlgorithm(String percentileAlgorithm) {
        this.percentileAlgorithm = PercentileAlgorithm.fromString(percentileAlgorithm);
    }

    public SparkConfig getSparkCluster() {
        return sparkCluster;
    }
    public void setSparkCluster(SparkConfig sparkCluster) {
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
}