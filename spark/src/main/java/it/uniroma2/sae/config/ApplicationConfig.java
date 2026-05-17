package it.uniroma2.sae.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;
import java.util.Map;
import java.util.HashMap;

/**
 * Main class representing the application's overall configuration loaded from a YAML file.
 * It handles multiple storage options for inputs, outputs, and metrics.
 */
public class ApplicationConfig {

    public static final String CONFIG_FILE = "local-config.yml";

    /**
     * Loads and parses a YAML configuration file from the classpath into an ApplicationConfig object.
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

    // --- Core Properties ---
    private String appName;
    private QueryType queryToRun;
    private AppBackendType appBackend;
    private PercentileAlgorithm percentileAlgorithm = PercentileAlgorithm.KLL;

    // --- Spark Cluster Configuration ---
    private SparkConfig sparkCluster;

    // --- Storage Maps ---
    private Map<String, StorageConfig> inputs = new HashMap<>();
    private Map<String, StorageConfig> outputs = new HashMap<>();
    private Map<String, StorageConfig> metricsOptions = new HashMap<>();

    // --- Selection State (ignored by Jackson deserialization from YAML) ---
    private String selectedInput = "hdfs";
    private String selectedOutput = "hdfs";
    private String selectedMetrics = "redis";

    // --- Getters and Setters (Standard) ---

    @JsonIgnore
    public String getAppName() {
        return appName + " - " + queryToRun + " (" + appBackend + ")";
    }
    public void setAppName(String appName) { this.appName = appName; }

    public QueryType getQueryToRun() { return queryToRun; }
    public void setQueryToRun(QueryType queryToRun) { this.queryToRun = queryToRun; }

    @JsonIgnore
    public String getStringQueryToRun() { return queryToRun != null ? queryToRun.toString() : null; }
    public void setStringQueryToRun(String queryToRun) { this.queryToRun = QueryType.fromString(queryToRun); }
    
    public AppBackendType getAppBackend() { return appBackend; }
    public void setAppBackend(AppBackendType appBackend) { this.appBackend = appBackend; }

    @JsonIgnore
    public String getStringAppBackend() { return appBackend != null ? appBackend.toString() : null; }
    public void setStringAppBackend(String appBackend) { this.appBackend = AppBackendType.fromString(appBackend); }

    public PercentileAlgorithm getPercentileAlgorithm() { return percentileAlgorithm; }
    public void setPercentileAlgorithm(PercentileAlgorithm percentileAlgorithm) { this.percentileAlgorithm = percentileAlgorithm; }

    @JsonIgnore
    public String getStringPercentileAlgorithm() { return percentileAlgorithm != null ? percentileAlgorithm.toString() : null; }
    public void setStringPercentileAlgorithm(String percentileAlgorithm) { this.percentileAlgorithm = PercentileAlgorithm.fromString(percentileAlgorithm); }

    public SparkConfig getSparkCluster() { return sparkCluster; }
    public void setSparkCluster(SparkConfig sparkCluster) { this.sparkCluster = sparkCluster; }

    // --- Storage Map Handlers ---

    public Map<String, StorageConfig> getInputs() { return inputs; }
    public void setInputs(Map<String, StorageConfig> inputs) { this.inputs = inputs; }

    public Map<String, StorageConfig> getOutputs() { return outputs; }
    public void setOutputs(Map<String, StorageConfig> outputs) { this.outputs = outputs; }

    @JsonProperty("metrics")
    public Map<String, StorageConfig> getMetricsOptions() { return metricsOptions; }
    @JsonProperty("metrics")
    public void setMetricsOptions(Map<String, StorageConfig> metricsOptions) { this.metricsOptions = metricsOptions; }

    // --- Dynamic Selectors (Set via CLI) ---

    @JsonIgnore
    public void setSelectedInput(String key) { this.selectedInput = key; }
    @JsonIgnore
    public void setSelectedOutput(String key) { this.selectedOutput = key; }
    @JsonIgnore
    public void setSelectedMetrics(String key) { this.selectedMetrics = key; }

    // --- Functional Accessors (Return the active configuration) ---

    @JsonIgnore
    public StorageConfig getInput() {
        return inputs.getOrDefault(selectedInput, inputs.get("hdfs"));
    }

    @JsonIgnore
    public StorageConfig getOutput() {
        return outputs.getOrDefault(selectedOutput, outputs.get("hdfs"));
    }

    @JsonIgnore
    public StorageConfig getMetrics() {
        return metricsOptions.getOrDefault(selectedMetrics, metricsOptions.get("redis"));
    }
}
