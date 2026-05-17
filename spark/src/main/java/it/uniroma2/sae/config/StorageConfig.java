package it.uniroma2.sae.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * Base class for storage configurations (input or output).
 * It defines common properties like the storage type and the path.
 *
 * Uses Jackson's @JsonTypeInfo and @JsonSubTypes to enable polymorphic deserialization.
 * This allows Jackson to instantiate the correct subclass (e.g., RemoteStorageConfig)
 * based on the 'type' field in the YAML configuration.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type",
        visible = true
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = RemoteStorageConfig.class, name = "hdfs"),
        @JsonSubTypes.Type(value = RemoteStorageConfig.class, name = "s3"),
        @JsonSubTypes.Type(value = JdbcStorageConfig.class, name = "postgres"),
        @JsonSubTypes.Type(value = MongoStorageConfig.class, name = "mongodb"),
        @JsonSubTypes.Type(value = RedisStorageConfig.class, name = "redis"),
        @JsonSubTypes.Type(value = HBaseStorageConfig.class, name = "hbase"),
        @JsonSubTypes.Type(value = StorageConfig.class, name = "local") // Default for local, no 'uri' field
})
public class StorageConfig implements Serializable {
    private StorageType type;
    private String path;
    private String datasetFilename;
    private String resultDirectory;


    public StorageType getType() {
        return type;
    }
    public void setType(StorageType type) {
        this.type = type;
    }
    public String getStringType() {
        return type != null ? type.toString() : null;
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

    public String getDatasetFilename() {
        return datasetFilename;
    }
    public void setDatasetFilename(String datasetFilename) {
        this.datasetFilename = datasetFilename;
    }

    public String getResultDirectory() {
        String rd = resultDirectory != null ? resultDirectory : "/";
        return rd.endsWith("/") ? rd : rd + "/";
    }
    public void setResultDirectory(String resultDirectory) {
        this.resultDirectory = resultDirectory;
    }
}
