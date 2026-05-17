package it.uniroma2.sae.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/*
 * Enum defining the available storage types.
 */
public enum StorageType implements Serializable {
    @JsonProperty("hdfs")
    HDFS("hdfs"),

    @JsonProperty("s3")
    S3("s3"),

    @JsonProperty("local")
    LOCAL("local"),
    
    @JsonProperty("postgres")
    POSTGRES("postgres"),

    @JsonProperty("redis")
    REDIS("redis"),

    @JsonProperty("hbase")
    HBASE("hbase");

    private final String type;
    private static final Map<String, StorageType> LOOKUP = Arrays
                                                                .stream(values())
                                                                .collect(Collectors.toMap(t -> t.type.toLowerCase(), t -> t));

    StorageType(String type) { this.type = type; }

    public static StorageType fromString(String type) {
        StorageType result = (type == null) ? null : LOOKUP.get(type.toLowerCase());
        if (result == null) throw new IllegalArgumentException("No StorageType found for: " + type);
        return result;
    }

    @Override
    public String toString() {
        return type;
    }
}
