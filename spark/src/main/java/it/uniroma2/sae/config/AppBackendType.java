package it.uniroma2.sae.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enum defining the available Spark backend APIs to execute a query.
 */
public enum AppBackendType implements Serializable {
    @JsonProperty("rdd")
    RDD("rdd"),

    @JsonProperty("dataframe")
    DATAFRAME("dataframe"),

    @JsonProperty("sql")
    SQL("sql");

    private final String type;
    private static final Map<String, AppBackendType> LOOKUP = Arrays.stream(values())
            .collect(Collectors.toMap(t -> t.type.toLowerCase(), t -> t));

    AppBackendType(String type) { this.type = type; }

    public static AppBackendType fromString(String type) {
        AppBackendType result = (type == null) ? null : LOOKUP.get(type.toLowerCase());
        if (result == null) throw new IllegalArgumentException("No AppBackendType found for: " + type);
        return result;
    }

    @Override
    public String toString() {
        return type;
    }
}
