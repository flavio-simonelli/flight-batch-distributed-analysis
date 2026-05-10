package it.uniroma2.sae.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enum defining the available percentile estimation algorithms for the RDD backend of Query 3.
 * Used to select between Apache DataSketches KLL and t-digest at runtime via config.yml.
 */
public enum PercentileAlgorithm implements Serializable {
    @JsonProperty("kll")
    KLL("kll"),

    @JsonProperty("tdigest")
    TDIGEST("tdigest");

    private final String type;
    private static final Map<String, PercentileAlgorithm> LOOKUP = Arrays.stream(values())
            .collect(Collectors.toMap(t -> t.type.toLowerCase(), t -> t));

    PercentileAlgorithm(String type) { this.type = type; }

    public static PercentileAlgorithm fromString(String type) {
        PercentileAlgorithm result = (type == null) ? null : LOOKUP.get(type.toLowerCase());
        if (result == null) throw new IllegalArgumentException("No PercentileAlgorithm found for: " + type);
        return result;
    }

    @Override
    public String toString() {
        return type;
    }
}
