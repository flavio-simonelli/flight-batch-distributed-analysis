package it.uniroma2.sae.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enum defining the available queries that can be executed.
 */
public enum QueryType implements Serializable {
    @JsonProperty("monthly_performance")
    MONTHLY_PERFORMANCE("monthly_performance"),

    @JsonProperty("arrival_delay_ranking")
    ARRIVAL_DELAY_RANKING("arrival_delay_ranking"),

    @JsonProperty("hourly_delay_percentiles")
    HOURLY_DELAY_PERCENTILES("hourly_delay_percentiles"),

    @JsonProperty("airline_clustering")
    AIRLINE_CLUSTERING("airline_clustering");

    private final String type;
    private static final Map<String, QueryType> LOOKUP = Arrays.stream(values())
            .collect(Collectors.toMap(t -> t.type.toLowerCase(), t -> t));

    QueryType(String type) { this.type = type; }

    public static QueryType fromString(String type) {
        QueryType result = (type == null) ? null : LOOKUP.get(type.toLowerCase());
        if (result == null) throw new IllegalArgumentException("No QueryType found for: " + type);
        return result;
    }

    @Override
    public String toString() {
        return type;
    }
}
