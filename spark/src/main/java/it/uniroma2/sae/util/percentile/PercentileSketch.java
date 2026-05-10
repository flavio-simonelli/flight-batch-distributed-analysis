package it.uniroma2.sae.util.percentile;

import it.uniroma2.sae.config.PercentileAlgorithm;

import java.io.Serializable;

/**
 * Strategy interface for streaming percentile estimation algorithms used by Query 3 in the RDD backend.
 *
 * Sketch state is exchanged as {@code byte[]} so it can travel through Spark's default Java
 * serialization on shuffle without needing custom Kryo registrations: neither KLL nor t-digest
 * implement {@link Serializable} directly, but both expose binary serialization.
 *
 * Implementations must themselves be {@link Serializable} because the strategy object is captured
 * by the {@code combineByKey} closures and shipped to executors.
 */
public interface PercentileSketch extends Serializable {

    /**
     * Build a new sketch state seeded with a single value (zero of {@code combineByKey}).
     */
    byte[] init(double firstValue);

    /**
     * Add a new value to an existing sketch state (seqOp of {@code combineByKey}).
     */
    byte[] update(byte[] state, double value);

    /**
     * Merge two sketch states (combOp of {@code combineByKey}).
     */
    byte[] merge(byte[] a, byte[] b);

    /**
     * Extract the requested quantiles from a sketch state.
     */
    double[] getQuantiles(byte[] state, double... quantiles);

    /**
     * Factory: pick the implementation matching the configured algorithm.
     */
    static PercentileSketch from(PercentileAlgorithm algorithm) {
        if (algorithm == null) throw new IllegalArgumentException("PercentileAlgorithm cannot be null");
        switch (algorithm) {
            case KLL:     return new KllSketchStrategy();
            case TDIGEST: return new TDigestStrategy();
            default:      throw new IllegalArgumentException("Unsupported percentile algorithm: " + algorithm);
        }
    }
}
