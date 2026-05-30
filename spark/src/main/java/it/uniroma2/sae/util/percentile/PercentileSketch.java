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
     *
     * @param firstValue the first value to initialize the sketch with
     * @return a byte array representing the serialized sketch state after initialization
     */
    byte[] init(double firstValue);

    /**
     * Add a new value to an existing sketch state (seqOp of {@code combineByKey}).
     *
     * @param state the current sketch state as a byte array
     * @param value the new value to add to the sketch
     * @return a byte array representing the serialized sketch state after update
     */
    byte[] update(byte[] state, double value);

    /**
     * Merge two sketch states (combOp of {@code combineByKey}).
     *
     * @param a the first sketch state as a byte array
     * @param b the second sketch state as a byte array
     * @return a byte array representing the serialized sketch state after merging
     */
    byte[] merge(byte[] a, byte[] b);

    /**
     * Extract the requested quantiles from a sketch state.
     * 
     * @param state the sketch state as a byte array
     * @param quantiles the quantiles to extract (values in [0, 1])
     * @return the requested quantiles as an array of doubles, in the same order as the input quantiles
     */
    double[] getQuantiles(byte[] state, double... quantiles);

    /**
     * Factory method
     * Pick the implementation matching the configured algorithm.
     * 
     * @param algorithm the percentile algorithm to use
     * @return a PercentileSketch implementation corresponding to the specified algorithm
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
