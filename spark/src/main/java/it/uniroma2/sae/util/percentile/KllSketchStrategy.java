package it.uniroma2.sae.util.percentile;

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

/**
 * KLL quantile sketch strategy backed by Apache DataSketches.
 *
 * KLL with default k=200 yields a normalized rank error of ~1.65% (two-sided), comparable to
 * Spark SQL's {@code percentile_approx}. Sketch state is round-tripped through {@code byte[]}
 * to make it transparent to Spark's serialization layer.
 */
public class KllSketchStrategy implements PercentileSketch {

    private static final long serialVersionUID = 1L;

    @Override
    public byte[] init(double firstValue) {
        KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
        sketch.update(firstValue);
        return sketch.toByteArray();
    }

    @Override
    public byte[] update(byte[] state, double value) {
        KllDoublesSketch sketch = KllDoublesSketch.heapify(Memory.wrap(state));
        sketch.update(value);
        return sketch.toByteArray();
    }

    @Override
    public byte[] merge(byte[] a, byte[] b) {
        KllDoublesSketch sa = KllDoublesSketch.heapify(Memory.wrap(a));
        KllDoublesSketch sb = KllDoublesSketch.heapify(Memory.wrap(b));
        sa.merge(sb);
        return sa.toByteArray();
    }

    @Override
    public double[] getQuantiles(byte[] state, double... quantiles) {
        KllDoublesSketch sketch = KllDoublesSketch.heapify(Memory.wrap(state));
        double[] out = new double[quantiles.length];
        for (int i = 0; i < quantiles.length; i++) {
            out[i] = sketch.getQuantile(quantiles[i], QuantileSearchCriteria.INCLUSIVE);
        }
        return out;
    }
}
