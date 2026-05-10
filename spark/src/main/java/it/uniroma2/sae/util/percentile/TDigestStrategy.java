package it.uniroma2.sae.util.percentile;

import com.tdunning.math.stats.MergingDigest;

import java.nio.ByteBuffer;

/**
 * T-Digest quantile sketch strategy backed by Ted Dunning's t-digest library.
 *
 * Uses {@link MergingDigest} with compression=100, which gives ~1% relative quantile error in the
 * tails — accuracy comparable to KLL with k=200. Sketch state is round-tripped through
 * {@code byte[]} for transparent Spark serialization.
 */
public class TDigestStrategy implements PercentileSketch {

    private static final long serialVersionUID = 1L;

    private static final double COMPRESSION = 100.0;

    @Override
    public byte[] init(double firstValue) {
        MergingDigest digest = new MergingDigest(COMPRESSION);
        digest.add(firstValue);
        return toBytes(digest);
    }

    @Override
    public byte[] update(byte[] state, double value) {
        MergingDigest digest = MergingDigest.fromBytes(ByteBuffer.wrap(state));
        digest.add(value);
        return toBytes(digest);
    }

    @Override
    public byte[] merge(byte[] a, byte[] b) {
        MergingDigest da = MergingDigest.fromBytes(ByteBuffer.wrap(a));
        MergingDigest db = MergingDigest.fromBytes(ByteBuffer.wrap(b));
        da.add(db);
        return toBytes(da);
    }

    @Override
    public double[] getQuantiles(byte[] state, double... quantiles) {
        MergingDigest digest = MergingDigest.fromBytes(ByteBuffer.wrap(state));
        double[] out = new double[quantiles.length];
        for (int i = 0; i < quantiles.length; i++) {
            out[i] = digest.quantile(quantiles[i]);
        }
        return out;
    }

    private static byte[] toBytes(MergingDigest digest) {
        ByteBuffer buf = ByteBuffer.allocate(digest.smallByteSize());
        digest.asSmallBytes(buf);
        return buf.array();
    }
}
