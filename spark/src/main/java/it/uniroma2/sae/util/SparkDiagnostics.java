package it.uniroma2.sae.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.LongSummaryStatistics;

/**
 * Utility for inspecting partition distribution and detecting data skew in Spark jobs.
 * Supports both RDD and DataFrame APIs. All output is routed through SLF4J so that
 * log-level filtering applies at runtime without code changes.
 *
 * Usage:
 *   SparkDiagnostics.profilePartitions(myRdd, "after groupBy");
 *   SparkDiagnostics.checkSkew(myDataset, "airports join", 2.0);
 *
 * Diagnostics can be suppressed globally via {@link #setEnabled(boolean)}.
 */
public final class SparkDiagnostics {

    private static final Logger log = LoggerFactory.getLogger(SparkDiagnostics.class);
    /** Default skew threshold: warn when max partition > N × average. */
    public static final double DEFAULT_SKEW_THRESHOLD = 2.0;
    private static volatile boolean enabled = true;
    private SparkDiagnostics() {}

    public static void setEnabled(boolean value) {
        enabled = value;
    }

    public static boolean isEnabled() {
        return enabled;
    }

    /**
     * Logs per-partition row counts for an RDD. The RDD is cached before inspection
     * so that the count action does not force a full re-computation of the lineage.
     */
    public static <T> void profilePartitions(JavaRDD<T> rdd, String label) {
        if (!enabled) return;

        rdd.cache();
        PartitionStats stats = collectRddStats(rdd);
        logProfile(label, stats);
    }

    /**
     * Warns when any partition holds more than {@code threshold × average} rows.
     * Use {@link #DEFAULT_SKEW_THRESHOLD} for a sensible default.
     */
    public static <T> void checkSkew(JavaRDD<T> rdd, String label, double threshold) {
        if (!enabled) return;

        rdd.cache();
        PartitionStats stats = collectRddStats(rdd);
        evaluateSkew(label, stats, threshold);
    }

    public static <T> void checkSkew(JavaRDD<T> rdd, String label) {
        checkSkew(rdd, label, DEFAULT_SKEW_THRESHOLD);
    }

    /**
     * Logs per-partition row counts for a Dataset (DataFrame). The Dataset is cached
     * before inspection; callers are responsible for unpersisting it when done.
     */
    public static void profilePartitions(Dataset<Row> dataset, String label) {
        if (!enabled) return;

        dataset.cache();
        PartitionStats stats = collectDatasetStats(dataset);
        logProfile(label, stats);
    }

    /**
     * Warns when any partition of a Dataset holds more than {@code threshold × average} rows.
     */
    public static void checkSkew(Dataset<Row> dataset, String label, double threshold) {
        if (!enabled) return;

        dataset.cache();
        PartitionStats stats = collectDatasetStats(dataset);
        evaluateSkew(label, stats, threshold);
    }

    public static void checkSkew(Dataset<Row> dataset, String label) {
        checkSkew(dataset, label, DEFAULT_SKEW_THRESHOLD);
    }

    private static <T> PartitionStats collectRddStats(JavaRDD<T> rdd) {
        int numPartitions = rdd.getNumPartitions();

        List<Long> counts = rdd.mapPartitionsWithIndex((index, it) -> {
            long count = 0;
            while (it.hasNext()) { it.next(); count++; }
            return Collections.singletonList(count).iterator();
        }, true).collect();

        return new PartitionStats(numPartitions, counts);
    }

    private static PartitionStats collectDatasetStats(Dataset<Row> dataset) {
        JavaRDD<Row> rdd = dataset.javaRDD();
        return collectRddStats(rdd);
    }

    private static void logProfile(String label, PartitionStats stats) {
        if (!log.isInfoEnabled()) return;

        StringBuilder sb = new StringBuilder();
        sb.append("\n=======================================================\n");
        sb.append("[DIAGNOSTICS] Partition Distribution: ").append(label).append("\n");
        sb.append(String.format("  Total rows   : %d%n", stats.summary.getSum()));
        sb.append(String.format("  Partitions   : %d%n", stats.numPartitions));
        sb.append(String.format("  Min / Avg / Max per partition: %d / %.1f / %d%n",
                stats.summary.getMin(),
                stats.summary.getAverage(),
                stats.summary.getMax()));
        sb.append("  --- Per-partition detail ---\n");
        for (int i = 0; i < stats.counts.size(); i++) {
            sb.append(String.format("  Partition %3d -> %d rows%n", i, stats.counts.get(i)));
        }
        sb.append("=======================================================");
        log.info(sb.toString());
    }

    private static void evaluateSkew(String label, PartitionStats stats, double threshold) {
        if (stats.numPartitions == 0 || stats.summary.getCount() == 0) return;

        double avg = stats.summary.getAverage();
        long max = stats.summary.getMax();

        if (max > avg * threshold) {
            log.warn("[SKEW WARNING - {}] Significant imbalance detected! "
                    + "Expected avg {:.2f} rows/partition, max partition has {} rows (threshold x{}).",
                    label, avg, max, threshold);
        } else {
            log.debug("[SKEW CHECK - {}] No critical skew detected. "
                    + "Avg {:.2f} rows/partition, max {} rows.", label, avg, max);
        }
    }

    // -------------------------------------------------------------------------
    // Value object
    // -------------------------------------------------------------------------

    private static final class PartitionStats {
        final int numPartitions;
        final List<Long> counts;
        final LongSummaryStatistics summary;

        PartitionStats(int numPartitions, List<Long> counts) {
            this.numPartitions = numPartitions;
            this.counts = counts;
            this.summary = counts.stream().mapToLong(Long::longValue).summaryStatistics();
        }
    }
}
