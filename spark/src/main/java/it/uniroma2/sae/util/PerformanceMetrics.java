package it.uniroma2.sae.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Singleton class for collecting performance metrics across different phases of the application.
 * It tracks wall-clock time and associated Spark job durations for each phase.
 */
public class PerformanceMetrics implements Serializable {

    private static PerformanceMetrics instance;

    // Use LinkedHashMap to preserve insertion order of phases
    private final Map<String, PhaseMetrics> phases = new LinkedHashMap<>();
    private String currentPhase;
    private long startTime;

    private PerformanceMetrics() {}

    /**
     * Returns the singleton instance of PerformanceMetrics.
     *
     * @return the singleton instance
     */
    public static synchronized PerformanceMetrics getInstance() {
        if (instance == null) {
            instance = new PerformanceMetrics();
        }
        return instance;
    }

    /**
     * Resets the metrics collection for a new run.
     */
    public void reset() {
        phases.clear();
        currentPhase = null;
        startTime = 0;
    }

    /**
     * Starts a new measurement phase.
     *
     * @param phaseName the name of the phase (e.g., "LOADING", "PROCESSING", "SAVING")
     */
    public void startPhase(String phaseName) {
        if (currentPhase != null) {
            stopPhase();
        }
        currentPhase = phaseName;
        startTime = System.currentTimeMillis();
        phases.put(phaseName, new PhaseMetrics());
    }

    /**
     * Stops the current measurement phase and records the wall-clock duration.
     */
    public void stopPhase() {
        if (currentPhase != null) {
            long duration = System.currentTimeMillis() - startTime;
            phases.get(currentPhase).wallClockTime = duration;
            currentPhase = null;
        }
    }

    /**
     * Records a Spark job duration for the current active phase.
     *
     * @param jobId the Spark job ID
     * @param duration the duration of the job in milliseconds
     */
    public void addSparkJob(int jobId, long duration) {
        if (currentPhase != null) {
            phases.get(currentPhase).sparkJobs.add(new SparkJobMetric(jobId, duration));
        }
    }

    /**
     * Returns the total wall-clock time across all phases.
     *
     * @return total wall-clock time in milliseconds
     */
    public long getTotalWallTime() {
        return phases.values().stream().mapToLong(p -> p.wallClockTime).sum();
    }

    /**
     * Return the total Spark time across all phases.
     *
     * @return total Spark time in milliseconds
     */
    public long getTotalSparkTime() {
        return phases.values().stream().mapToLong(p -> p.sparkJobs.stream().mapToLong(j -> j.duration).sum()).sum();
    }

    /**
     * Prints a detailed performance report to the console.
     *
     * @param queryName the name of the query being reported
     */
    public void printReport(String queryName) {
        System.out.println("\n=======================================================");
        System.out.println("--- PERFORMANCE REPORT: " + queryName + " ---");
        System.out.println("=======================================================");
        
        phases.forEach((name, metrics) -> {
            System.out.printf("Phase: %s%n", name);
            System.out.printf("  - Wall Clock Time: %d ms%n", metrics.wallClockTime);
            if (!metrics.sparkJobs.isEmpty()) {
                long totalSparkTime = metrics.sparkJobs.stream().mapToLong(j -> j.duration).sum();
                long maxJobTime = metrics.sparkJobs.stream().mapToLong(j -> j.duration).max().orElse(0);
                System.out.printf("  - Spark Jobs Count: %d%n", metrics.sparkJobs.size());
                System.out.printf("  - Total Spark Execution Time: %d ms%n", totalSparkTime);
                System.out.printf("  - Longest Spark Job: %d ms%n", maxJobTime);
                System.out.println("  - Spark Job Details:");
                metrics.sparkJobs.forEach(j -> System.out.printf("    - Job ID: %d, Duration: %d ms%n", j.jobId, j.duration));
            } else {
                System.out.println("  - No Spark jobs triggered in this phase.");
            }
            System.out.println("-------------------------------------------------------");
        });

        System.out.printf("TOTAL RUNTIME:%n");
        System.out.printf("  - Wall Clock Time: %d ms%n", getTotalWallTime());
        System.out.printf("  - Spark Execution Time: %d ms%n", getTotalSparkTime());
        System.out.println("=======================================================\n");
    }

    private static class PhaseMetrics implements Serializable {
        long wallClockTime;
        final List<SparkJobMetric> sparkJobs = new ArrayList<>();
    }

    private static class SparkJobMetric implements Serializable {
        final int jobId;
        final long duration;

        SparkJobMetric(int jobId, long duration) {
            this.jobId = jobId;
            this.duration = duration;
        }
    }
}
