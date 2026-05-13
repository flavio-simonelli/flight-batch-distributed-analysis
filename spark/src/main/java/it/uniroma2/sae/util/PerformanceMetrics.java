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
     * Records stage metrics for the current active phase.
     *
     * @param stageId the Spark stage ID
     * @param duration the duration of the stage in milliseconds
     * @param gcTime the JVM GC time in milliseconds
     * @param bytesRead total bytes read in this stage
     * @param bytesWritten total bytes written in this stage
     * @param shuffleRead total shuffle bytes read in this stage
     * @param shuffleWrite total shuffle bytes written in this stage
     */
    public void addStageMetrics(int stageId, long duration, long gcTime, long bytesRead, long bytesWritten, long shuffleRead, long shuffleWrite) {
        if (currentPhase != null) {
            phases.get(currentPhase).sparkStages.add(new SparkStageMetric(stageId, duration, gcTime, bytesRead, bytesWritten, shuffleRead, shuffleWrite));
        }
    }

    /**
     * Records executor metrics for the current active phase.
     *
     * @param executorId the executor ID
     * @param host the host address
     * @param runTime the task run time in milliseconds
     * @param cpuTime the CPU time in milliseconds
     * @param gcTime the JVM GC time in milliseconds
     * @param deserializeTime the deserialization time in milliseconds
     * @param serializeTime the serialization time in milliseconds
     * @param bytesRead bytes read by this task
     * @param bytesWritten bytes written by this task
     * @param shuffleRead shuffle bytes read by this task
     * @param shuffleWrite shuffle bytes written by this task
     */
    public void addExecutorMetrics(String executorId, String host, long runTime, long cpuTime, long gcTime, 
                                 long deserializeTime, long serializeTime, long bytesRead, long bytesWritten,
                                 long shuffleRead, long shuffleWrite) {
        if (currentPhase != null) {
            Map<String, ExecutorMetric> executorMetrics = phases.get(currentPhase).executorMetrics;
            executorMetrics.computeIfAbsent(executorId, id -> new ExecutorMetric(id, host))
                    .addMetrics(runTime, cpuTime, gcTime, deserializeTime, serializeTime, bytesRead, bytesWritten, shuffleRead, shuffleWrite);
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
                System.out.println("  - Spark Job Details:");
                metrics.sparkJobs.forEach(j -> 
                    System.out.printf("    - Job ID: %d, Total Duration: %d ms%n", j.jobId, j.duration));
            }

            if (!metrics.sparkStages.isEmpty()) {
                System.out.println("  - Spark Stage Details:");
                metrics.sparkStages.forEach(s -> 
                    System.out.printf("    - Stage ID: %d, Duration: %d ms, GC Time: %d ms, Read: %d bytes, Written: %d bytes, Shuffle Read: %d bytes, Shuffle Write: %d bytes%n", 
                        s.stageId, s.duration, s.gcTime, s.bytesRead, s.bytesWritten, s.shuffleRead, s.shuffleWrite));
            }

            if (!metrics.executorMetrics.isEmpty()) {
                System.out.println("  - Worker Node (Executor) Metrics:");
                metrics.executorMetrics.values().forEach(e -> {
                    System.out.printf("    - Executor %s (%s):%n", e.executorId, e.host);
                    System.out.printf("      Tasks: %d, RunTime: %d ms, CPU Time: %d ms, GC Time: %d ms%n", 
                        e.taskCount, e.totalRunTime, e.totalCpuTime, e.totalGcTime);
                    System.out.printf("      Deserialization: %d ms, Serialization: %d ms%n", 
                        e.totalDeserializeTime, e.totalSerializeTime);
                    System.out.printf("      I/O Read: %d bytes, I/O Written: %d bytes%n", 
                        e.totalBytesRead, e.totalBytesWritten);
                    System.out.printf("      Shuffle Read: %d bytes, Shuffle Written: %d bytes%n", 
                        e.totalShuffleRead, e.totalShuffleWritten);
                });
            }

            if (metrics.sparkJobs.isEmpty() && metrics.sparkStages.isEmpty()) {
                System.out.println("  - No Spark operations triggered in this phase.");
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
        final List<SparkStageMetric> sparkStages = new ArrayList<>();
        final Map<String, ExecutorMetric> executorMetrics = new LinkedHashMap<>();
    }

    private static class SparkJobMetric implements Serializable {
        final int jobId;
        final long duration;

        SparkJobMetric(int jobId, long duration) {
            this.jobId = jobId;
            this.duration = duration;
        }
    }

    private static class SparkStageMetric implements Serializable {
        final int stageId;
        final long duration;
        final long gcTime;
        final long bytesRead;
        final long bytesWritten;
        final long shuffleRead;
        final long shuffleWrite;

        SparkStageMetric(int stageId, long duration, long gcTime, long bytesRead, long bytesWritten, long shuffleRead, long shuffleWrite) {
            this.stageId = stageId;
            this.duration = duration;
            this.gcTime = gcTime;
            this.bytesRead = bytesRead;
            this.bytesWritten = bytesWritten;
            this.shuffleRead = shuffleRead;
            this.shuffleWrite = shuffleWrite;
        }
    }

    private static class ExecutorMetric implements Serializable {
        final String executorId;
        final String host;
        int taskCount = 0;
        long totalRunTime = 0;
        long totalCpuTime = 0;
        long totalGcTime = 0;
        long totalDeserializeTime = 0;
        long totalSerializeTime = 0;
        long totalBytesRead = 0;
        long totalBytesWritten = 0;
        long totalShuffleRead = 0;
        long totalShuffleWritten = 0;

        ExecutorMetric(String executorId, String host) {
            this.executorId = executorId;
            this.host = host;
        }

        void addMetrics(long runTime, long cpuTime, long gcTime, long deserializeTime, long serializeTime, 
                        long bytesRead, long bytesWritten, long shuffleRead, long shuffleWrite) {
            this.taskCount++;
            this.totalRunTime += runTime;
            this.totalCpuTime += (cpuTime / 1000000); // Convert nanoseconds to milliseconds
            this.totalGcTime += gcTime;
            this.totalDeserializeTime += deserializeTime;
            this.totalSerializeTime += serializeTime;
            this.totalBytesRead += bytesRead;
            this.totalBytesWritten += bytesWritten;
            this.totalShuffleRead += shuffleRead;
            this.totalShuffleWritten += shuffleWrite;
        }
    }
}
