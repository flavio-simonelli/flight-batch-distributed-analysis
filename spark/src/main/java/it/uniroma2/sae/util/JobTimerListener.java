package it.uniroma2.sae.util;

import org.apache.spark.scheduler.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class JobTimerListener extends SparkListener {
    private final ConcurrentHashMap<Integer, Long> jobStartTimes = new ConcurrentHashMap<>();
    private final AtomicLong maxJobDuration = new AtomicLong(0);
    private final AtomicLong totalSparkTime = new AtomicLong(0);

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        jobStartTimes.put(jobStart.jobId(), jobStart.time());
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        Long start = jobStartTimes.remove(jobEnd.jobId());
        if (start != null) {
            long duration = jobEnd.time() - start;
            totalSparkTime.addAndGet(duration);
            // Update the maximum job duration
            maxJobDuration.updateAndGet(currentMax -> Math.max(currentMax, duration));
            System.out.printf("[SPARK] Job %d finished in %d ms%n", jobEnd.jobId(), duration);
        }
    }

    public long getMaxJobDuration() { return maxJobDuration.get(); }
    public long getTotalSparkTime() { return totalSparkTime.get(); }

    public void reset() {
        maxJobDuration.set(0);
        totalSparkTime.set(0);
    }
}