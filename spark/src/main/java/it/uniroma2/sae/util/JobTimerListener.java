package it.uniroma2.sae.util;

import org.apache.spark.scheduler.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SparkListener that tracks the duration of each Spark job and reports them
 * to the PerformanceMetrics singleton.
 */
public class JobTimerListener extends SparkListener {
    
    private final ConcurrentHashMap<Integer, Long> jobStartTimes = new ConcurrentHashMap<>();

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        jobStartTimes.put(jobStart.jobId(), jobStart.time());
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        Long start = jobStartTimes.remove(jobEnd.jobId());
        if (start != null) {
            long duration = jobEnd.time() - start;
            // Report to the PerformanceMetrics singleton
            PerformanceMetrics.getInstance().addSparkJob(jobEnd.jobId(), duration);
        }
    }
}
