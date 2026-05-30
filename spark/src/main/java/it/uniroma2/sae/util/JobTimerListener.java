package it.uniroma2.sae.util;

import org.apache.spark.scheduler.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SparkListener that tracks the duration of each Spark job and reports them
 * to the PerformanceMetrics singleton.
 */
public class JobTimerListener extends SparkListener {
    
    // Map to track start times of jobs and stages
    private final ConcurrentHashMap<Integer, Long> jobStartTimes = new ConcurrentHashMap<>();
    // Map to track start times of stages
    private final ConcurrentHashMap<Integer, Long> stageStartTimes = new ConcurrentHashMap<>();

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        stageStartTimes.put(stageSubmitted.stageInfo().stageId(), System.currentTimeMillis());
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        Long start = stageStartTimes.remove(stageCompleted.stageInfo().stageId());
        if (start != null) {
            long duration = System.currentTimeMillis() - start;
            long gcTime = stageCompleted.stageInfo().taskMetrics().jvmGCTime();
            long bytesRead = stageCompleted.stageInfo().taskMetrics().inputMetrics().bytesRead();
            long bytesWritten = stageCompleted.stageInfo().taskMetrics().outputMetrics().bytesWritten();
            long shuffleRead = stageCompleted.stageInfo().taskMetrics().shuffleReadMetrics().totalBytesRead();
            long shuffleWrite = stageCompleted.stageInfo().taskMetrics().shuffleWriteMetrics().bytesWritten();

            // Riporta metriche più dettagliate
            PerformanceMetrics.getInstance().addStageMetrics(
                    stageCompleted.stageInfo().stageId(),
                    duration,
                    gcTime,
                    bytesRead,
                    bytesWritten,
                    shuffleRead,
                    shuffleWrite
            );
        }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        if (taskEnd.taskMetrics() != null) {
            String executorId = taskEnd.taskInfo().executorId();
            String host = taskEnd.taskInfo().host();
            long runTime = taskEnd.taskMetrics().executorRunTime();
            long cpuTime = taskEnd.taskMetrics().executorCpuTime();
            long gcTime = taskEnd.taskMetrics().jvmGCTime();
            long deserializeTime = taskEnd.taskMetrics().executorDeserializeTime();
            long serializeTime = taskEnd.taskMetrics().resultSerializationTime();
            long bytesRead = taskEnd.taskMetrics().inputMetrics().bytesRead();
            long bytesWritten = taskEnd.taskMetrics().outputMetrics().bytesWritten();
            long shuffleRead = taskEnd.taskMetrics().shuffleReadMetrics().totalBytesRead();
            long shuffleWrite = taskEnd.taskMetrics().shuffleWriteMetrics().bytesWritten();

            PerformanceMetrics.getInstance().addExecutorMetrics(
                executorId, host, runTime, cpuTime, gcTime, deserializeTime, serializeTime, 
                bytesRead, bytesWritten, shuffleRead, shuffleWrite
            );
        }
    }

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
