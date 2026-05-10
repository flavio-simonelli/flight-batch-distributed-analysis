package it.uniroma2.sae.util;

import org.apache.spark.api.java.JavaRDD;

import java.util.Collections;
import java.util.List;

public class SparkDiagnostics {

    public static boolean ENABLED = true;

    public static <T> void profilePartitions(JavaRDD<T> rdd, String label) {
        if (!ENABLED) return;

        System.out.println("\n=======================================================");
        System.out.println("[DIAGNOSTICA] Analisi Distribuzione: " + label);

        rdd.cache();

        long totalElements = rdd.count();
        int numPartitions = rdd.getNumPartitions();

        System.out.printf("Totale elementi: %d%n", totalElements);
        System.out.printf("Numero partizioni: %d%n", numPartitions);
        System.out.println("--- Dettaglio Partizioni ---");

        List<String> partitionStats = rdd.mapPartitionsWithIndex((index, iterator) -> {
            long count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            return Collections.singletonList(String.format("Partizione %3d -> %d righe", index, count)).iterator();
        }, true).collect();

        for (String stat : partitionStats) {
            System.out.println(stat);
        }
        System.out.println("=======================================================\n");
    }

    public static <T> void checkSkew(JavaRDD<T> rdd, String label, double threshold) {
        if (!ENABLED) return;

        rdd.cache();
        long totalElements = rdd.count();
        int numPartitions = rdd.getNumPartitions();

        if (numPartitions == 0 || totalElements == 0) return;

        double averagePerPartition = (double) totalElements / numPartitions;

        List<Long> counts = rdd.mapPartitions(iterator -> {
            long count = 0;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            return Collections.singletonList(count).iterator();
        }).collect();

        long maxPartitionSize = Collections.max(counts);

        if (maxPartitionSize > (averagePerPartition * threshold)) {
            System.err.println("\n[WARNING SKEW - " + label + "]");
            System.err.printf("Rilevato forte sbilanciamento! Media attesa: %.2f righe, Max rilevato: %d righe%n",
                    averagePerPartition, maxPartitionSize);
        } else {
            System.out.println("\n[DIAGNOSTICA SKEW - " + label + "] Nessuno sbilanciamento critico rilevato.");
        }
    }
}
