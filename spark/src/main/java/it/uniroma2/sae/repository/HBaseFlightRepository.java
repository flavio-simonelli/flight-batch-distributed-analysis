package it.uniroma2.sae.repository;

import it.uniroma2.sae.config.HBaseStorageConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * A robust implementation of {@link DbFlightRepository} for HBase.
 * Uses a unique Row Key strategy (Value + UUID) to handle duplicates.
 */
public class HBaseFlightRepository extends DbFlightRepository<HBaseStorageConfig> implements Serializable {

    public HBaseFlightRepository(SparkSession spark, HBaseStorageConfig config) {
        super(spark, config);
    }

    @Override
    public void saveResults(Dataset<Row> results, String table) {
        if (results == null) throw new IllegalArgumentException("Results dataset cannot be null.");
        
        final String targetTable = (table != null && !table.isEmpty()) ? table : config.getTableName();
        if (targetTable == null || targetTable.isEmpty()) {
            throw new IllegalArgumentException("Target table must be provided for HBase output.");
        }

        final String quorum = config.getZookeeperQuorum();
        final String port = String.valueOf(config.getZookeeperClientPort());
        final StructType schema = results.schema();

        // Ensure table exists
        ensureTableExists(quorum, port, targetTable);

        // 2Perform distributed insertion
        results.toJavaRDD().foreachPartition(partition -> {
            Configuration hbaseConfig = HBaseConfiguration.create();
            hbaseConfig.set("hbase.zookeeper.quorum", quorum);
            hbaseConfig.set("hbase.zookeeper.property.clientPort", port);

            try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
                 Table hbaseTable = connection.getTable(TableName.valueOf(targetTable))) {

                List<Put> batch = new ArrayList<>();
                StructField[] fields = schema.fields();

                while (partition.hasNext()) {
                    Row row = partition.next();
                    if (row.get(0) == null) continue;

                    // STRATEGY: Handle duplicates by creating a unique Row Key.
                    // Format: [OriginalValue]_[RandomUUID]
                    // This keeps records grouped by the first column but ensures uniqueness.
                    String uniqueId = UUID.randomUUID().toString().substring(0, 8);
                    String rowKeyStr = row.get(0).toString() + "_" + uniqueId;
                    
                    Put put = new Put(Bytes.toBytes(rowKeyStr));

                    // Add all columns (including the first one) to the 'cf' family
                    for (int i = 0; i < fields.length; i++) {
                        Object value = row.get(i);
                        if (value != null) {
                            put.addColumn(
                                    Bytes.toBytes("cf"),
                                    Bytes.toBytes(fields[i].name().replaceAll("[^a-zA-Z0-9]", "_")),
                                    Bytes.toBytes(value.toString())
                            );
                        }
                    }
                    batch.add(put);

                    if (batch.size() >= 1000) {
                        hbaseTable.put(batch);
                        batch.clear();
                    }
                }

                if (!batch.isEmpty()) {
                    hbaseTable.put(batch);
                }
            } catch (IOException e) {
                throw new RuntimeException("Error writing to HBase table: " + targetTable, e);
            }
        });
    }

    /**
     * Verifies if the target table exists and creates it if missing.
     * This logic runs on the Driver to ensure schema consistency before workers start.
     *
     * @param quorum zookeeper address
     * @param port zookeeper port
     * @param tableName table name
     */
    private void ensureTableExists(String quorum, String port, String tableName) {
        Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", quorum);
        hbaseConfig.set("hbase.zookeeper.property.clientPort", port);

        try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
             Admin admin = connection.getAdmin()) {

            TableName tn = TableName.valueOf(tableName);
            if (!admin.tableExists(tn)) {
                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tn)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf")).build())
                        .build();
                admin.createTable(tableDescriptor);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error ensuring HBase table exists: " + tableName, e);
        }
    }

    @Override
    public void saveResults(JavaRDD<Row> results, StructType schema, String table) {
        if (results == null) throw new IllegalArgumentException("Results RDD cannot be null.");
        if (schema == null) throw new IllegalArgumentException("Schema cannot be null.");

        Dataset<Row> df = spark.createDataFrame(results, schema);
        saveResults(df, table);
    }
}
