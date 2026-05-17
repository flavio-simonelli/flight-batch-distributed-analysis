package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.PercentileAlgorithm;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class to verify the consistency of results across different Spark backends (RDD, DataFrame, SQL).
 * It uses a hardcoded dummy dataset to ensure that all implementations produce identical outputs.
 */
public class QueryConsistencyTest {

    private static SparkSession spark;
    private static ApplicationConfig config;

    @BeforeAll
    public static void setup() {
        // Initialize a local Spark session for testing
        spark = SparkSession.builder()
                .appName("QueryConsistencyTest")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate();

        // Default configuration for tests
        config = new ApplicationConfig();
        config.setPercentileAlgorithm(PercentileAlgorithm.KLL);
    }

    @AfterAll
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    /**
     * Verifies consistency for Query 1: Monthly Performance Analyzer.
     */
    @Test
    public void testMonthlyPerformanceAnalyzerConsistency() {
        List<Row> data = new ArrayList<>();
        // Schema: YEAR, MONTH, DAY_OF_MONTH, OP_UNIQUE_CARRIER, CRS_DEP_TIME, DEP_DELAY, ARR_DELAY, CANCELLED, DIVERTED, CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY
        data.add(RowFactory.create(2025, 1, 1, "AA", 1000, 10.0, 15.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0));
        data.add(RowFactory.create(2025, 1, 2, "AA", 1100, 20.0, 25.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0));
        data.add(RowFactory.create(2025, 1, 3, "AA", 1200, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)); // Cancelled
        data.add(RowFactory.create(2025, 2, 1, "DL", 1000, 5.0, 5.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0));

        Dataset<Row> fullDf = spark.createDataFrame(data, FlightRepository.FLIGHT_SCHEMA);
        // Apply the same selection as loadData() in MonthlyPerformanceAnalyzer
        Dataset<Row> inputDf = fullDf.select("MONTH", "OP_UNIQUE_CARRIER", "DEP_DELAY", "CANCELLED");

        MonthlyPerformanceAnalyzer analyzer = new MonthlyPerformanceAnalyzer();

        // Run query on all backends
        List<Dataset<Row>> dfResultList = analyzer.runQueryDataFrame(inputDf, config);
        List<Dataset<Row>> sqlResultList = analyzer.runQuerySQL(inputDf, config, spark);
        List<Tuple2<JavaRDD<Row>, StructType>> rddResultList = analyzer.runQueryRDD(inputDf, config);

        List<Row> dfRows = dfResultList.get(0).collectAsList();
        List<Row> sqlRows = sqlResultList.get(0).collectAsList();
        List<Row> rddRows = spark.createDataFrame(rddResultList.get(0)._1, rddResultList.get(0)._2).collectAsList();

        assertEquals(dfRows.size(), sqlRows.size(), "DataFrame and SQL row counts should match");
        assertEquals(dfRows.size(), rddRows.size(), "DataFrame and RDD row counts should match");

        for (int i = 0; i < dfRows.size(); i++) {
            Row df = dfRows.get(i);
            Row sql = sqlRows.get(i);
            Row rdd = rddRows.get(i);

            // Compare Month and Carrier
            assertEquals(df.get(0), sql.get(0));
            assertEquals(df.get(1), sql.get(1));
            assertEquals(df.get(0), rdd.get(0));
            assertEquals(df.get(1), rdd.get(1));

            // Compare Metrics (Avg Delay and Cancellation Rate)
            assertEquals(df.getDouble(2), sql.getDouble(2), 0.01, "Avg delay should match between DF and SQL");
            assertEquals(df.getDouble(5), sql.getDouble(5), 0.01, "Cancellation rate should match between DF and SQL");
            
            assertEquals(df.getDouble(2), rdd.getDouble(2), 0.01, "Avg delay should match between DF and RDD");
            assertEquals(df.getDouble(5), rdd.getDouble(5), 0.01, "Cancellation rate should match between DF and RDD");
        }
    }

    /**
     * Verifies consistency for Query 2: Arrival Delay Ranking.
     */
    @Test
    public void testArrivalDelayRankingConsistency() {
        List<Row> data = new ArrayList<>();
        // We need > 500 rows for an airline to pass the internal filter
        for (int i = 0; i < 501; i++) {
            data.add(RowFactory.create(2025, 1, 1, "AA", 1000, 10.0, 20.0, 0.0, 0.0, 5.0, 0.0, 5.0, 0.0, 10.0));
        }
        for (int i = 0; i < 501; i++) {
            data.add(RowFactory.create(2025, 1, 1, "DL", 1000, 5.0, 10.0, 0.0, 0.0, 2.0, 0.0, 2.0, 0.0, 6.0));
        }

        Dataset<Row> fullDf = spark.createDataFrame(data, FlightRepository.FLIGHT_SCHEMA);
        // Apply the same selection as loadData() in ArrivalDelayRanking
        Dataset<Row> inputDf = fullDf.select("OP_UNIQUE_CARRIER", "ARR_DELAY", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "CANCELLED", "DIVERTED");

        ArrivalDelayRanking analyzer = new ArrivalDelayRanking();

        List<Dataset<Row>> dfResultList = analyzer.runQueryDataFrame(inputDf, config);
        List<Dataset<Row>> sqlResultList = analyzer.runQuerySQL(inputDf, config, spark);
        List<Tuple2<JavaRDD<Row>, StructType>> rddResultList = analyzer.runQueryRDD(inputDf, config);

        List<Row> dfRows = dfResultList.get(0).collectAsList();
        List<Row> sqlRows = sqlResultList.get(0).collectAsList();
        List<Row> rddRows = spark.createDataFrame(rddResultList.get(0)._1, rddResultList.get(0)._2).collectAsList();

        assertEquals(2, dfRows.size(), "Should have 2 airlines passing the 500 flights filter");
        assertEquals(dfRows.size(), sqlRows.size());
        assertEquals(dfRows.size(), rddRows.size());

        for (int i = 0; i < dfRows.size(); i++) {
            Row df = dfRows.get(i);
            Row sql = sqlRows.get(i);
            Row rdd = rddRows.get(i);

            assertEquals(df.getString(0), sql.getString(0));
            assertEquals(df.getString(0), rdd.getString(0));

            assertEquals(df.getLong(1), sql.getLong(1));
            assertEquals(df.getLong(1), rdd.getLong(1));

            assertEquals(df.getDouble(2), sql.getDouble(2), 0.01, "Avg arrival delay should match between DF and SQL");
            assertEquals(df.getDouble(2), rdd.getDouble(2), 0.01, "Avg arrival delay should match between DF and RDD");
        }
    }

    /**
     * Verifies consistency for Query 3: Hourly Delay Percentiles.
     * Note: Percentiles are estimated, so we use a wider tolerance for RDD (sketches) vs SQL/DF (percentile_approx).
     */
    @Test
    public void testHourlyDelayPercentilesConsistency() {
        List<Row> data = new ArrayList<>();
        // Adding enough data points to have meaningful percentiles at 10:00 for AA
        List<Double> delays = Arrays.asList(10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0);
        for (Double d : delays) {
            data.add(RowFactory.create(2025, 1, 1, "AA", 1000, d, d, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0));
        }

        Dataset<Row> fullDf = spark.createDataFrame(data, FlightRepository.FLIGHT_SCHEMA);
        // Apply the same selection as loadData() in HourlyDelayPercentiles
        Dataset<Row> inputDf = fullDf.select("OP_UNIQUE_CARRIER", "CRS_DEP_TIME", "DEP_DELAY", "CANCELLED");

        HourlyDelayPercentiles analyzer = new HourlyDelayPercentiles();

        // Test with KLL algorithm for RDD
        config.setPercentileAlgorithm(PercentileAlgorithm.KLL);

        List<Dataset<Row>> dfResultList = analyzer.runQueryDataFrame(inputDf, config);
        List<Dataset<Row>> sqlResultList = analyzer.runQuerySQL(inputDf, config, spark);
        List<Tuple2<JavaRDD<Row>, StructType>> rddResultList = analyzer.runQueryRDD(inputDf, config);

        // Verification of Hourly Percentiles (Result 1)
        List<Row> dfHourly = dfResultList.get(0).collectAsList();
        List<Row> sqlHourly = sqlResultList.get(0).collectAsList();
        List<Row> rddHourly = spark.createDataFrame(rddResultList.get(0)._1, rddResultList.get(0)._2).collectAsList();

        assertEquals(1, dfHourly.size());
        assertEquals(dfHourly.size(), sqlHourly.size());
        assertEquals(dfHourly.size(), rddHourly.size());

        // For exact matching values (p50 of 1..10 is 55.0 or similar depending on implementation)
        // SQL/DF use the same implementation, so they should be identical.
        assertEquals(dfHourly.get(0).getDouble(3), sqlHourly.get(0).getDouble(3), 0.01, "p50 should match between DF and SQL");
        
        // RDD uses sketches, so we expect some approximation error
        double p50_df = dfHourly.get(0).getDouble(3);
        double p50_rdd = rddHourly.get(0).getDouble(3);
        assertTrue(Math.abs(p50_df - p50_rdd) < 10.0, "p50 RDD sketch should be close to DF estimate");

        // Verification of Global Min/Max (Result 2)
        List<Row> dfGlobal = dfResultList.get(1).collectAsList();
        List<Row> rddGlobal = spark.createDataFrame(rddResultList.get(1)._1, rddResultList.get(1)._2).collectAsList();

        assertEquals(dfGlobal.get(0).getDouble(1), rddGlobal.get(0).getDouble(1), 0.01, "Global Min should match exactly");
        assertEquals(dfGlobal.get(0).getDouble(2), rddGlobal.get(0).getDouble(2), 0.01, "Global Max should match exactly");
    }
}
