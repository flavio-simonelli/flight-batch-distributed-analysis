package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.PercentileAlgorithm;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class to verify the consistency of results across different Spark backends (RDD, DataFrame, SQL).
 * It uses a real dataset to ensure that all implementations produce identical outputs.
 */
public class QueryConsistencyTest {

    private static SparkSession spark;
    private static ApplicationConfig config;
    private static Dataset<Row> fullDf;
    private static boolean datasetPresent = false;

    private final static Double DELTA_TOLERANCE = 0.01;
    private final static Double PERCENTILES_TOLERANCE = 10.00;

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

        // Load real data for testing
        String dataPath = "../data/conv/flights.parquets";
        File datasetFile = new File(dataPath);
        
        if (datasetFile.exists()) {
            fullDf = spark.read().schema(FlightRepository.FLIGHT_SCHEMA).parquet(dataPath).cache();
            datasetPresent = true;
        }
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
        Assumptions.assumeTrue(datasetPresent, "Dataset not found at ../data/conv/flights.parquets, skipping test.");

        // Apply the same selection as loadData() in MonthlyPerformanceAnalyzer
        Dataset<Row> inputDf = fullDf.select("MONTH", "OP_UNIQUE_CARRIER", "DEP_DELAY", "CANCELLED");

        MonthlyPerformanceAnalyzer analyzer = new MonthlyPerformanceAnalyzer();

        // Run query on all backends
        List<Dataset<Row>> dfResultList = analyzer.runQueryDataFrame(inputDf, config);
        List<Dataset<Row>> sqlResultList = analyzer.runQuerySQL(inputDf, config, spark);
        List<Tuple2<JavaRDD<Row>, StructType>> rddResultList = analyzer.runQueryRDD(inputDf, config);

        List<Row> dfRows = dfResultList.get(0).orderBy("month", "airline").collectAsList();
        List<Row> sqlRows = sqlResultList.get(0).orderBy("month", "airline").collectAsList();
        List<Row> rddRows = spark.createDataFrame(rddResultList.get(0)._1, rddResultList.get(0)._2).orderBy("month", "airline").collectAsList();

        assertFalse(dfRows.isEmpty(), "Should have some results with real data");
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

            // Compare Metrics
            assertEquals(df.getDouble(2), sql.getDouble(2), DELTA_TOLERANCE, "Avg delay should match between DF and SQL");
            assertEquals(df.getDouble(3), sql.getDouble(3), DELTA_TOLERANCE, "Min delay should match between DF and SQL");
            assertEquals(df.getDouble(4), sql.getDouble(4), DELTA_TOLERANCE, "Max delay should match between DF and SQL");
            assertEquals(df.getDouble(5), sql.getDouble(5), DELTA_TOLERANCE, "Cancellation rate should match between DF and SQL");
            
            assertEquals(df.getDouble(2), rdd.getDouble(2), DELTA_TOLERANCE, "Avg delay should match between DF and RDD");
            assertEquals(df.getDouble(3), rdd.getDouble(3), DELTA_TOLERANCE, "Min delay should match between DF and RDD");
            assertEquals(df.getDouble(4), rdd.getDouble(4), DELTA_TOLERANCE, "Max delay should match between DF and RDD");
            assertEquals(df.getDouble(5), rdd.getDouble(5), DELTA_TOLERANCE, "Cancellation rate should match between DF and RDD");
        }
    }

    /**
     * Verifies consistency for Query 2: Arrival Delay Ranking.
     */
    @Test
    public void testArrivalDelayRankingConsistency() {
        Assumptions.assumeTrue(datasetPresent, "Dataset not found, skipping test.");

        // Apply the same selection as loadData() in ArrivalDelayRanking
        Dataset<Row> inputDf = fullDf.select("OP_UNIQUE_CARRIER", "ARR_DELAY", "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY", "CANCELLED", "DIVERTED");

        ArrivalDelayRanking analyzer = new ArrivalDelayRanking();

        List<Dataset<Row>> dfResultList = analyzer.runQueryDataFrame(inputDf, config);
        List<Dataset<Row>> sqlResultList = analyzer.runQuerySQL(inputDf, config, spark);
        List<Tuple2<JavaRDD<Row>, StructType>> rddResultList = analyzer.runQueryRDD(inputDf, config);

        List<Row> dfRows = dfResultList.get(0).orderBy("carrier").collectAsList();
        List<Row> sqlRows = sqlResultList.get(0).orderBy("carrier").collectAsList();
        List<Row> rddRows = spark.createDataFrame(rddResultList.get(0)._1, rddResultList.get(0)._2).orderBy("carrier").collectAsList();

        assertFalse(dfRows.isEmpty(), "Should have some airlines passing the 500 flights filter in real data");
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

            assertEquals(df.getDouble(2), sql.getDouble(2), DELTA_TOLERANCE, "Avg arrival delay should match between DF and SQL");
            assertEquals(df.getDouble(2), rdd.getDouble(2), DELTA_TOLERANCE, "Avg arrival delay should match between DF and RDD");

            assertEquals(df.getDouble(3), sql.getDouble(3), DELTA_TOLERANCE, "Avg carrier delay should match between DF and SQL");
            assertEquals(df.getDouble(3), rdd.getDouble(3), DELTA_TOLERANCE, "Avg carrier delay should match between DF and SQL");

            assertEquals(df.getDouble(4), sql.getDouble(4), DELTA_TOLERANCE, "Avg weather delay should match between DF and SQL");
            assertEquals(df.getDouble(4), rdd.getDouble(4), DELTA_TOLERANCE, "Avg weather delay should match between DF and SQL");

            assertEquals(df.getDouble(5), sql.getDouble(5), DELTA_TOLERANCE, "Avg NAS delay should match between DF and SQL");
            assertEquals(df.getDouble(5), rdd.getDouble(5), DELTA_TOLERANCE, "Avg NAS delay should match between DF and SQL");

            assertEquals(df.getDouble(6), sql.getDouble(6), DELTA_TOLERANCE, "Avg security delay should match between DF and SQL");
            assertEquals(df.getDouble(6), rdd.getDouble(6), DELTA_TOLERANCE, "Avg security delay should match between DF and SQL");

            assertEquals(df.getDouble(7), sql.getDouble(7), DELTA_TOLERANCE, "Avg late aircraft delay should match between DF and SQL");
            assertEquals(df.getDouble(7), rdd.getDouble(7), DELTA_TOLERANCE, "Avg late aircraft delay should match between DF and SQL");
        }
    }

    /**
     * Verifies consistency for Query 3: Hourly Delay Percentiles.
     * Percentiles are estimated, so we use a wider tolerance for RDD vs SQL/DF.
     */
    @Test
    public void testHourlyDelayPercentilesConsistency() {
        Assumptions.assumeTrue(datasetPresent, "Dataset not found, skipping test.");

        // Apply the same selection as loadData() in HourlyDelayPercentiles
        Dataset<Row> inputDf = fullDf.select("OP_UNIQUE_CARRIER", "CRS_DEP_TIME", "DEP_DELAY", "CANCELLED");

        HourlyDelayPercentiles analyzer = new HourlyDelayPercentiles();

        // Test with KLL algorithm for RDD
        config.setPercentileAlgorithm(PercentileAlgorithm.KLL);

        List<Dataset<Row>> dfResultList = analyzer.runQueryDataFrame(inputDf, config);
        List<Dataset<Row>> sqlResultList = analyzer.runQuerySQL(inputDf, config, spark);
        List<Tuple2<JavaRDD<Row>, StructType>> rddResultList = analyzer.runQueryRDD(inputDf, config);

        // Verification of Hourly Percentiles (Result 1)
        List<Row> dfHourly = dfResultList.get(0).orderBy("airline", "hour").collectAsList();
        List<Row> sqlHourly = sqlResultList.get(0).orderBy("airline", "hour").collectAsList();
        List<Row> rddHourly = spark.createDataFrame(rddResultList.get(0)._1, rddResultList.get(0)._2).orderBy("airline", "hour").collectAsList();

        assertFalse(dfHourly.isEmpty(), "Should have some hourly percentiles results");
        assertEquals(dfHourly.size(), sqlHourly.size());
        assertEquals(dfHourly.size(), rddHourly.size());

        for (int i = 0; i < dfHourly.size(); i++) {
            // SQL/DF use the same implementation, so they should be identical.
            assertEquals(dfHourly.get(i).getDouble(3), sqlHourly.get(i).getDouble(3), DELTA_TOLERANCE, "p50 should match between DF and SQL");
            
            // RDD uses sketches, so we expect some approximation error. 
            // With real data and potentially high variance, we use a more relaxed tolerance.
            double p50_df = dfHourly.get(i).getDouble(3);
            double p50_rdd = rddHourly.get(i).getDouble(3);
            assertTrue(Math.abs(p50_df - p50_rdd) < PERCENTILES_TOLERANCE, "p50 RDD sketch should be close to DF estimate");
        }

        // Verification of Global Min/Max (Result 2)
        List<Row> dfGlobal = dfResultList.get(1).orderBy("airline").collectAsList();
        List<Row> rddGlobal = spark.createDataFrame(rddResultList.get(1)._1, rddResultList.get(1)._2).orderBy("airline").collectAsList();

        assertEquals(dfGlobal.get(0).getDouble(1), rddGlobal.get(0).getDouble(1), DELTA_TOLERANCE, "Global Min should match exactly");
        assertEquals(dfGlobal.get(0).getDouble(2), rddGlobal.get(0).getDouble(2), DELTA_TOLERANCE, "Global Max should match exactly");
    }
}
