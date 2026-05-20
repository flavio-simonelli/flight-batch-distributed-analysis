package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.linalg.SQLDataTypes;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.ml.functions.vector_to_array;
import static org.apache.spark.sql.functions.*;

/**
 * Implementation of Query 4: Airline Clustering.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 * This query groups airlines based on their operational performance features using K-Means.
 * 1. Clusters airlines based on multiple operational features (multidimensional space).
 * 2. Evaluates the optimal number of clusters (K) using the Silhouette Score.
 * 3. Projects the multidimensional results into a lower-dimensional space (e.g., 2D) 
 *    using PCA AFTER clustering, specifically for visualization purposes.
 */
public class AirlineClustering extends BaseQuery {

    // --- Algorithmic Configuration Constants ---
    private static final int TOP_CARRIER_NUM = 15;
    private static final int MIN_K = 2;
    private static final int MAX_K = 5;
    private static final long KMEANS_SEED = 42L;
    private static final String KMEANS_INIT_MODE = "k-means||";

    // --- Visualization Configuration Constants ---
    // Total dimensions of the final projection to show clusters
    private static final int CHART_DIMENSION = 2;

    @Override
    protected Dataset<Row> loadData(FlightRepository repository, ApplicationConfig config) {
        // Method-level constants for data loading (Algorithmically uninfluential)
        final String[] REQUIRED_COLUMNS = {
                "OP_UNIQUE_CARRIER", "DEP_DELAY", "ARR_DELAY", "CANCELLED", "DIVERTED",
                "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY"
        };
        
        String datasetFilename = config.getInput().getDatasetFilename();
        
        // Select only the necessary columns to optimize read performance
        Column[] cols = Arrays.stream(REQUIRED_COLUMNS).map(colName -> col(colName)).toArray(Column[]::new);
        return repository.getFlights(datasetFilename).select(cols);
    }

    /**
     * Executes the query using the Spark DataFrame API.
     * Computes the multidimensional clustering of airlines and projects centers and points via PCA.
     *
     * @param dataset the dataset to query
     * @param config the application configuration containing input/output paths
     * @return a list containing two Datasets: the first with airline assignments and PCA coordinates, 
     *         the second with cluster centers and their PCA coordinates.
     */
    @Override
    protected List<Dataset<Row>> runQueryDataFrame(Dataset<Row> dataset, ApplicationConfig config) {
        
        // --- Method-Level Constants (Algorithmically uninfluential) ---
        final String CARRIER_COL = "OP_UNIQUE_CARRIER";
        final String CANCELLED_COL = "CANCELLED";
        final String DIVERTED_COL = "DIVERTED";
        final String DEP_DELAY_COL = "DEP_DELAY";
        final String ARR_DELAY_COL = "ARR_DELAY";
        
        final String RAW_FEATURES_COL = "raw_features";
        final String FEATURES_COL = "features";
        final String PCA_FEATURES_COL = "pca_features";
        final String PCA_ARRAY_COL = "pca_array";
        final String PREDICTION_COL = "prediction";
        final String CLUSTER_COL = "cluster";
        
        final String[] PCA_COLS = {"pca_x", "pca_y", "pca_z", "pca_w", "pca_v", "pca_u"};

        // PHASE 1: Identify Top Carriers
        // Filter the dataset to include only the top N carriers by flight volume to ensure statistical significance.
        Dataset<Row> topCarriers = dataset.groupBy(CARRIER_COL)
                .count()
                .orderBy(col("count").desc())
                .limit(TOP_CARRIER_NUM)
                .select(CARRIER_COL);

        // PHASE 2: Aggregate Features
        // Computes average delays, cancellation rates, and standard deviations for the selected carriers.
        Dataset<Row> featuresRaw = dataset
                .join(topCarriers, CARRIER_COL)
                .groupBy(CARRIER_COL)
                .agg(
                        avg(when(col(CANCELLED_COL).equalTo(0), col(DEP_DELAY_COL))).as("avg_dep_delay"),
                        avg(when(col(CANCELLED_COL).equalTo(0), col(ARR_DELAY_COL))).as("avg_arr_delay"),

                        // Recovered time in fly
                        avg(when(col(CANCELLED_COL).equalTo(0), col(ARR_DELAY_COL).minus(col(DEP_DELAY_COL)))).as("avg_flight_makeup"),
                        stddev(ARR_DELAY_COL).as("arr_delay_stddev"),
                        (sum(col(DIVERTED_COL)).divide(count("*"))).as("diverted_rate"),

                        (sum(col(CANCELLED_COL)).divide(count("*"))).as("cancellation_rate"),
                        avg("CARRIER_DELAY").as("avg_carrier_delay"),
                        avg("WEATHER_DELAY").as("avg_weather_delay"),
                        avg("NAS_DELAY").as("avg_nas_delay"),
                        avg("SECURITY_DELAY").as("avg_security_delay"),
                        avg("LATE_AIRCRAFT_DELAY").as("avg_late_aircraft_delay")
                )
                .na().fill(0.0);

        final String[] featureCols = {
                "avg_dep_delay", "avg_arr_delay", "cancellation_rate",
                "avg_flight_makeup", "arr_delay_stddev", "diverted_rate",
                "avg_carrier_delay", "avg_weather_delay", "avg_nas_delay",
                "avg_security_delay", "avg_late_aircraft_delay"
        };
        
        // PHASE 3: Feature Assembly and Scaling
        // Combine individual feature columns into a single vector and standardize them (mean=0, std=1).
        VectorAssembler assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol(RAW_FEATURES_COL);
        Dataset<Row> assembledData = assembler.transform(featuresRaw);

        StandardScaler scaler = new StandardScaler()
                .setInputCol(RAW_FEATURES_COL)
                .setOutputCol(FEATURES_COL)
                .setWithStd(true)
                .setWithMean(true);
        Dataset<Row> scaledData = scaler.fit(assembledData).transform(assembledData);
        scaledData.cache();

        // PHASE 4: Optimal K Selection (Silhouette Analysis)
        // Iteratively test values of K to find the configuration with the highest Silhouette score.
        ClusteringEvaluator evaluator = new ClusteringEvaluator()
                .setFeaturesCol(FEATURES_COL)
                .setPredictionCol(PREDICTION_COL)
                .setMetricName("silhouette");
                
        int bestK = MIN_K;
        double bestScore = -1.0;
        
        logger.info("--- Finding optimal K using Silhouette Score ---");
        for (int k = MIN_K; k <= MAX_K; k++) {
            KMeans kmeans = new KMeans()
                    .setK(k)
                    .setSeed(KMEANS_SEED)
                    .setFeaturesCol(FEATURES_COL)
                    .setInitMode(KMEANS_INIT_MODE);
            KMeansModel model = kmeans.fit(scaledData);
            double score = evaluator.evaluate(model.transform(scaledData));
            logger.info("K={} -> Silhouette Score: {}", k, score);
            if (score > bestScore) { 
                bestScore = score; 
                bestK = k; 
            }
        }
        logger.info("Best K identified: {}", bestK);

        // PHASE 5: Final Clustering and PCA Projection
        // Run K-Means with the optimal K on the multidimensional space, then project the result to lower dimensions.
        KMeans kmeansFinal = new KMeans()
                .setK(bestK)
                .setSeed(KMEANS_SEED)
                .setFeaturesCol(FEATURES_COL)
                .setInitMode(KMEANS_INIT_MODE);
        KMeansModel modelFinal = kmeansFinal.fit(scaledData);
        Dataset<Row> finalPredictions = modelFinal.transform(scaledData);

        PCA pca = new PCA()
                .setInputCol(FEATURES_COL)
                .setOutputCol(PCA_FEATURES_COL)
                .setK(CHART_DIMENSION);
        PCAModel pcaModel = pca.fit(finalPredictions);
        Dataset<Row> predictedWithPCA = pcaModel.transform(finalPredictions);

        // Extract individual PCA components into distinct columns for easier database storage/querying
        Dataset<Row> finalOutput = appendPcaColumns(predictedWithPCA, PCA_ARRAY_COL, PCA_FEATURES_COL, PCA_COLS)
                .drop(RAW_FEATURES_COL, FEATURES_COL, PCA_FEATURES_COL, PCA_ARRAY_COL);

        // PHASE 6: Extract and Project Cluster Centers
        // Extract the original centroids and project them using the same PCA model.
        SparkSession spark = dataset.sparkSession();
        List<Row> centerFeatureRows = new ArrayList<>();
        Vector[] centers = modelFinal.clusterCenters();
        
        for (int i = 0; i < centers.length; i++) {
             centerFeatureRows.add(RowFactory.create(i, centers[i]));
        }
        
        StructType centerFeatureSchema = new StructType(new StructField[]{
             DataTypes.createStructField(CLUSTER_COL, DataTypes.IntegerType, false),
             DataTypes.createStructField(FEATURES_COL, SQLDataTypes.VectorType(), false)
        });
        
        Dataset<Row> centersND = spark.createDataFrame(centerFeatureRows, centerFeatureSchema);
        Dataset<Row> centersPCA = pcaModel.transform(centersND);
        List<Row> pcaCenterRows = centersPCA.collectAsList();
        
        List<Row> finalCenterRows = new ArrayList<>();
        for (int i = 0; i < centers.length; i++) {
            double[] originalFeatures = centers[i].toArray();
            Vector pcaFeatures = pcaCenterRows.get(i).getAs(PCA_FEATURES_COL);
            
            Object[] rowValues = new Object[originalFeatures.length + CHART_DIMENSION + 1];
            rowValues[0] = i; // Cluster ID
            
            // Append original features
            for (int j = 0; j < originalFeatures.length; j++) {
                rowValues[j + 1] = originalFeatures[j];
            }
            
            // Append projected PCA features
            for(int j = 0; j < CHART_DIMENSION; j++) {
                rowValues[originalFeatures.length + j + 1] = pcaFeatures.toArray()[j];
            }
            
            finalCenterRows.add(RowFactory.create(rowValues));
        }

        // Dynamically build the schema for the centers dataframe
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(CLUSTER_COL, DataTypes.IntegerType, false));
        for (String colName : featureCols) {
            fields.add(DataTypes.createStructField(colName + "_center", DataTypes.DoubleType, false));
        }
        for (int i = 0; i < CHART_DIMENSION; i++) {
            fields.add(DataTypes.createStructField(PCA_COLS[i], DataTypes.DoubleType, false));
        }
        
        Dataset<Row> centersDF = spark.createDataFrame(finalCenterRows, DataTypes.createStructType(fields));

        scaledData.unpersist();
        return Arrays.asList(finalOutput, centersDF);
    }
    
    /**
     * Helper method to expand the PCA vector into distinct columns.
     * Prevents code duplication when dynamically extracting up to N dimensions.
     *
     * @param dataset the dataset containing the PCA vector
     * @param arrayColName temporary column name for the array
     * @param pcaFeaturesCol the source vector column name
     * @param pcaCols array of target column names
     * @return a new dataset with the PCA dimensions as separate double columns
     */
    private Dataset<Row> appendPcaColumns(Dataset<Row> dataset, String arrayColName, String pcaFeaturesCol, String[] pcaCols) {
        Dataset<Row> result = dataset.withColumn(arrayColName, vector_to_array(col(pcaFeaturesCol), "float64"));
        for (int i = 0; i < CHART_DIMENSION; i++) {
            result = result.withColumn(pcaCols[i], col(arrayColName).getItem(i));
        }
        return result;
    }

    /**
     * Executes the query using the Spark SQL API.
     * @throws UnsupportedOperationException as clustering is heavily dependent on MLlib DataFrame API.
     */
    @Override 
    protected List<Dataset<Row>> runQuerySQL(Dataset<Row> flights, ApplicationConfig config, SparkSession spark) { 
        throw new UnsupportedOperationException("Clustering is only supported via DataFrame API."); 
    }
    
    /**
     * Executes the query using the Spark RDD API.
     * @throws UnsupportedOperationException as clustering is heavily dependent on MLlib DataFrame API.
     */
    @Override 
    protected List<Tuple2<JavaRDD<Row>, StructType>> runQueryRDD(Dataset<Row> dataset, ApplicationConfig config) { 
        throw new UnsupportedOperationException("Clustering is only supported via DataFrame API."); 
    }
}
