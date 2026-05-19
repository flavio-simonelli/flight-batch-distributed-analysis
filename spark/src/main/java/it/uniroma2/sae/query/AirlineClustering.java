package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
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

import static org.apache.spark.sql.functions.*;

/**
 * Implementation of Query 4: Airline Clustering.
 * Uses Spark MLlib to group airlines based on their operational performance features.
 */
public class AirlineClustering extends BaseQuery {

    private final static int TOP_CARRIER_NUM = 10;
    private final static int MAX_K = 8;

    @Override
    protected Dataset<Row> loadData(FlightRepository repository, ApplicationConfig config) {
        String datasetFilename = config.getInput().getDatasetFilename();
        return repository.getFlights(datasetFilename)
                .select("OP_UNIQUE_CARRIER", "DEP_DELAY", "ARR_DELAY", "CANCELLED", 
                        "CARRIER_DELAY", "WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY");
    }

    /**
     * Executes the clustering query using Spark MLlib.
     * 1. Aggregates performance features for the top 10 airlines.
     * 2. Normalizes features using StandardScaler.
     * 3. Finds the optimal number of clusters (K) using the Silhouette Score.
     * 4. Applies K-Means clustering.
     */
    @Override
    protected List<Dataset<Row>> runQueryDataFrame(Dataset<Row> dataset, ApplicationConfig config) {
        
        // 1. Identify Top 10 Carriers by flight count to ensure statistical significance
        Dataset<Row> topCarriers = dataset.groupBy("OP_UNIQUE_CARRIER")
                .count()
                .orderBy(col("count").desc())
                .limit(TOP_CARRIER_NUM)
                .select("OP_UNIQUE_CARRIER");

        // 2. Aggregate features for these top carriers
        Dataset<Row> featuresRaw = dataset
                .join(topCarriers, "OP_UNIQUE_CARRIER")
                .groupBy("OP_UNIQUE_CARRIER")
                .agg(
                        avg(when(col("CANCELLED").equalTo(0), col("DEP_DELAY"))).as("avg_dep_delay"),
                        avg(when(col("CANCELLED").equalTo(0), col("ARR_DELAY"))).as("avg_arr_delay"),
                        (sum(col("CANCELLED")).divide(count("*"))).as("cancellation_rate"),
                        avg("CARRIER_DELAY").as("avg_carrier_delay"),
                        avg("WEATHER_DELAY").as("avg_weather_delay"),
                        avg("NAS_DELAY").as("avg_nas_delay"),
                        avg("SECURITY_DELAY").as("avg_security_delay"),
                        avg("LATE_AIRCRAFT_DELAY").as("avg_late_aircraft_delay")
                )
                .na().fill(0.0);

        // 3. Assemble features into a single vector column
        String[] featureCols = {"avg_dep_delay", "avg_arr_delay", "cancellation_rate", 
                               "avg_carrier_delay", "avg_weather_delay", "avg_nas_delay", 
                               "avg_security_delay", "avg_late_aircraft_delay"};
        
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(featureCols)
                .setOutputCol("raw_features");

        Dataset<Row> assembledData = assembler.transform(featuresRaw);

        // 4. Scale features (Standardization: (x - mean) / std)
        StandardScaler scaler = new StandardScaler()
                .setInputCol("raw_features")
                .setOutputCol("features")
                .setWithStd(true)
                .setWithMean(true);

        Dataset<Row> scaledData = scaler.fit(assembledData).transform(assembledData);
        scaledData.cache(); // Cache for the iterative evaluation loop

        // 5. Determine optimal K using Silhouette Score (Elbow method alternative)
        ClusteringEvaluator evaluator = new ClusteringEvaluator()
                .setFeaturesCol("features")
                .setPredictionCol("prediction")
                .setMetricName("silhouette");

        int bestK = 2;
        double bestScore = -1.0;
        
        System.out.println("--- Finding optimal K using Silhouette Score ---");
        for (int k = 2; k <= MAX_K; k++) {
            KMeans kmeans = new KMeans().setK(k).setSeed(42L).setFeaturesCol("features").setInitMode("k-means||");
            KMeansModel model = kmeans.fit(scaledData);
            Dataset<Row> predictions = model.transform(scaledData);
            double score = evaluator.evaluate(predictions);
            System.out.println("K=" + k + " -> Silhouette Score: " + score);
            if (score > bestScore) {
                bestScore = score;
                bestK = k;
            }
        }
        System.out.println("Best K identified: " + bestK);

        // 6. Final Clustering with the best K
        KMeans kmeansFinal = new KMeans().setK(bestK).setSeed(42L).setFeaturesCol("features").setInitMode("k-means||");
        KMeansModel modelFinal = kmeansFinal.fit(scaledData);
        Dataset<Row> finalPredictions = modelFinal.transform(scaledData);

        // 7. Extract Cluster Centers for interpretation
        SparkSession spark = dataset.sparkSession();
        List<Row> centerRows = new ArrayList<>();
        Vector[] centers = modelFinal.clusterCenters();
        for (int i = 0; i < centers.length; i++) {
            double[] centerValues = centers[i].toArray();
            Object[] rowValues = new Object[centerValues.length + 1];
            rowValues[0] = i;
            for (int j = 0; j < centerValues.length; j++) {
                rowValues[j + 1] = centerValues[j];
            }
            centerRows.add(RowFactory.create(rowValues));
        }

        // Define schema for centers (for visualization in Grafana)
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("cluster", DataTypes.IntegerType, false));
        for (String colName : featureCols) {
            fields.add(DataTypes.createStructField(colName + "_center", DataTypes.DoubleType, false));
        }
        Dataset<Row> centersDF = spark.createDataFrame(centerRows, DataTypes.createStructType(fields));

        scaledData.unpersist();

        // Return: 1. Airline predictions, 2. Cluster centers
        return Arrays.asList(
                finalPredictions.drop("raw_features", "features"), 
                centersDF
        );
    }

    @Override
    protected List<Dataset<Row>> runQuerySQL(Dataset<Row> flights, ApplicationConfig config, SparkSession spark) {
        throw new UnsupportedOperationException("Clustering is only supported via DataFrame API.");
    }

    @Override
    protected List<Tuple2<JavaRDD<Row>, StructType>> runQueryRDD(Dataset<Row> dataset, ApplicationConfig config) {
        throw new UnsupportedOperationException("Clustering is only supported via DataFrame API.");
    }
}
