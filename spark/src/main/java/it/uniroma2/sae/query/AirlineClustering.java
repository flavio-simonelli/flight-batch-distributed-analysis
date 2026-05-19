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
 * 1. Clusters airlines based on 8 operational features (multidimensional space).
 * 2. Projects the results into 2D space using PCA AFTER clustering for visualization.
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

    @Override
    protected List<Dataset<Row>> runQueryDataFrame(Dataset<Row> dataset, ApplicationConfig config) {
        
        // 1. Identify Top 10 Carriers
        Dataset<Row> topCarriers = dataset.groupBy("OP_UNIQUE_CARRIER")
                .count()
                .orderBy(col("count").desc())
                .limit(TOP_CARRIER_NUM)
                .select("OP_UNIQUE_CARRIER");

        // 2. Aggregate features (8 dimensions)
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

        String[] featureCols = {"avg_dep_delay", "avg_arr_delay", "cancellation_rate", 
                               "avg_carrier_delay", "avg_weather_delay", "avg_nas_delay", 
                               "avg_security_delay", "avg_late_aircraft_delay"};
        
        // 3. Scale and Cluster on 8D space
        VectorAssembler assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("raw_features");
        Dataset<Row> assembledData = assembler.transform(featuresRaw);

        StandardScaler scaler = new StandardScaler().setInputCol("raw_features").setOutputCol("features").setWithStd(true).setWithMean(true);
        Dataset<Row> scaledData = scaler.fit(assembledData).transform(assembledData);
        scaledData.cache();

        // 4. Find optimal K on 8D features
        ClusteringEvaluator evaluator = new ClusteringEvaluator().setFeaturesCol("features").setPredictionCol("prediction").setMetricName("silhouette");
        int bestK = 2;
        double bestScore = -1.0;
        for (int k = 2; k <= MAX_K; k++) {
            KMeans kmeans = new KMeans().setK(k).setSeed(42L).setFeaturesCol("features").setInitMode("k-means||");;
            KMeansModel model = kmeans.fit(scaledData);
            double score = evaluator.evaluate(model.transform(scaledData));
            System.out.println("K=" + k + " -> Silhouette Score: " + score);
            if (score > bestScore) { bestScore = score; bestK = k; }
        }
        System.out.println("Best K identified: " + bestK);

        // 5. Final Clustering and PCA Projection
        KMeans kmeansFinal = new KMeans().setK(bestK).setSeed(42L).setFeaturesCol("features").setInitMode("k-means||");
        KMeansModel modelFinal = kmeansFinal.fit(scaledData);
        Dataset<Row> finalPredictions = modelFinal.transform(scaledData);

        PCA pca = new PCA().setInputCol("features").setOutputCol("pca_features").setK(bestK);
        PCAModel pcaModel = pca.fit(finalPredictions);
        Dataset<Row> predictedWithPCA = pcaModel.transform(finalPredictions);

        Dataset<Row> finalOutput = predictedWithPCA
                .withColumn("pca_array", org.apache.spark.ml.functions.vector_to_array(col("pca_features"), "float64"))
                .withColumn("pca_x", col("pca_array").getItem(0))
                .withColumn("pca_y", col("pca_array").getItem(1))
                .drop("raw_features", "features", "pca_features", "pca_array");

        // 6. Extract Cluster Centers (8D) and project them to 2D
        SparkSession spark = dataset.sparkSession();
        List<Row> centerFeatureRows = new ArrayList<>();
        Vector[] centers = modelFinal.clusterCenters();
        for (int i = 0; i < centers.length; i++) {
             centerFeatureRows.add(RowFactory.create(i, centers[i]));
        }
        StructType centerFeatureSchema = new StructType(new StructField[]{
             DataTypes.createStructField("cluster", DataTypes.IntegerType, false),
             DataTypes.createStructField("features", org.apache.spark.ml.linalg.SQLDataTypes.VectorType(), false)
        });
        Dataset<Row> centers8D = spark.createDataFrame(centerFeatureRows, centerFeatureSchema);
        Dataset<Row> centers2D = pcaModel.transform(centers8D);
        List<Row> pcaCenterRows = centers2D.collectAsList();
        
        List<Row> finalCenterRows = new ArrayList<>();
        for (int i = 0; i < centers.length; i++) {
            double[] c8d = centers[i].toArray();
            Vector c2d = pcaCenterRows.get(i).getAs("pca_features");
            Object[] rowValues = new Object[c8d.length + 3];
            rowValues[0] = i;
            for (int j = 0; j < c8d.length; j++) rowValues[j + 1] = c8d[j];
            rowValues[c8d.length + 1] = c2d.toArray()[0]; // pca_x
            rowValues[c8d.length + 2] = c2d.toArray()[1]; // pca_y
            finalCenterRows.add(RowFactory.create(rowValues));
        }

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("cluster", DataTypes.IntegerType, false));
        for (String colName : featureCols) fields.add(DataTypes.createStructField(colName + "_center", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("pca_x", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("pca_y", DataTypes.DoubleType, false));
        Dataset<Row> centersDF = spark.createDataFrame(finalCenterRows, DataTypes.createStructType(fields));

        scaledData.unpersist();
        return Arrays.asList(finalOutput, centersDF);
    }

    @Override protected List<Dataset<Row>> runQuerySQL(Dataset<Row> f, ApplicationConfig c, SparkSession s) { throw new UnsupportedOperationException(); }
    @Override protected List<Tuple2<JavaRDD<Row>, StructType>> runQueryRDD(Dataset<Row> d, ApplicationConfig c) { throw new UnsupportedOperationException(); }
}
