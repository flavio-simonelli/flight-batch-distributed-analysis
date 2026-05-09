package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Implementation of Query 1: Monthly Performance Analyzer.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 */
public class MonthlyPerformanceAnalyzer extends BaseQuery {

    @Override
    protected Dataset<Row> runQuery(FlightRepository repository, ApplicationConfig config) {
        Dataset<Row> result = null;
        switch (2) {
            case 1:
                result = runQueryDataFrame(repository, config);
            case 2:
                result = runQueryRDD(repository, config);
        }
        return result;
    }

    protected Dataset<Row> runQueryDataFrame(FlightRepository repository, ApplicationConfig config) {

        String datasetFilename = config.getInput().getDatasetFilename();
        
        Dataset<RawFlight> flights = repository.getFlightsOfAirlines(datasetFilename, "AA", "DL");
        System.out.println("=== Dataset caricato ===");
        flights.show(5);

        Dataset<Row> result = flights
                .groupBy("month", "opUniqueCarrier")
                .agg(
                        round(avg(when(col("cancelled").equalTo(0), col("depDelay"))), 2).as("dep-delay-mean"),
                        round(min(when(col("cancelled").equalTo(0), col("depDelay"))), 2).as("dep-delay-min"),
                        round(max(when(col("cancelled").equalTo(0), col("depDelay"))), 2).as("dep-delay-max"),
                        round(sum(col("cancelled")).divide(count("*")).multiply(100), 2).as("cancellation-rate")
                )
                .orderBy("month", "opUniqueCarrier");
        result.show();
        
        return result;
    }

    protected Dataset<Row> runQueryRDD(FlightRepository repository, ApplicationConfig config) {

        String datasetFilename = config.getInput().getDatasetFilename();
        if (datasetFilename == null || datasetFilename.isEmpty()) {
            throw new IllegalArgumentException("datasetFilename is not defined in config.yml");
        }

        JavaRDD<RawFlight> flights = repository.getFlightsOfAirlines(datasetFilename, "AA", "DL").javaRDD();
        System.out.println("=== Dataset caricato ===");
        // stampa delle prime 5 righe
        List<RawFlight> firstRows = flights.take(5);
        firstRows.forEach(System.out::println);

        // FASE 1: Map
        JavaPairRDD<Tuple2<String, Integer>, double[]> mappedRDD = flights.mapToPair(flight -> {
            // Chiave: (Compagnia, Mese)
            Tuple2<String, Integer> key = new Tuple2<>(flight.getOpUniqueCarrier(), flight.getMonth());
            // [0: SumDelay, 1: MaxDelay, 2: MinDelay, 3: NotCancelledCount, 4: TotalCount]
            double[] values = new double[5];
            boolean isCancelled = (flight.getCancelled() != null && flight.getCancelled() == true);
            values[4] = 1.0; // TotalCount
            if (isCancelled) {
                values[0] = 0.0; // Delay in SumDelay è 0 se il volo è stato cancellato
                values[1] = -Double.MAX_VALUE; // Elemento neutro per il MAX
                values[2] = Double.MAX_VALUE;  // Elemento neutro per il MIN
                values[3] = 0.0; // NotCancelledCount
            } else {
                double delay = (flight.getDepDelay() != null) ? flight.getDepDelay() : 0.0;
                values[0] = delay;
                values[1] = delay;
                values[2] = delay;
                values[3] = 1.0;
            }

            return new Tuple2<>(key, values);
        });

        // FASE 2: Reduce
        JavaPairRDD<Tuple2<String, Integer>, double[]> reducedRDD = mappedRDD.reduceByKey((a, b) -> {
            double[] res = new double[5];
            res[0] = a[0] + b[0]; // Somma dei ritardi
            res[1] = Math.max(a[1], b[1]); // Massimo ritardo
            res[2] = Math.min(a[2], b[2]); // Minimo ritardo
            res[3] = a[3] + b[3]; // Somma voli non cancellati
            res[4] = a[4] + b[4]; // Somma voli totali

            return res;
        });

        // FASE 3: Map
        JavaRDD<Row> rowRDD = reducedRDD.map(tuple -> {
            Tuple2<String, Integer> key = tuple._1;
            double[] stats = tuple._2;

            String carrier = key._1;
            Integer month = key._2;

            double totalFlights = stats[4];
            double notCancelledFlights = stats[3];
            double cancelledFlights = totalFlights - notCancelledFlights;

            double avgDelay = (notCancelledFlights > 0) ? (stats[0] / notCancelledFlights) : 0.0;
            double maxDelay = (notCancelledFlights > 0) ? stats[1] : 0.0;
            double minDelay = (notCancelledFlights > 0) ? stats[2] : 0.0;
            double cancellationRate = (totalFlights > 0) ? (cancelledFlights / totalFlights) * 100 : 0.0;

            return org.apache.spark.sql.RowFactory.create(month, carrier, avgDelay, minDelay, maxDelay, cancellationRate);
        });

        java.util.List<org.apache.spark.sql.types.StructField> fields = new java.util.ArrayList<>();
        fields.add(org.apache.spark.sql.types.DataTypes.createStructField("month", org.apache.spark.sql.types.DataTypes.IntegerType, false));
        fields.add(org.apache.spark.sql.types.DataTypes.createStructField("opUniqueCarrier", org.apache.spark.sql.types.DataTypes.StringType, false));
        fields.add(org.apache.spark.sql.types.DataTypes.createStructField("dep-delay-mean", org.apache.spark.sql.types.DataTypes.DoubleType, false));
        fields.add(org.apache.spark.sql.types.DataTypes.createStructField("dep-delay-min", org.apache.spark.sql.types.DataTypes.DoubleType, false));
        fields.add(org.apache.spark.sql.types.DataTypes.createStructField("dep-delay-max", org.apache.spark.sql.types.DataTypes.DoubleType, false));
        fields.add(org.apache.spark.sql.types.DataTypes.createStructField("cancellation-rate", org.apache.spark.sql.types.DataTypes.DoubleType, false));

        org.apache.spark.sql.types.StructType schema = org.apache.spark.sql.types.DataTypes.createStructType(fields);

        Dataset<Row> result = spark.createDataFrame(rowRDD, schema).orderBy("month", "opUniqueCarrier");

        result.show();

        return result;
    }
}
