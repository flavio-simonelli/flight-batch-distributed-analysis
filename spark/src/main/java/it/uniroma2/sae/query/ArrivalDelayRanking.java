package it.uniroma2.sae.query;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.repository.FlightRepository;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * Implementation of Query 2: Arrival Delay Ranking.
 * Inherits the initialization and data loading boilerplate from {@link BaseQuery}.
 */
public class ArrivalDelayRanking extends BaseQuery {

    @Override
    protected Dataset<Row> runQueryDataFrame(FlightRepository repository, ApplicationConfig config) {
        String datasetFilename = config.getInput().getDatasetFilename();

        Dataset<RawFlight> flights = repository.getFlights(datasetFilename);
        System.out.println("=== Dataset caricato ===");
        flights.show(5);

        Dataset<Row> result = flights
                .groupBy("opUniqueCarrier")
                .agg(
                        count(when(col("cancelled").equalTo(0).and(col("diverted").equalTo(0)), 1)).as("num_flights"),
                        round(avg(col("arrDelay")), 2).as("arrdelay_mean"),
                        round(avg(col("carrierDelay")), 2).as("carrier_delay_mean"),
                        round(avg(col("weatherDelay")), 2).as("weather_delay_mean"),
                        round(avg(col("nasDelay")), 2).as("nas_delay_mean"),
                        round(avg(col("securityDelay")), 2).as("security_delay_mean"),
                        round(avg(col("lateAircraftDelay")), 2).as("late_aircraft_delay_mean")
                )
                .filter(col("num_flights").gt(500))
                .orderBy(col("arrdelay_mean").desc())
                .limit(10);
        result.show();

        return result;
    }

    @Override
    protected Dataset<Row> runQuerySQL(FlightRepository repository, ApplicationConfig config, SparkSession spark) {
        String datasetFilename = config.getInput().getDatasetFilename();
        Dataset<RawFlight> flights = repository.getFlights(datasetFilename);

        flights.createOrReplaceTempView("flights");

        String sqlQuery = "SELECT opUniqueCarrier, " +
                "COUNT(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 END) AS num_flights, " +
                "ROUND(AVG(arrDelay), 2) AS arrdelay_mean, " +
                "ROUND(AVG(carrierDelay), 2) AS carrier_delay_mean, " +
                "ROUND(AVG(weatherDelay), 2) AS weather_delay_mean, " +
                "ROUND(AVG(nasDelay), 2) AS nas_delay_mean, " +
                "ROUND(AVG(securityDelay), 2) AS security_delay_mean, " +
                "ROUND(AVG(lateAircraftDelay), 2) AS late_aircraft_delay_mean " +
                "FROM flights " +
                "GROUP BY opUniqueCarrier " +
                "HAVING num_flights > 500 " +
                "ORDER BY arrdelay_mean DESC " +
                "LIMIT 10";

        Dataset<Row> result = spark.sql(sqlQuery);
        result.show();
        return result;
    }

    @Override
    protected JavaRDD<Row> runQueryRDD(FlightRepository repository, ApplicationConfig config) {
        String datasetFilename = config.getInput().getDatasetFilename();
        JavaRDD<RawFlight> flights = repository.getFlights(datasetFilename).javaRDD();

        // scartiamo i voli cancellati o dirottati
        JavaRDD<RawFlight> validFlights = flights.filter(flight -> {
            boolean isCancelled = flight.getCancelled() != null && flight.getCancelled() > 0.0;
            boolean isDiverted = flight.getDiverted() != null && flight.getDiverted() > 0.0;
            return !isCancelled && !isDiverted;
        });

        // FASE 1: Map
        JavaPairRDD<String, double[]> mappedRDD = validFlights.mapToPair(flight -> {
            String carrier = flight.getOpUniqueCarrier();
            // [0: Count, 1: Arrive_delay, 2: Carrier_delay, 3: Weather_delay, 4: NAS_delay, 5: Security_delay, 6: LateAircraft_delay]
            double[] values = new double[7];
            values[0] = 1.0; // Contatore volo valido
            values[1] = flight.getArrDelay() != null ? flight.getArrDelay() : 0.0;
            values[2] = flight.getCarrierDelay() != null ? flight.getCarrierDelay() : 0.0;
            values[3] = flight.getWeatherDelay() != null ? flight.getWeatherDelay() : 0.0;
            values[4] = flight.getNasDelay() != null ? flight.getNasDelay() : 0.0;
            values[5] = flight.getSecurityDelay() != null ? flight.getSecurityDelay() : 0.0;
            values[6] = flight.getLateAircraftDelay() != null ? flight.getLateAircraftDelay() : 0.0;

            return new Tuple2<>(carrier, values);
        });

        // FASE 2: Reduce
        JavaPairRDD<String, double[]> reducedRDD = mappedRDD.reduceByKey((a, b) -> {
            double[] res = new double[7];
            for (int i = 0; i < 7; i++) {
                res[i] = a[i] + b[i];
            }
            return res;
        });

        // FASE 3: Map
        JavaRDD<Row> processedRDD = reducedRDD
                .filter(tuple -> tuple._2[0] >= 500.0) // Filtro carriers con >= 500 voli validi
                .map(tuple -> {
                    String carrier = tuple._1;
                    double[] stats = tuple._2;

                    double count = stats[0];
                    double avgArrDelay = stats[1] / count;
                    double avgCarrier = stats[2] / count;
                    double avgWeather = stats[3] / count;
                    double avgNas = stats[4] / count;
                    double avgSecurity = stats[5] / count;
                    double avgLateAircraft = stats[6] / count;

                    return RowFactory.create(
                            carrier,
                            (long) count,
                            avgArrDelay,
                            avgCarrier,
                            avgWeather,
                            avgNas,
                            avgSecurity,
                            avgLateAircraft
                    );
                });

        // Ordinamento risultato
        JavaRDD<Row> sortedRDD = processedRDD.sortBy(row -> row.getDouble(2), false, 1);

        // Limitiamo a top 10
        List<Row> top10List = sortedRDD.take(10);

        System.out.println("=== TOP 10 AIRLINES BY AVG ARRIVAL DELAY ===");
        top10List.forEach(r -> System.out.printf("Airline: %s | Delay: %.2f | Flights: %d%n", r.getString(0), r.getDouble(2), r.getLong(1)));

        return sortedRDD;
    }

}
