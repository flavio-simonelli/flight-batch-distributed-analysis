package it.uniroma2.sae;

import it.uniroma2.sae.config.AppConfig;
import it.uniroma2.sae.factory.FlightRepositoryFactory;
import it.uniroma2.sae.model.RawFlight;
import it.uniroma2.sae.repository.FlightRepository;
import it.uniroma2.sae.service.FlightService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;

public class HelloSpark {

    public static void main(String[] args) {
        try {

            AppConfig config = AppConfig.load(AppConfig.CONFIG_FILE);
            SparkSession spark = SparkSession.builder()
                    .appName(config.getAppName())
                    .master(config.getSparkCluster().getMasterUri())
                    .getOrCreate();

            spark.sparkContext().setLogLevel("WARN");

            FlightRepository repository = FlightRepositoryFactory.createRepository(config);
            Dataset<RawFlight> flights = repository.getFlights("202501_T_ONTIME_REPORTING.parquet");

            FlightService service = new FlightService();

            System.out.println("=== Dataset caricato ===");
            flights.show(5);

            System.out.println("=== Voli in ritardo (delay > 0) ===");
            service.getDelayedFlights(flights)
                   .show();

            System.out.println("=== Ritardo medio per origine ===");
            service.getAverageDelayByOrigin(flights)
                   .show();

            spark.stop();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
