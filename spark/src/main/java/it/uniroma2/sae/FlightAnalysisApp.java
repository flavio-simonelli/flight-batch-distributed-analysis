package it.uniroma2.sae;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.QueryType;
import it.uniroma2.sae.query.ArrivalDelayRanking;
import it.uniroma2.sae.query.BaseQuery;
import it.uniroma2.sae.query.HourlyDelayPercentiles;
import it.uniroma2.sae.query.MonthlyPerformanceAnalyzer;
import it.uniroma2.sae.query.AirlineClustering;

/**
 * The main entry point for the Flight Analysis application.
 * It reads the configuration and executes the selected query.
 */
public class FlightAnalysisApp {

    public static void main(String[] args) {
        try {
            String configFilePath = ApplicationConfig.CONFIG_FILE;

            // First pass for configuration file path
            for (int i = 0; i < args.length; i++) {
                if (("--config".equalsIgnoreCase(args[i]) || "-c".equalsIgnoreCase(args[i])) && i + 1 < args.length) {
                    configFilePath = args[++i];
                    break;
                }
            }

            // Load the configuration from the chosen YAML file
            ApplicationConfig config = ApplicationConfig.load(configFilePath);

            // Simple argument parsing: supports --query/-q and --backend/-b
            // If provided, these override the values loaded from the YAML file.
            for (int i = 0; i < args.length; i++) {
                if (("--query".equalsIgnoreCase(args[i]) || "-q".equalsIgnoreCase(args[i])) && i + 1 < args.length) {
                    config.setStringQueryToRun(args[++i]);
                } else if (("--backend".equalsIgnoreCase(args[i]) || "-b".equalsIgnoreCase(args[i])) && i + 1 < args.length) {
                    config.setStringAppBackend(args[++i]);
                }
            }

            QueryType queryToRun = config.getQueryToRun();
            if (queryToRun == null) {
                throw new IllegalArgumentException("queryToRun is not defined. " +
                        "Please provide it via command line argument (--query or -q) or in " + configFilePath);
            }

            if (config.getAppBackend() == null) {
                throw new IllegalArgumentException("appBackend is not defined. " +
                        "Please provide it via command line argument (--backend or -b) or in " + configFilePath);
            }


            BaseQuery queryJob;

            // Instantiate the correct query logic based on the configuration
            switch (queryToRun) {
                case MONTHLY_PERFORMANCE:
                    queryJob = new MonthlyPerformanceAnalyzer();
                    break;
                case ARRIVAL_DELAY_RANKING:
                    queryJob = new ArrivalDelayRanking();
                    break;
                case HOURLY_DELAY_PERCENTILES:
                    queryJob = new HourlyDelayPercentiles();
                    break;
                case AIRLINE_CLUSTERING:
                    queryJob = new AirlineClustering();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown query type: " + queryToRun);
            }

            // System.out.println("Starting execution of query: " + queryToRun + " with backend: " + config.getAppBackend());
            
            // Execute the selected query, passing the configuration object.
            queryJob.execute(config);

        } catch (Exception e) {
            System.err.println("Failed to start the application.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
