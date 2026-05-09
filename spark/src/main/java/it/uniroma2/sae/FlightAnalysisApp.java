package it.uniroma2.sae;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.QueryType;
import it.uniroma2.sae.query.ArrivalDelayRanking;
import it.uniroma2.sae.query.BaseQuery;
import it.uniroma2.sae.query.HourlyDelayPercentiles;
import it.uniroma2.sae.query.MonthlyPerformanceAnalyzer;

/**
 * The main entry point for the Flight Analysis application.
 * It reads the configuration and executes the selected query.
 */
public class FlightAnalysisApp {

    public static void main(String[] args) {
        try {
            // Load the configuration
            ApplicationConfig config = ApplicationConfig.load(ApplicationConfig.CONFIG_FILE);
            QueryType queryToRun = config.getQueryToRun();

            if (queryToRun == null) {
                throw new IllegalArgumentException("queryToRun is not defined in config.yml");
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
                default:
                    throw new IllegalArgumentException("Unknown query type: " + queryToRun);
            }

            System.out.println("Starting execution of query: " + queryToRun);
            
            // Execute the selected query, passing the configuration object.
            queryJob.execute(config);

        } catch (Exception e) {
            System.err.println("Failed to start the application.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
