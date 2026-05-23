package it.uniroma2.sae;

import it.uniroma2.sae.config.ApplicationConfig;
import it.uniroma2.sae.config.QueryType;
import it.uniroma2.sae.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * The main entry point for the Flight Analysis application.
 * It reads the configuration and executes the selected query.
 */
public class FlightAnalysisApp {
    private static final Logger logger = LoggerFactory.getLogger(FlightAnalysisApp.class);

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

            // Simple argument parsing: supports --query/-q, --backend/-b, --input-type/-i, --output-type/-o, --metrics-type/-m, --partitions/-p
            // If provided, these override the values loaded from the YAML file.
            for (int i = 0; i < args.length; i++) {
                if (("--query".equalsIgnoreCase(args[i]) || "-q".equalsIgnoreCase(args[i])) && i + 1 < args.length) {
                    config.setStringQueryToRun(args[++i]);
                } else if (("--backend".equalsIgnoreCase(args[i]) || "-b".equalsIgnoreCase(args[i])) && i + 1 < args.length) {
                    config.setStringAppBackend(args[++i]);
                } else if (("--input-type".equalsIgnoreCase(args[i]) || "-i".equalsIgnoreCase(args[i])) && i + 1 < args.length) {
                    config.setSelectedInput(args[++i]);
                } else if (("--output-type".equalsIgnoreCase(args[i]) || "-o".equalsIgnoreCase(args[i])) && i + 1 < args.length) {
                    config.setSelectedOutput(args[++i]);
                } else if (("--metrics-type".equalsIgnoreCase(args[i]) || "-m".equalsIgnoreCase(args[i])) && i + 1 < args.length) {
                    config.setSelectedMetrics(args[++i]);
                } else if (("--partitions".equalsIgnoreCase(args[i]) || "-p".equalsIgnoreCase(args[i])) && i + 1 < args.length) {
                    config.setStringOutputPartitions(args[++i]);
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

            // Structured logging context
            MDC.put("query", queryToRun.toString());
            MDC.put("backend", config.getAppBackend().toString());

            logger.info("Initializing flight analysis job | query={} | backend={}", queryToRun, config.getAppBackend());

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

            // Execute the selected query, passing the configuration object.
            queryJob.execute(config);

            logger.info("Flight analysis job completed successfully.");

        } catch (Exception e) {
            logger.error("Fatal error during application execution: {}", e.getMessage(), e);
            System.exit(1);
        } finally {
            MDC.clear();
        }
    }
}
