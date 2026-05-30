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

            // Simple argument parsing, supports:
            // --query/-q
            // --backend/-b
            // --input-type/-i
            // --output-type/-o
            // --metrics-type/-m
            // --partitions/-p
            //
            // If provided, these override the values loaded from the YAML file.
            for (int i = 0; i < args.length; i++) {
                CliArgs argType = CliArgs.fromString(args[i]);

                if (argType != null && i + 1 < args.length) {
                    String value = args[++i]; // Take the next argument as the value for this option

                    switch (argType) {
                        case CONFIG: break;
                        case QUERY: config.setStringQueryToRun(value); break;
                        case BACKEND: config.setStringAppBackend(value); break;
                        case INPUT_TYPE: config.setSelectedInput(value); break;
                        case OUTPUT_TYPE: config.setSelectedOutput(value); break;
                        case METRICS_TYPE: config.setSelectedMetrics(value); break;
                        case PARTITIONS: config.setStringOutputPartitions(value); break;
                    }
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

            logger.info("Initializing flight analysis job | query={} | backend={} | input={} | output={} | metrics={} | partitions={}", queryToRun, config.getAppBackend(), config.getInput().getStringType(), config.getOutput().getStringType(), config.getMetrics().getStringType(), config.getOutputPartitions());

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

    /**
     * Enum for command-line arguments, supporting both long and short forms.
     * Includes a factory method to parse arguments from strings.
     */
    private enum CliArgs {
        CONFIG("--config", "-c"),
        QUERY("--query", "-q"),
        BACKEND("--backend", "-b"),
        INPUT_TYPE("--input-type", "-i"),
        OUTPUT_TYPE("--output-type", "-o"),
        METRICS_TYPE("--metrics-type", "-m"),
        PARTITIONS("--partitions", "-p");

        private final String longName;
        private final String shortName;

        /**
         * Factory method to parse a CLI argument from a string.
         * 
         * @param arg the argument string to parse
         * @return the corresponding CliArgs enum value, or null if no match is found
         */
        public static CliArgs fromString(String arg) {
            for (CliArgs c : values()) {
                if (c.matches(arg)) {
                    return c;
                }
            }
            return null;
        }

        /**
         * Constructor for CliArgs enum.
         * 
         * @param longName the long form of the argument (e.g., "--query")
         * @param shortName the short form of the argument (e.g., "-q")
         */
        CliArgs(String longName, String shortName) {
            this.longName = longName;
            this.shortName = shortName;
        }

        /**
         * Checks if the given argument matches this CLI argument.
         *
         * @param arg the argument to check
         * @return true if it matches, false otherwise
         */
        public boolean matches(String arg) {
            return this.longName.equalsIgnoreCase(arg) || this.shortName.equalsIgnoreCase(arg);
        }
    }
}
