package it.uniroma2.sae.repository;

import it.uniroma2.sae.model.RawFlight;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * Base abstract class for flight repositories.
 * Centralizes the logic for retrieving and converting data into a strongly typed Dataset.
 */
public abstract class FlightRepository {

    protected final SparkSession spark;

    /**
     * Explicit schema for flight data to avoid Spark's schema inference overhead.
     * Maps to the column names found in the Parquet files.
     */
    public static final StructType FLIGHT_SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("YEAR", DataTypes.IntegerType, true),
            DataTypes.createStructField("MONTH", DataTypes.IntegerType, true),
            DataTypes.createStructField("DAY_OF_MONTH", DataTypes.IntegerType, true),
            DataTypes.createStructField("OP_UNIQUE_CARRIER", DataTypes.StringType, true),
            DataTypes.createStructField("OP_CARRIER_FL_NUM", DataTypes.IntegerType, true),
            DataTypes.createStructField("ORIGIN_AIRPORT_ID", DataTypes.IntegerType, true),
            DataTypes.createStructField("ORIGIN_CITY_MARKET_ID", DataTypes.IntegerType, true),
            DataTypes.createStructField("ORIGIN_STATE_ABR", DataTypes.StringType, true),
            DataTypes.createStructField("DEST_AIRPORT_ID", DataTypes.IntegerType, true),
            DataTypes.createStructField("DEST_CITY_MARKET_ID", DataTypes.IntegerType, true),
            DataTypes.createStructField("DEST_STATE_ABR", DataTypes.StringType, true),
            DataTypes.createStructField("CRS_DEP_TIME", DataTypes.IntegerType, true),
            DataTypes.createStructField("DEP_TIME", DataTypes.IntegerType, true),
            DataTypes.createStructField("DEP_DELAY", DataTypes.DoubleType, true),
            DataTypes.createStructField("CRS_ARR_TIME", DataTypes.IntegerType, true),
            DataTypes.createStructField("ARR_TIME", DataTypes.IntegerType, true),
            DataTypes.createStructField("ARR_DELAY", DataTypes.DoubleType, true),
            DataTypes.createStructField("CANCELLED", DataTypes.DoubleType, true),
            DataTypes.createStructField("CANCELLATION_CODE", DataTypes.StringType, true),
            DataTypes.createStructField("DIVERTED", DataTypes.DoubleType, true),
            DataTypes.createStructField("ACTUAL_ELAPSED_TIME", DataTypes.FloatType, true),
            DataTypes.createStructField("DISTANCE", DataTypes.FloatType, true),
            DataTypes.createStructField("CARRIER_DELAY", DataTypes.DoubleType, true),
            DataTypes.createStructField("WEATHER_DELAY", DataTypes.DoubleType, true),
            DataTypes.createStructField("NAS_DELAY", DataTypes.DoubleType, true),
            DataTypes.createStructField("SECURITY_DELAY", DataTypes.DoubleType, true),
            DataTypes.createStructField("LATE_AIRCRAFT_DELAY", DataTypes.DoubleType, true)
    });

    /**
     * Constructs a new FlightRepository.
     *
     * @param spark the SparkSession to be used for data operations
     */
    public FlightRepository(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * Retrieves the flights from the specified file or directory and converts them into a Dataset<Row>.
     * If the filename is not provided (null or empty), it reads all parquet files in the base path.
     *
     * @param datasetFilename the name of the Parquet file to read, or null/empty to read all files in the directory
     * @return a Dataset of Row objects
     * @throws IllegalArgumentException if the filename is invalid (when provided)
     */
    public Dataset<Row> getFlights(String datasetFilename) {
        return getFlights(datasetFilename, null);
    }

    /**
     * Retrieves the flights from the specified file or directory for the given airlines and converts them into a Dataset<Row>.
     *
     * @param datasetFilename the name of the Parquet file to read, or null/empty to read all files in the directory
     * @param airlines an array of airline codes to filter the dataset
     * @return a Dataset of Row objects
     * @throws IllegalArgumentException if the filename is invalid (when provided)
     */
    public final Dataset<Row> getFlightsOfAirlines(String datasetFilename, String... airlines) {
        if(airlines == null || airlines.length == 0) throw new IllegalArgumentException("Airlines array cannot be null or empty");
        Map<String, Object[]> filters = Map.of("OP_UNIQUE_CARRIER", airlines);
        return getFlights(datasetFilename, filters);
    }

    /**
     * Retrieves the flights from the specified file or directory and converts them into a Dataset<Row>.
     *
     * @param datasetFilename the name of the Parquet file to read, or null/empty to read all files in the directory
     * @param filters a map of column names to arrays of values to filter the dataset (e.g., {"OP_UNIQUE_CARRIER": ["AA", "DL"]})
     * @return a Dataset of Row objects
     * @throws IllegalArgumentException if the filename is invalid (when provided)
     */
    protected Dataset<Row> getFlights(String datasetFilename, Map<String, Object[]> filters) {
        String fullPath;
        if (datasetFilename == null || datasetFilename.trim().isEmpty()) {
            // If no filename is provided, read all Parquet files in the base path
            fullPath = getFullPath("");
        } else {
            // Otherwise, check and normalize the filename
            datasetFilename = checkInputFilename(datasetFilename);
            fullPath = getFullPath(datasetFilename);
        }
        
        // Use explicit schema to avoid inference job
        Dataset<Row> rawRows = this.spark.read().schema(FLIGHT_SCHEMA).parquet(fullPath);

        if (filters != null) {
            for (Map.Entry<String, Object[]> filter : filters.entrySet()) {
                if (filter.getValue() != null && filter.getValue().length > 0) {
                    rawRows = rawRows.filter(col(filter.getKey()).isin((Object[]) filter.getValue()));
                }
            }
        }

        return rawRows;
    }

    /**
     * Saves the given Dataset as a CSV file to the specified output path.
     *
     * @param results the Dataset to save
     * @param resultDirectory the name of the output directory
     */
    public void saveResults(Dataset<Row> results, String resultDirectory) {
        if (results == null) throw new IllegalArgumentException("Results dataset cannot be null.");

        resultDirectory = checkOutputDirectory(resultDirectory);
        String fullPath = getFullPath(resultDirectory);

        results.coalesce(1) // Consolidate into a single partition
                .write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(fullPath);
    }

    /**
     * Saves the given JavaRDD as a CSV file to the specified output path.
     *
     * @param results the JavaRDD to save
     * @param schema the schema of the RDD
     * @param resultDirectory the name of the output directory
     */
    public void saveResults(JavaRDD<Row> results, StructType schema, String resultDirectory) {
        if (results == null) throw new IllegalArgumentException("Results RDD cannot be null.");
        if (schema == null) throw new IllegalArgumentException("Schema cannot be null when saving JavaRDD<Row>.");
        // Avoid to use .empty() for performance reasons
        // if (results.isEmpty()) return;

        Dataset<Row> convertedResults = spark.createDataFrame(results, schema);
        saveResults(convertedResults, resultDirectory);
    }

    /**
     * @deprecated This method is deprecated in favor of the version that accepts a Dataset<Row> for better performance and simplicity.
     * Saves the given JavaRDD as a CSV file to the specified output path.
     *
     * @param jsc the SparkContext to use
     * @param results the JavaRDD to save
     * @param schema the schema of the RDD
     * @param resultDirectory the name of the output directory
     */
    @Deprecated
    public void saveResults(JavaSparkContext jsc, JavaRDD<Row> results, StructType schema, String resultDirectory) {
        if (results == null) throw new IllegalArgumentException("Results RDD cannot be null.");
        if (schema == null) throw new IllegalArgumentException("Schema cannot be null when saving JavaRDD<Row>.");
        // Avoid to use .empty() for performance reasons
        // if (results.isEmpty()) return;

        resultDirectory = checkOutputDirectory(resultDirectory);
        String fullPath = getFullPath(resultDirectory);

        String header = String.join(",", schema.fieldNames());

        // Convert each row to a comma-separated string
        JavaRDD<String> dataLines = results.map(row -> {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.length(); i++) {
                sb.append(row.get(i) == null ? "" : row.get(i).toString());
                if (i < row.length() - 1) sb.append(",");
            }
            return sb.toString();
        });

        JavaRDD<String> headerRDD = jsc.parallelize(Collections.singletonList(header));

        headerRDD.union(dataLines)
                .coalesce(1)
                .saveAsTextFile(getFullPath(resultDirectory));
    }

    /**
     * Constructs the full path to the input file.
     * Must be implemented by subclasses to provide the specific URI scheme (e.g., file://, hdfs://, s3a://).
     *
     * @param filename the name of the file (can be empty to point to the base directory)
     * @return the full path string
     */
    protected abstract String getFullPath(String filename);

    /**
     * Validates and normalizes the given input filename.
     * Ensures the filename ends with '.parquet' and starts with a slash '/'.
     *
     * @param filename the raw filename string
     * @return the normalized filename string
     * @throws IllegalArgumentException if the filename is null or doesn't have the correct extension
     */
    protected String checkInputFilename(String filename) {
        return checkFilename(filename, ".parquet");
    }

    /**
     * Validates and normalizes the given output directory.
     * Ensures the directory starts with a slash '/'.
     *
     * @param directory the raw directory string
     * @return the normalized directory string
     * @throws IllegalArgumentException if the directory is null or doesn't have the correct extension
     */
    protected String checkOutputDirectory(String directory) {
        return checkFilename(directory, null);
    }

    /**
     * Validates and normalizes the given filename.
     * Ensures the filename ends with extension and starts with a slash '/'.
     *
     * @param filename the raw filename string
     * @param extension the expected file extension (e.g., ".csv", ".parquet")
     * @return the normalized filename string
     * @throws IllegalArgumentException if the filename is null or doesn't have the correct extension
     */
    private String checkFilename(String filename, String extension) {
        if(filename == null) throw new IllegalArgumentException("Filename cannot be null");
        if(extension != null && !filename.endsWith(extension)) throw new IllegalArgumentException("Filename must end with " + extension + ". Provided: " + filename);
        if(!filename.startsWith("/")) filename = "/" + filename;
        return filename;
    }

    /**
     * Converts a generic Dataset<Row> into a strongly typed Dataset<RawFlight>.
     * It maps the snake_case column names from the Parquet file to the camelCase fields of the RawFlight class.
     *
     * @param rawRows the input Dataset of Row objects
     * @return a strongly typed Dataset of RawFlight objects
     */
    public Dataset<RawFlight> convertToRawFlight(Dataset<Row> rawRows) {
        // Here we explicitly cast CANCELLED and DIVERTED to Double because in the model RawFlight 
        // they are Double (as read from Parquet), NOT Boolean.
        return rawRows.select(
                col("YEAR").as("year"),
                col("MONTH").as("month"),
                col("DAY_OF_MONTH").as("dayOfMonth"),
                col("OP_UNIQUE_CARRIER").as("opUniqueCarrier"),
                col("OP_CARRIER_FL_NUM").as("opCarrierFlNum"),
                col("ORIGIN_AIRPORT_ID").as("originAirportId"),
                col("ORIGIN_CITY_MARKET_ID").as("originCityMarketId"),
                col("ORIGIN_STATE_ABR").as("originStateAbr"),
                col("DEST_AIRPORT_ID").as("destAirportId"),
                col("DEST_CITY_MARKET_ID").as("destCityMarketId"),
                col("DEST_STATE_ABR").as("destStateAbr"),
                col("CRS_DEP_TIME").as("crsDepTime"),
                col("DEP_TIME").as("depTime"),
                col("DEP_DELAY").as("depDelay"),
                col("CRS_ARR_TIME").as("crsArrTime"),
                col("ARR_TIME").as("arrTime"),
                col("ARR_DELAY").as("arrDelay"),
                col("CANCELLED").cast(DataTypes.DoubleType).as("cancelled"),
                col("CANCELLATION_CODE").as("cancellationCode"),
                col("DIVERTED").cast(DataTypes.DoubleType).as("diverted"),
                col("ACTUAL_ELAPSED_TIME").as("actualElapsedTime"),
                col("DISTANCE").as("distance"),
                col("CARRIER_DELAY").as("carrierDelay"),
                col("WEATHER_DELAY").as("weatherDelay"),
                col("NAS_DELAY").as("nasDelay"),
                col("SECURITY_DELAY").as("securityDelay"),
                col("LATE_AIRCRAFT_DELAY").as("lateAircraftDelay")
        ).as(Encoders.bean(RawFlight.class));
    }
}
