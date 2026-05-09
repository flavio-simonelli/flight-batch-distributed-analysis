package it.uniroma2.sae.repository;

import it.uniroma2.sae.model.RawFlight;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;

import static org.apache.spark.sql.functions.col;

/**
 * Base abstract class for flight repositories.
 * Centralizes the logic for retrieving and converting data into a strongly typed Dataset.
 */
public abstract class FlightRepository {

    protected final SparkSession spark;

    /**
     * Constructs a new FlightRepository.
     *
     * @param spark the SparkSession to be used for data operations
     */
    public FlightRepository(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * Retrieves the flights from the specified file and converts them into a strongly typed Dataset.
     * This method orchestrates the reading process by delegating the path construction to subclasses.
     *
     * @param datasetFilename the name of the Parquet file to read
     * @return a Dataset of RawFlight objects
     * @throws IllegalArgumentException if the filename is null or invalid
     */
    public final Dataset<RawFlight> getFlights(String datasetFilename) {
        datasetFilename = checkInputFilename(datasetFilename);
        String fullPath = getFullPath(datasetFilename);

        Dataset<Row> rawRows = this.spark.read().parquet(fullPath);

        return convertToRawFlight(rawRows);
    }

    /**
     * Retrieves the flights from the specified file for the given airlines and converts them into a strongly typed Dataset.
     * This method orchestrates the reading process by delegating the path construction to subclasses.
     *
     * @param datasetFilename the name of the Parquet file to read
     * @return a Dataset of RawFlight objects
     * @throws IllegalArgumentException if the filename is null or invalid
     */
    public final Dataset<RawFlight> getFlightsOfAirlines(String datasetFilename, String... airlines) {
        datasetFilename = checkInputFilename(datasetFilename);
        String fullPath = getFullPath(datasetFilename);

        Dataset<Row> filteredRows = this.spark.read().parquet(fullPath).filter(col("OP_UNIQUE_CARRIER").isin((Object[]) airlines));
        return convertToRawFlight(filteredRows);
    }

    /**
     * Saves the given Dataset as a CSV file to the specified output path.
     *
     * @param results the Dataset to save
     * @param resultDirectory the name of the output directory
     */
    public final void saveResults(Dataset<Row> results, String resultDirectory) {
        if (results == null) throw new IllegalArgumentException("Results dataset cannot be null.");

        resultDirectory = checkOutputDirectory(resultDirectory);
        String fullPath = getFullPath(resultDirectory);

        results.write()
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .csv(fullPath);
    }

    /**
     * Constructs the full path to the input file.
     * Must be implemented by subclasses to provide the specific URI scheme (e.g., file://, hdfs://, s3a://).
     *
     * @param filename the name of the file
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
    protected Dataset<RawFlight> convertToRawFlight(Dataset<Row> rawRows) {
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
                col("CANCELLED").as("cancelled"),
                col("CANCELLATION_CODE").as("cancellationCode"),
                col("DIVERTED").as("diverted"),
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
