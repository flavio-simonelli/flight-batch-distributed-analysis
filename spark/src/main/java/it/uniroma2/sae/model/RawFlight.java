package it.uniroma2.sae.model;

import java.io.Serializable;

public class RawFlight implements Serializable {

    // Time is in format HHmm, e.g., 2359 for 11:59 PM

    private Integer year;
    private Integer month;
    private Integer dayOfMonth;
    private String opUniqueCarrier;
    private String opCarrierFlNum;
    private Long originAirportId;
    private Long originCityMarketId;
    private String originStateAbr;
    private Long destAirportId;
    private Long destCityMarketId;
    private String destStateAbr;
    private Integer crsDepTime;          // Expected departure time
    private Integer depTime;             // Actual departure time
    private Double depDelay;
    private Integer crsArrTime;          // Expected arrival time
    private Integer arrTime;             // Actual arrival time
    private Double arrDelay;
    private Boolean cancelled;
    private String cancellationCode;
    private Boolean diverted;
    private Double actualElapsedTime;
    private Double distance;
    private Double carrierDelay;
    private Double weatherDelay;
    private Double nasDelay;
    private Double securityDelay;
    private Double lateAircraftDelay;

}
