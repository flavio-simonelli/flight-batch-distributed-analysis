package it.uniroma2.sae.model;

import java.io.Serializable;

public class RawFlight implements Serializable {

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
    private String crsDepTime;
    private String depTime;
    private Double depDelay;
    private String crsArrTime;
    private String arrTime;
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
