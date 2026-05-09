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

        public Integer getYear() { return year; }
    public Integer getMonth() { return month; }
    public Integer getDayOfMonth() { return dayOfMonth; }
    public String getOpUniqueCarrier() { return opUniqueCarrier; }
    public String getOpCarrierFlNum() { return opCarrierFlNum; }
    public Long getOriginAirportId() { return originAirportId; }
    public Long getOriginCityMarketId() { return originCityMarketId; }
    public String getOriginStateAbr() { return originStateAbr; }
    public Long getDestAirportId() { return destAirportId; }
    public Long getDestCityMarketId() { return destCityMarketId; }
    public String getDestStateAbr() { return destStateAbr; }
    public Integer getCrsDepTime() { return crsDepTime; }
    public Integer getDepTime() { return depTime; }
    public Double getDepDelay() { return depDelay; }
    public Integer getCrsArrTime() { return crsArrTime; }
    public Integer getArrTime() { return arrTime; }
    public Double getArrDelay() { return arrDelay; }
    public Boolean getCancelled() { return cancelled; }
    public String getCancellationCode() { return cancellationCode; }
    public Boolean getDiverted() { return diverted; }
    public Double getActualElapsedTime() { return actualElapsedTime; }
    public Double getDistance() { return distance; }
    public Double getCarrierDelay() { return carrierDelay; }
    public Double getWeatherDelay() { return weatherDelay; }
    public Double getNasDelay() { return nasDelay; }
    public Double getSecurityDelay() { return securityDelay; }
    public Double getLateAircraftDelay() { return lateAircraftDelay; }

}
