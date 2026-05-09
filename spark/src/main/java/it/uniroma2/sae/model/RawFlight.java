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
    private Double cancelled;
    private String cancellationCode;
    private Double diverted;
    private Double actualElapsedTime;
    private Double distance;
    private Double carrierDelay;
    private Double weatherDelay;
    private Double nasDelay;
    private Double securityDelay;
    private Double lateAircraftDelay;

    public Integer getYear() { return year; }
    public void setYear(Integer year) { this.year = year; }
    public Integer getMonth() { return month; }
    public void setMonth(Integer month) { this.month = month; }
    public Integer getDayOfMonth() { return dayOfMonth; }
    public void setDayOfMonth(Integer dayOfMonth) { this.dayOfMonth = dayOfMonth; }
    public String getOpUniqueCarrier() { return opUniqueCarrier; }
    public void setOpUniqueCarrier(String opUniqueCarrier) { this.opUniqueCarrier = opUniqueCarrier; }
    public String getOpCarrierFlNum() { return opCarrierFlNum; }
    public void setOpCarrierFlNum(String opCarrierFlNum) { this.opCarrierFlNum = opCarrierFlNum; }
    public Long getOriginAirportId() { return originAirportId; }
    public void setOriginAirportId(Long originAirportId) { this.originAirportId = originAirportId; }
    public Long getOriginCityMarketId() { return originCityMarketId; }
    public void setOriginCityMarketId(Long originCityMarketId) { this.originCityMarketId = originCityMarketId; }
    public String getOriginStateAbr() { return originStateAbr; }
    public void setOriginStateAbr(String originStateAbr) { this.originStateAbr = originStateAbr; }
    public Long getDestAirportId() { return destAirportId; }
    public void setDestAirportId(Long destAirportId) { this.destAirportId = destAirportId; }
    public Long getDestCityMarketId() { return destCityMarketId; }
    public void setDestCityMarketId(Long destCityMarketId) { this.destCityMarketId = destCityMarketId; }
    public String getDestStateAbr() { return destStateAbr; }
    public void setDestStateAbr(String destStateAbr) { this.destStateAbr = destStateAbr; }
    public Integer getCrsDepTime() { return crsDepTime; }
    public void setCrsDepTime(Integer crsDepTime) { this.crsDepTime = crsDepTime; }
    public Integer getDepTime() { return depTime; }
    public void setDepTime(Integer depTime) { this.depTime = depTime; }
    public Double getDepDelay() { return depDelay; }
    public void setDepDelay(Double depDelay) { this.depDelay = depDelay; }
    public Integer getCrsArrTime() { return crsArrTime; }
    public void setCrsArrTime(Integer crsArrTime) { this.crsArrTime = crsArrTime; }
    public Integer getArrTime() { return arrTime; }
    public void setArrTime(Integer arrTime) { this.arrTime = arrTime; }
    public Double getArrDelay() { return arrDelay; }
    public void setArrDelay(Double arrDelay) { this.arrDelay = arrDelay; }
    public Double getCancelled() { return cancelled; }
    public void setCancelled(Double cancelled) { this.cancelled = cancelled; }
    public String getCancellationCode() { return cancellationCode; }
    public void setCancellationCode(String cancellationCode) { this.cancellationCode = cancellationCode; }
    public Double getDiverted() { return diverted; }
    public void setDiverted(Double diverted) { this.diverted = diverted; }
    public Double getActualElapsedTime() { return actualElapsedTime; }
    public void setActualElapsedTime(Double actualElapsedTime) { this.actualElapsedTime = actualElapsedTime; }
    public Double getDistance() { return distance; }
    public void setDistance(Double distance) { this.distance = distance; }
    public Double getCarrierDelay() { return carrierDelay; }
    public void setCarrierDelay(Double carrierDelay) { this.carrierDelay = carrierDelay; }
    public Double getWeatherDelay() { return weatherDelay; }
    public void setWeatherDelay(Double weatherDelay) { this.weatherDelay = weatherDelay; }
    public Double getNasDelay() { return nasDelay; }
    public void setNasDelay(Double nasDelay) { this.nasDelay = nasDelay; }
    public Double getSecurityDelay() { return securityDelay; }
    public void setSecurityDelay(Double securityDelay) { this.securityDelay = securityDelay; }
    public Double getLateAircraftDelay() { return lateAircraftDelay; }
    public void setLateAircraftDelay(Double lateAircraftDelay) { this.lateAircraftDelay = lateAircraftDelay; }

    @Override
    public String toString() {
        return "RawFlight{" +
                "year=" + year +
                ", month=" + month +
                ", dayOfMonth=" + dayOfMonth +
                ", opUniqueCarrier='" + opUniqueCarrier + '\'' +
                ", opCarrierFlNum='" + opCarrierFlNum + '\'' +
                ", originAirportId=" + originAirportId +
                ", originCityMarketId=" + originCityMarketId +
                ", originStateAbr='" + originStateAbr + '\'' +
                ", destAirportId=" + destAirportId +
                ", destCityMarketId=" + destCityMarketId +
                ", destStateAbr='" + destStateAbr + '\'' +
                ", crsDepTime=" + crsDepTime +
                ", depTime=" + depTime +
                ", depDelay=" + depDelay +
                ", crsArrTime=" + crsArrTime +
                ", arrTime=" + arrTime +
                ", arrDelay=" + arrDelay +
                ", cancelled=" + cancelled +
                ", cancellationCode='" + cancellationCode + '\'' +
                ", diverted=" + diverted +
                ", actualElapsedTime=" + actualElapsedTime +
                ", distance=" + distance +
                ", carrierDelay=" + carrierDelay +
                ", weatherDelay=" + weatherDelay +
                ", nasDelay=" + nasDelay +
                ", securityDelay=" + securityDelay +
                ", lateAircraftDelay=" + lateAircraftDelay +
                '}';
    }
}
