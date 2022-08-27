package renaissance.bean;

public class MemUsedWithColorBean {
    private int id;
    private String hostname;
    private int highUse;
    private int lowUse;
    private String startTime;
    private String endTime;
    private String alertLev;
    private String threhUpdateTime;
    private String threhCreateTime;
    private int amberThreshold;
    private int redThreshold;
    private String metricName;


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getHighUse() {
        return highUse;
    }

    public void setHighUse(int highUse) {
        this.highUse = highUse;
    }

    public int getLowUse() {
        return lowUse;
    }

    public void setLowUse(int lowUse) {
        this.lowUse = lowUse;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getAlertLev() {
        return alertLev;
    }

    public void setAlertLev(String alertLev) {
        this.alertLev = alertLev;
    }

    public String getThrehUpdateTime() {
        return threhUpdateTime;
    }

    public void setThrehUpdateTime(String threhUpdateTime) {
        this.threhUpdateTime = threhUpdateTime;
    }

    public String getThrehCreateTime() {
        return threhCreateTime;
    }

    public void setThrehCreateTime(String threhCreateTime) {
        this.threhCreateTime = threhCreateTime;
    }

    public int getAmberThreshold() {
        return amberThreshold;
    }

    public void setAmberThreshold(int amberThreshold) {
        this.amberThreshold = amberThreshold;
    }

    public int getRedThreshold() {
        return redThreshold;
    }

    public void setRedThreshold(int redThreshold) {
        this.redThreshold = redThreshold;
    }

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    @Override
    public String toString() {
        return "MemUsedWithColorBean{" +
                "id=" + id +
                ", hostname='" + hostname + '\'' +
                ", highUse=" + highUse +
                ", lowUse=" + lowUse +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                ", alertLev='" + alertLev + '\'' +
                ", threhUpdateTime='" + threhUpdateTime + '\'' +
                ", threhCreateTime='" + threhCreateTime + '\'' +
                ", amberThreshold=" + amberThreshold +
                ", redThreshold=" + redThreshold +
                ", metricName='" + metricName + '\'' +
                '}';
    }
}
