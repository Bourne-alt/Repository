package renaissance.bean;

public class MemUsedWithColorBean {
    private int id;
    private String hostname;
    private String alertLev;
    private String threhUpdateTime;
    private String threhCreateTime;
    private int amberThreshold;
    private String value;
    private int redThreshold;
    private String metricName;


    @Override
    public String toString() {
        return "MemUsedWithColorBean{" +
                "id=" + id +
                ", hostname='" + hostname + '\'' +
                ", alertLev='" + alertLev + '\'' +
                ", threhUpdateTime='" + threhUpdateTime + '\'' +
                ", threhCreateTime='" + threhCreateTime + '\'' +
                ", amberThreshold=" + amberThreshold +
                ", value='" + value + '\'' +
                ", redThreshold=" + redThreshold +
                ", metricName='" + metricName + '\'' +
                '}';
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

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

}
