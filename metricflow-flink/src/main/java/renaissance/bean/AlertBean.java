package renaissance.bean;

public class AlertBean {
    private String id;
    private String hostname;
    private String timestamp;
    private String value;
    private String lastvalue;
    private String diffValue;
    private String color;

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getLastvalue() {
        return lastvalue;
    }

    public void setLastvalue(String lastvalue) {
        this.lastvalue = lastvalue;
    }

    public String getDiffValue() {
        return diffValue;
    }

    public void setDiffValue(String diffValue) {
        this.diffValue = diffValue;
    }

    @Override
    public String toString() {
        return "AlertBean{" +
                "id='" + id + '\'' +
                ", hostname='" + hostname + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", value='" + value + '\'' +
                ", lastvalue='" + lastvalue + '\'' +
                ", diffValue='" + diffValue + '\'' +
                ", color='" + color + '\'' +
                '}';
    }
}
