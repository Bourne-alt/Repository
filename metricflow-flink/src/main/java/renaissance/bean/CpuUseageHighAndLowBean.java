package renaissance.bean;

public class CpuUseageHighAndLowBean {
    private int id;
    private String hostname;
    private int highUse;
    private int lowUse;
    private String startTime;
    private String endTime;


    @Override
    public String toString() {
        return "CpuUseageHighAndLowBean{" +
                "id='" + id + '\'' +
                ", hostname='" + hostname + '\'' +
                ", highUse=" + highUse +
                ", lowUse=" + lowUse +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                '}';
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
}
