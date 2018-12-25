package ru.mephi.bsbda;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;



//class for own type
public class LogInfo implements WritableComparable<LogInfo> {

    private Text ipId;
    private Text ip;
    private String browser;
    private Integer totalByte;


    public LogInfo() {
        this.ipId = new Text();
        this.ip = new Text();
        this.browser = "";
        this.totalByte = 0;
    }

    public LogInfo(Text ipId, Text ip, String browser, Integer totalByte) {
        this.ipId = ipId;
        this.ip = ip;
        this.browser = browser;
        this.totalByte = totalByte;
    }

    public Text getIpId() {
        return this.ipId;
    }

    public Text getIp() {
        return this.ip;
    }

    public String getBrowser() {
        return this.browser;
    }

    public Integer getTotalByte() {
        return this.totalByte;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogInfo logInfo = (LogInfo) o;
        return Objects.equals(ipId, logInfo.ipId) &&
                Objects.equals(ip, logInfo.ip) &&
                Objects.equals(browser, logInfo.browser) &&
                Objects.equals(totalByte, logInfo.totalByte);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ipId, ip, browser, totalByte);
    }

    @Override
    public String toString() {
        return "LogString: " + ipId + "; " + ip + "; " + browser + "; " + totalByte + ";";
    }

    public int compareTo(LogInfo o) {
        return this.ipId.compareTo(o.ipId);
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.ipId.write(dataOutput);
        this.ip.write(dataOutput);
        dataOutput.writeUTF(browser);
        dataOutput.writeInt(totalByte);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.ipId.readFields(dataInput);
        this.ip.readFields(dataInput);
        this.browser   = dataInput.readUTF();
        this.totalByte = dataInput.readInt();
    }
}
