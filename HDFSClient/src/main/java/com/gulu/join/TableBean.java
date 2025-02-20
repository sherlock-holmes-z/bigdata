package com.gulu.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author chocolate
 * 2025/2/20 16:02
 */
public class TableBean implements Writable {

    private String orderId;
    private String pId;
    private String pName;
    private Integer num;
    private String type;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(pId);
        dataOutput.writeUTF(pName);
        dataOutput.writeInt(num);
        dataOutput.writeUTF(type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.pId = dataInput.readUTF();
        this.pName = dataInput.readUTF();
        this.num = dataInput.readInt();
        this.type = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "TableBean{" +
                "orderId='" + orderId + '\'' +
                ", pId='" + pId + '\'' +
                ", pName='" + pName + '\'' +
                ", num=" + num +
                ", type='" + type + '\'' +
                '}';
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
