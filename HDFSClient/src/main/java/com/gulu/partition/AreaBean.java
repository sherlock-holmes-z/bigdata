package com.gulu.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Gollum
 * @date 2025-02-17 21:33
 */
public class AreaBean implements Writable {

    private String area;

    private Integer num;

    public AreaBean(String area, Integer num) {
        this.area = area;
        this.num = num;
    }

    public AreaBean() {
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(area);
        dataOutput.writeInt(num);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.area = dataInput.readUTF();
        this.num = dataInput.readInt();
    }

    @Override
    public String toString() {
        return area + "\t" + num;
    }
}
