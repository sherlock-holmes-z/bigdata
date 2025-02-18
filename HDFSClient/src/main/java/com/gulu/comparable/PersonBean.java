package com.gulu.comparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * WritableComparable继承了Writable接口
 *
 * @author chocolate
 * 2025/2/18 15:13
 */
public class PersonBean implements WritableComparable<PersonBean> {

    private String area;

    private int age;

    private int count;


    @Override
    public int compareTo(PersonBean o) {
        if (this.area.equals(o.area)){
            if (this.age > o.age){
                return 1;
            }else if (this.age < o.getAge()){
                return -1;
            }else {
                return 0;
            }
        }else {
            return this.area.compareTo(o.area);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(area);
        dataOutput.writeInt(age);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.area = dataInput.readUTF();
        this.age = dataInput.readInt();
        this.count = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "PersonBean{" +
                "area='" + area + '\'' +
                ", age=" + age +
                ", count=" + count +
                '}';
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
