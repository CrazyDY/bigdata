package com.itheima.demo2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairSortBean implements WritableComparable<PairSortBean> {
    private String first;
    private Integer second;

    public PairSortBean() {
    }

    public PairSortBean(String first, Integer second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return first+"\t"+second;
    }

    /**
     * 还有两个数值的对象比较大小的规则，以第一个为主，先比较第一个，如果第一个相等，则比较第二个数
     * @param o
     * @return
     */
    @Override
    public int compareTo(PairSortBean o) {
        int compare = this.getFirst().compareTo(o.getFirst());
        if (compare != 0) {//第一个不相等
            return compare;
        } else {
            return this.getSecond().compareTo(o.getSecond());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readUTF();
        this.second = in.readInt();
    }
}
