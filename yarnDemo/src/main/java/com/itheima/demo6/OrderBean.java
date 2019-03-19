package com.itheima.demo6;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean>{
    private String orderId;
    private Double price;

    public OrderBean() {
    }

    public OrderBean(String orderId, Double price) {
        this.orderId = orderId;
        this.price = price;
    }

    @Override
    public String toString() {
        return  orderId +"\t" + price ;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        if (this.orderId.compareTo(o.orderId) == 0) {
            return -this.price.compareTo(o.price);
        }
        return this.orderId.compareTo(o.orderId);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }
}
