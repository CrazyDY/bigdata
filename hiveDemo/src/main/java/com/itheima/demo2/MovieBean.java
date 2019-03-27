package com.itheima.demo2;

public class MovieBean {
    private String movie;
    private String rate;
    private String timestamp;
    private String uid;

    @Override
    public String toString() {
        return
                 movie + '\t' + rate + '\t' + timestamp + '\t' +uid;
    }

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }
}
