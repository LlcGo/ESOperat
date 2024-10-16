package com.lc.es.bean;

public class Value {
    private Float boost;
    private Object value;
    private String analyzer;

    public Value(Float boost, Object value) {
        this.boost = boost;
        this.value = value;
    }

    public Value(Float boost, Object value, String analyzer) {
        this.boost = boost;
        this.value = value;
        this.analyzer = analyzer;
    }

    public Float getBoost() {
        return boost;
    }

    public void setBoost(Float boost) {
        this.boost = boost;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getAnalyzer() {
        return analyzer;
    }

    public void setAnalyzer(String analyzer) {
        this.analyzer = analyzer;
    }
}
