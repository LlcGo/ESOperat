package com.lc.es.bean;

import java.util.Collection;

public class Field {
    private String name;
    private String type;
    private String analyzer;
    private String store;
    private String index;
    private String format;
    private Collection<Field> properties;
    private Collection<Field> fields;

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getAnalyzer() {
        return analyzer;
    }

    public String getStore() {
        return store;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setAnalyzer(String analyzer) {
        this.analyzer = analyzer;
    }

    public void setStore(String store) {
        this.store = store;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setProperties(Collection<Field> properties) {
        this.properties = properties;
    }

    public void setFields(Collection<Field> fields) {
        this.fields = fields;
    }

    public String getIndex() {
        return index;
    }

    public String getFormat() {
        return format;
    }

    public Collection<Field> getProperties() {
        return properties;
    }

    public Collection<Field> getFields() {
        return fields;
    }
}
