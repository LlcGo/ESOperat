package com.lc.es.bean;

public enum EsRangeType {
    op_lt("<"),
    op_lte("<="),
    op_gt(">"),
    op_gte(">="),
    op_eq("=="),
    op_neq("!="),
    ;
    private String op_value;

    public String getOp_value() {
        return op_value;
    }

    public void setOp_value(String op_value) {
        this.op_value = op_value;
    }

    private EsRangeType(String v){this.setOp_value(v);}
}
