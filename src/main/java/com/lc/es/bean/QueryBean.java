package com.lc.es.bean;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.metrics.TopHits;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class QueryBean implements Serializable {
    private static final long serialVersionUID = -5780017052087907419L;

    private Integer pageIndex = 1;
    private Integer pageSize = 10;
    private String orderBy;
    private String format;
    private String groupField;
    private Boolean groupAsc;
    private Boolean keySort;
    private String kName;
    private String vName;
    private String includes;
    private String excludes;
    private JSONObject termJson;
    private JSONObject wildcardJson;
    private JSONObject multiMatchJson;
    private List<Object[]> rangeFields;
    private List<String[]> compareFields;
    private List<String> scripts;
    private QueryBean notQueryBean;
    private List<QueryBean> orQueryBeans;
    private List<QueryBean> andQueryBeans;
    private String existsFields;
    private Integer minDocCount;
    private QueryBuilder queryBuilder;

    public Integer getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(Integer pageIndex) {
        this.pageIndex = pageIndex;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getGroupField() {
        return groupField;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }

    public Boolean getGroupAsc() {
        return groupAsc;
    }

    public void setGroupAsc(Boolean groupAsc) {
        this.groupAsc = groupAsc;
    }

    public Boolean getKeySort() {
        return keySort;
    }

    public void setKeySort(Boolean keySort) {
        this.keySort = keySort;
    }

    public String getkName() {
        return kName;
    }

    public void setkName(String kName) {
        this.kName = kName;
    }

    public String getvName() {
        return vName;
    }

    public void setvName(String vName) {
        this.vName = vName;
    }

    public String getIncludes() {
        return includes;
    }

    public void setIncludes(String includes) {
        this.includes = includes;
    }

    public String getExcludes() {
        return excludes;
    }

    public void setExcludes(String excludes) {
        this.excludes = excludes;
    }

    public JSONObject getTermJson() {
        return termJson;
    }

    public void setTermJson(JSONObject termJson) {
        this.termJson = termJson;
    }

    public JSONObject getWildcardJson() {
        return wildcardJson;
    }

    public void setWildcardJson(JSONObject wildcardJson) {
        this.wildcardJson = wildcardJson;
    }

    public JSONObject getMultiMatchJson() {
        return multiMatchJson;
    }

    public void setMultiMatchJson(JSONObject multiMatchJson) {
        this.multiMatchJson = multiMatchJson;
    }

    public List<Object[]> getRangeFields() {
        return rangeFields;
    }

    public void setRangeFields(List<Object[]> rangeFields) {
        this.rangeFields = rangeFields;
    }

    public List<String[]> getCompareFields() {
        return compareFields;
    }

    public void setCompareFields(List<String[]> compareFields) {
        this.compareFields = compareFields;
    }

    public List<String> getScripts() {
        return scripts;
    }

    public void setScripts(List<String> scripts) {
        this.scripts = scripts;
    }

    public QueryBean getNotQueryBean() {
        return notQueryBean;
    }

    public void setNotQueryBean(QueryBean notQueryBean) {
        this.notQueryBean = notQueryBean;
    }

    public List<QueryBean> getOrQueryBeans() {
        return orQueryBeans;
    }

    public void setOrQueryBeans(List<QueryBean> orQueryBeans) {
        this.orQueryBeans = orQueryBeans;
    }

    public List<QueryBean> getAndQueryBeans() {
        return andQueryBeans;
    }

    public void setAndQueryBeans(List<QueryBean> andQueryBeans) {
        this.andQueryBeans = andQueryBeans;
    }

    public String getExistsFields() {
        return existsFields;
    }

    public void setExistsFields(String existsFields) {
        this.existsFields = existsFields;
    }

    public Integer getMinDocCount() {
        return minDocCount;
    }

    public void setMinDocCount(Integer minDocCount) {
        this.minDocCount = minDocCount;
    }

    public QueryBuilder getQueryBuilder() {
        if (queryBuilder == null){
            return getBoolQueryBuilder();
        }
        return queryBuilder;
    }

    public void setQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public BoolQueryBuilder getBoolQueryBuilder(){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        if (getNotQueryBean() != null){
            boolQueryBuilder.mustNot(getNotQueryBean().getQueryBuilder());
        }

        if(getOrQueryBeans() != null && getOrQueryBeans().size() > 0){
            BoolQueryBuilder orBoolQuery = QueryBuilders.boolQuery();
            for (QueryBean reInfo:getOrQueryBeans()){
                orBoolQuery.should(reInfo.getQueryBuilder());
            }
            boolQueryBuilder.must(orBoolQuery);
        }

        if (getAndQueryBeans() != null && getAndQueryBeans().size() > 0){
            BoolQueryBuilder andBoolQuery = QueryBuilders.boolQuery();
            for (QueryBean reInfo: getAndQueryBeans()){
                andBoolQuery.must(reInfo.getQueryBuilder().boost(2f));
            }
            boolQueryBuilder.must(andBoolQuery);
        }

        if(getRangeFields() != null && getRangeFields().size() > 0){
            for (Object [] obj : getRangeFields()){
                RangeQueryBuilder range = QueryBuilders.rangeQuery(obj[0].toString());
                EsRangeType op = (EsRangeType)obj[1];

                Object value = obj[2];
                Float boost = null;
                if(value instanceof  Value){
                    boost = ((Value)value).getBoost();
                    value = ((Value)value).getValue();
                }

                if (EsRangeType.op_gt == op){
                    range.gt(value);
                }else if (EsRangeType.op_gte == op){
                    range.gte(value);
                }else if (EsRangeType.op_lt == op){
                    range.lt(value);
                }else if (EsRangeType.op_lte == op){
                    range.lte(value);
                }

                if (boost != null){
                    range.boost(boost);
                }

                boolQueryBuilder.must(range);
            }
        }

        // 字段比较
        if(getCompareFields() != null && getCompareFields().size() > 0){
            for (String[] obj : getCompareFields()) {
                String idOrCode = "doc['" + obj[0] + "'].value.compareTo(doc['" + obj[2] +"'].value)" + obj[1] + "0";
                Script script = new Script(idOrCode);
                ScriptQueryBuilder scriptQueryBuilder = QueryBuilders.scriptQuery(script);

                boolQueryBuilder.must(QueryBuilders.existsQuery(obj[0]));
                boolQueryBuilder.must(QueryBuilders.existsQuery(obj[2]));
                boolQueryBuilder.must(scriptQueryBuilder);
            }
        }

        // 自定义脚本
        if (getScripts() != null && getScripts().size() > 0){
            for (String idOrCode : getScripts()) {
                Script script = new Script(idOrCode);
                ScriptQueryBuilder scriptQueryBuilder = QueryBuilders.scriptQuery(script);
                boolQueryBuilder.must(scriptQueryBuilder);
            }
        }
        if (getExistsFields() != null){
            for (String field : getExistsFields().split(",")) {
                ExistsQueryBuilder existsQueryBuilder = QueryBuilders.existsQuery(field);
                boolQueryBuilder.must(existsQueryBuilder);
            }
        }

        // 自定义字段
        if (getTermJson() != null){
            for (String key : getTermJson().keySet()) {
                Object value = getTermJson().get(key);
                QueryBuilder qb = null;
                Float boost = null;
                if (value instanceof Value){
                    boost = ((Value)value).getBoost();
                    value = ((Value)value).getValue();
                }

                if (value instanceof Collection){
                    qb = QueryBuilders.termsQuery(key,(Collection<?>) value);
                }else if (value instanceof Object[]){
                    qb = QueryBuilders.termsQuery(key,(Object[]) value);
                }else {
                    qb = QueryBuilders.termQuery(key,value);
                }

                if (boost != null){
                    qb.boost(boost);
                }

                boolQueryBuilder.must(qb);
            }
        }

        // 模糊搜索字段
        if (getWildcardJson() != null){
            for (String key : getWildcardJson().keySet()) {
                Object value = getWildcardJson().get(key);
                Float boost = null;
                if (value instanceof Value){
                    boost = ((Value)value).getBoost();
                    value = ((Value)value).getValue();
                }
                QueryBuilder qb = QueryBuilders.wildcardQuery(key,value.toString());
                if (boost != null){
                    qb.boost(boost);
                }

                boolQueryBuilder.must(qb);
            }
        }

        // 分词字段
        if (getMultiMatchJson() != null){
            for (String fields : getMultiMatchJson().keySet()) {
                Object value = getMultiMatchJson().get(fields);
                Float boost = null;
                String analyzer = null;
                if (value instanceof Value){
                    boost =((Value)value).getBoost();
                    analyzer = ((Value)value).getAnalyzer();
                    value = ((Value)value).getValue();
                }
                MultiMatchQueryBuilder qb = QueryBuilders.multiMatchQuery(value, fields);
                if (analyzer != null){
                    qb.analyzer(analyzer);
                }
                if (boost != null){
                    qb.boost(boost);
                }
                boolQueryBuilder.must(qb);
            }
        }
        return boolQueryBuilder;
    }

    public void putTermField(String field,Object value){
        if (termJson == null){
            termJson = new JSONObject();
        }
        termJson.put(field,value);
    }

    public void putAndTermField(String field,Object value){
        QueryBean andReq = new QueryBean();
        andReq.putTermField(field,value);
        putAndQueryBean(andReq);
    }

    public void putAndQueryBean(QueryBean andQueryBean) {
        if (this.andQueryBeans == null){
            this.andQueryBeans = new ArrayList<>();
        }
        this.andQueryBeans.add(andQueryBean);
    }

    public void putOrQueryBean(QueryBean orQueryBean){
        if (this.orQueryBeans == null){
            this.orQueryBeans = new ArrayList<>();
        }
        this.orQueryBeans.add(orQueryBean);
    }

    public void putOrTermField(String field,Object value){
        QueryBean orReq = new QueryBean();
        orReq.putTermField(field,value);
        putOrQueryBean(orReq);
    }
}
