package com.lc.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lc.es.bean.Field;
import com.lc.es.bean.QueryBean;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.platform.commons.util.StringUtils;
import org.omg.CORBA.PUBLIC_MEMBER;
import org.springframework.util.CollectionUtils;

import javax.swing.*;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class ElasticsearchUtils {
    private RestHighLevelClient client;

    private float slowTime = 1.0f;

    private ESConfig config = new ESConfig();

    private long clientTime = 0;// 客户端加载时间

    public static final String _id = "_id";

    public static final String _index = "_index";

    public ElasticsearchUtils(ESConfig config){
        this.config = config;
    }

    public ElasticsearchUtils(String httpAddresses){
        config.setHttpAddresses(httpAddresses);
    }

    public ElasticsearchUtils(String httpAddress,String userName,String passWord){
        config.setHttpAddresses(httpAddress);
        config.setUserName(userName);
        config.setPassword(passWord);
    }

    public ElasticsearchUtils(String httpAddress,float slowTime){
        this.slowTime = slowTime;
        config.setHttpAddresses(httpAddress);
    }

    /**
     * 获取client
     */
    public RestHighLevelClient getClient(){
        if (client == null){
            try {
                System.out.println("Elasticsearch初始化开始......");
                client = config.getRestHighLevelClient();
                clientTime = System.currentTimeMillis();
            }catch (Exception e){
                 printException(e);
                 slowLog("初始化RestHighLeveClient失败...");
            }
        }
        return client;
    }

    /**
     * 获取索引信息
     * @param index
     */
    public GetIndexResponse getIndex(String index){
        GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        try {
            return getClient().indices().get(getIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            printException(e);
        }
        return null;
    }

    public List<String> getIndices(){
        Response response;
        try{
            Request request = new Request("get", "/_cat/indices?v&h=index&format=json");
            response = getClient().getLowLevelClient().performRequest(request);
            JSONArray index = JSONObject.parseArray(EntityUtils.toString(response.getEntity()));
            ArrayList<String> strings = new ArrayList<>();
            for (int i = 0; i < index.size(); i++) {
                strings.add(index.getJSONObject(i).getString("index"));
            }
            return strings;
        }catch (Exception e){
            printException(e);
            return null;
        }
    }

    public JSONObject getMapping(String index){
        Response response;
        try {
            Request request = new Request("get", "/" + index + "/_mappings");
            response = getClient().getLowLevelClient().performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());
            JSONObject responseBodyJson = JSONObject.parseObject(responseBody);
            JSONObject responseBodyIndexJson = responseBodyJson.getJSONObject(index);
            return responseBodyIndexJson.getJSONObject("mappings");
        } catch (IOException e) {
            printException(e);
        }
        return null;
    }

    public JSONObject getMappingFields(String index){
        JSONObject mapping = getMapping(index);
        if (mapping != null){
            return mapping.getJSONObject("properties");
        }
        return null;
    }


    public List<Field> getFields(String index){
        List<Field> cols = new ArrayList<>();
        JSONObject json = getMappingFields(index);
        if (json != null){
            return getFields(json);
        }
        return cols;
    }

    public List<Field> getFields(JSONObject json){
        List<Field> clos = new ArrayList<>();
        if (json != null){
            for (String key : json.keySet()) {
                Field col = new Field();
                col.setName(key);

                JSONObject cjson = json.getJSONObject(key);
                if (cjson.containsKey("type")){
                    col.setType(cjson.getString("type"));
                }

                if (cjson.containsKey("format")){
                    col.setFormat(cjson.getString("format"));
                }

                if(cjson.containsKey("index")){
                    col.setIndex(cjson.getString("index"));
                }

                if (cjson.containsKey("store")){
                    col.setStore(cjson.getString("store"));
                }

                if(cjson.containsKey("analyzer")){
                    col.setAnalyzer(cjson.getString("analyzer"));
                }

                if (cjson.containsKey("properties")){
                    col.setProperties(getFields(cjson.getJSONObject("properties")));
                }

                if (cjson.containsKey("fields")){
                    col.setFields(getFields(cjson.getJSONObject("fields")));
                }

                clos.add(col);
            }
        }
        return clos;
    }

    public List<JSONObject> searchDocs(String index, QueryBean query){
        if (query == null){
            query = new QueryBean();
        }

        SearchResponse searchResponse = searchDocsResponse(index, query);
        if (searchResponse != null && searchResponse.status().getStatus() == 200){
            long totalHits = searchResponse.getHits().getTotalHits().value;
            long length = searchResponse.getHits().getHits().length;

            slowLog(index," search:" + length + "/" + totalHits);
            return setSearchResponse(searchResponse,index,query.getIncludes());
        }
        return null;
    }

    private List<JSONObject> setSearchResponse(SearchResponse searchResponse, String index, String fields) {
        boolean mulit = false;
        if (index.contains(",") || index.contains("*")){
            mulit = true;

        }
        List<JSONObject> sourceList = new ArrayList<>();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            searchHit.getSourceAsMap().put(_id,searchHit.getId());
            JSONObject record = JSONObject.parseObject(JSON.toJSONString(searchHit.getSourceAsMap()));
            if (mulit){
                record.put("_index",searchHit.getIndex());
            }
            sourceList.add(dealJson(record, fields));
        }

        return sourceList;
    }

    public <T> List<T> queryEsData(String index, Class<T> clazz, LocalDateTime st,LocalDateTime ed,Set<String> indexList) throws IOException {
        String[] indices = {"test","test01"};
        // 若索引不存在直接返回
        int notExistCount = 0;
        for (String str : indices) {
            str = str.replace("*","");
            if (!indexList.contains(str)){
                notExistCount++;
            }
        }
        if (notExistCount ==indices.length){
            return null;
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("absTime")
                .gte(st).lt(ed).timeZone("GMT+8")));
        searchSourceBuilder.sort("absTime",SortOrder.ASC);
        List<SearchHit> list = searchAll(getClient(),searchSourceBuilder,indices);
        if (CollectionUtils.isEmpty(list)){
            return null;
        }
        return list.stream().map(o->JSON.parseObject(o.getSourceAsString(),clazz)).collect(Collectors.toList());
    }

    private <T> List<T> queryEsData(String [] index,Class<T> clazz,String st, String ed,Set<String> indexList) throws IOException {
        // 若索引不存在直接返回
        int notExitsCount = 0;
        for (String s : index) {
          s = s.replace("*","");
          if (!indexList.contains(s)){
              notExitsCount++;
          }
        }

        if (notExitsCount == index.length){
            return null;
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("absTime").gte(st).lt(ed).timeZone("GMT+8")));
        searchSourceBuilder.sort("absTime",SortOrder.ASC);
        List<SearchHit> searchHits = searchAll(client, searchSourceBuilder, index);
        if (CollectionUtils.isEmpty(searchHits)){
            return null;
        }
        return searchHits.stream().map(o->JSON.parseObject(o.getSourceAsString(),clazz)).collect(Collectors.toList());
    }

    private List<SearchHit> searchAll(RestHighLevelClient client, SearchSourceBuilder searchSourceBuilder, String[] indices) throws IOException {
        TimeValue scrollTime = TimeValue.timeValueMinutes(1l);
        int batchSize = 5000;
        searchSourceBuilder.size(batchSize);
        SearchRequest searchRequest = new SearchRequest(indices);
        searchRequest.source(searchSourceBuilder);
        searchRequest.scroll(scrollTime);
        searchRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // 滚动查询
        String scrollId = searchResponse.getScrollId();
        ArrayList<SearchHit> searchHits = new ArrayList<>();
        try {
            while (searchResponse.getHits() != null && searchResponse.getHits().getHits().length > 0){
                searchHits.addAll(Arrays.asList(searchResponse.getHits().getHits()));
                //下一页
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scrollId);
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId =  searchResponse.getScrollId();
            }
        }finally {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest,RequestOptions.DEFAULT);
        }
        return searchHits;
    }

    private List<SearchHit> searchAll(SearchSourceBuilder searchSourceBuilder,RestHighLevelClient client,String [] indices) throws IOException {
        // 设置时间
        TimeValue timeValue = TimeValue.timeValueMinutes(1l);
        int batchSize = 500;
        searchSourceBuilder.size(batchSize);
        SearchRequest searchRequest = new SearchRequest(indices);
        searchRequest.scroll(timeValue);
        searchRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

        List<SearchHit> searchHits = new ArrayList<>();
        String scrollId = search.getScrollId();
        try {
            while (search.getHits() != null && search.getHits().getHits().length > 0){
                searchHits.addAll(Arrays.asList(search.getHits().getHits()));

                SearchScrollRequest searchScrollRequest = new SearchScrollRequest(scrollId);
                searchScrollRequest.scroll(scrollId);
                search = client.scroll(searchScrollRequest, RequestOptions.DEFAULT);
                scrollId = search.getScrollId();
            }
        }finally {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest,RequestOptions.DEFAULT);
        }
        return searchHits;
    }




    private JSONObject dealJson(JSONObject data, String fields) {
        if (data != null && StringUtils.isNotBlank(fields)){
           Map<String,String> map =  getFieldMap(fields);
            for (String field : map.keySet()) {
                String nname = map.get(field);
                data.put(nname,data.get(field));
                data.remove(field);
            }
        }
        return null;
    }

    public static Map<String, String> getFieldMap(String fields) {
        HashMap<String, String> map = new HashMap<>();
        String[] fieldArr = fields.split(",");
        for (int i = 0; i <fieldArr.length; i++) {
            String field =fieldArr[i];
            field = field.trim();
            if (field.indexOf(" ") != -1){
                String nname = field.split("\\s+")[1].trim();
                field = fields.split("\\s+")[0].trim();
                map.put(field,nname);
            }
        }
        return map;
    }

    private SearchResponse searchDocsResponse(String index,QueryBean query){
        return searchDocsponse(index,query,null);
    }

    private SearchResponse searchDocsponse(String index, QueryBean query, Long timeValueMinutes) {
         if (query == null){
             query = new QueryBean();
         }
        SearchRequest searchRequest = new SearchRequest(index.split(","));;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query.getQueryBuilder());

        if (StringUtils.isNotBlank(query.getIncludes()) || StringUtils.isNotBlank(query.getExcludes())){
            searchSourceBuilder.fetchSource(getFieldNameArr(query.getIncludes()),getFieldNameArr(query.getExistsFields()));
            searchSourceBuilder.fetchSource(true);
        }

        if (StringUtils.isNotBlank(query.getOrderBy())){
            String[] sortFieldArr = query.getOrderBy().split(",");
            for (String sort : sortFieldArr) {
                SortOrder order = SortOrder.ASC;
                sort = sort.trim();

                if (sort.indexOf(" ") != -1){
                    String orderType = sort.split("\\s+")[1].trim();
                    sort = sort.split("\\s+")[0].trim();
                    if ("desc".equalsIgnoreCase(orderType)){
                        order = SortOrder.DESC;
                    }
                }
                searchSourceBuilder.sort(sort,order);
            }
        }

        if (query.getPageSize() != null && query.getPageSize() > 0){
            // 分页
            if (query.getPageIndex() != null && query.getPageIndex() > 0){
                searchSourceBuilder.from((query.getPageIndex() -1) * query.getPageSize());
            }
            searchSourceBuilder.size(query.getPageSize());
        }

        long startTime = System.currentTimeMillis();
        SearchResponse searchResponse = null;
        try {
            searchSourceBuilder.trackTotalHits(true);
            searchRequest.source(searchSourceBuilder);
            searchRequest.searchType(SearchType.DEFAULT);
            if (timeValueMinutes != null){
                searchRequest.scroll(TimeValue.timeValueMinutes(timeValueMinutes));
            }
            searchResponse = getClient().search(searchRequest,RequestOptions.DEFAULT);
        }catch (Exception e){
            printException(e);
        }
        slowLog(index,searchRequest,startTime);
        return searchResponse;
    }

    /**
     * 获取字段信息
     *
     * @param fields
     * @return
     */
    private String[] getFieldNameArr(String fields) {
        if (StringUtils.isBlank(fields)){
            return null;
        }
        String[] fieldArr = fields.split(",");
        String[] reArr = new String[fieldArr.length];
        for (int i = 0; i < fieldArr.length; i++) {
            String field = fieldArr[i];
            field = field.trim();
            if (field.indexOf(" ") != -1){
                field = fields.split("\\s+")[0].trim();
            }
            reArr[i] = field;
        }
        return reArr;
    }


    private void slowLog(Object content){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(simpleDateFormat.format(new Date()) + "=>" + content);
    }

    private void slowLog(String index,String content){
        StringBuilder buffer = new StringBuilder();
        if(StringUtils.isNotBlank(index)){
            buffer.append("index=" + index);
        }
        buffer.append("      " + content);
        slowLog(buffer.toString());
    }

    private void slowLog(String index, ActionRequest request, long startTime) {
        long endTime = System.currentTimeMillis();
        float excTime = (float) (endTime - startTime) / 1000;
        if (excTime >= slowTime) {
            slowLog(index, "es执行时间：" + excTime + "s    " + request);
        }
    }

    private void printException(Exception e){
        if(e.getMessage().contains("Request cannot be executed; I/O reactor status: STOPPED")){
            if(System.currentTimeMillis() - clientTime > 300000){
                // 距离上次初始化客户端超过5分钟
                this.client = null;
                slowLog("Request cannot be executed; I/O reactor status: STOPPED    clients.clear");
            }
        }
    }



}
