package com.lc.es;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.time.DateUtils;
import org.junit.platform.commons.util.StringUtils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
