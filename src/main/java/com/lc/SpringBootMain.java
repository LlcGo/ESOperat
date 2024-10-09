package com.lc;

import com.alibaba.fastjson.JSONObject;
import com.lc.es.ESConfig;
import com.lc.es.ElasticsearchUtils;
import com.lc.es.bean.Field;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

@SpringBootApplication
public class SpringBootMain {
    public static void main(String[] args) {
        SpringApplication.run(SpringBootMain.class,args);
    }

    static ElasticsearchUtils elasticsearchUtils;
    static {
        elasticsearchUtils = new ElasticsearchUtils("192.168.66.178:19201,192.168.66.179:19201,192.168.66.180:19201", "elastic", "123456");
    }

    @PostConstruct
    public void Test() throws IOException {
//        testGetIndex();
//        testMapping();
//        testGetMappingFields();
        testGetFields();
    }

    public void testGetFields(){
        List<Field> test = elasticsearchUtils.getFields("test");
        System.out.println(test);
    }


    public static void testGetMappingFields(){
        JSONObject test = elasticsearchUtils.getMappingFields("test");
        System.out.println(test);
    }

    public void testMapping(){
        ElasticsearchUtils elastic = new ElasticsearchUtils("192.168.66.175:19201,192.168.66.176:19201,192.168.66.177:19201", "elastic", "123456");
        JSONObject test = elastic.getMapping("test");
        JSONObject jsonObject = test.getJSONObject("properties");
        System.out.println(jsonObject);
    }

    public void testGetIndex(){
        ElasticsearchUtils elasticsearchUtils = new ElasticsearchUtils("192.168.66.175:19201,192.168.66.176:19201,192.168.66.177:19201", "elastic", "123456");
        GetIndexResponse test = elasticsearchUtils.getIndex("test");
        System.out.println(test);
    }

    public void testElasticsearchUtils() throws IOException {
        ElasticsearchUtils elastic = new ElasticsearchUtils("192.168.66.175:19201,192.168.66.176:19201,192.168.66.177:19201", "elastic", "123456");
        RestHighLevelClient client = elastic.getClient();
        GetIndexResponse test = client.indices().get(new GetIndexRequest("test"), RequestOptions.DEFAULT);
        System.out.println(test);
    }

    public void testConfig() throws IOException {
        ESConfig esConfig = new ESConfig();
        RestHighLevelClient restHighLevelClient = esConfig.getRestHighLevelClient();
        System.out.println(restHighLevelClient);
        GetIndexResponse test = restHighLevelClient.indices().get(new GetIndexRequest("test"), RequestOptions.DEFAULT);
        System.out.println(test);
    }

}
