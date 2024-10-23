package com.lc;

import com.alibaba.fastjson.JSONObject;
import com.lc.es.ESConfig;
import com.lc.es.ElasticsearchUtils;
import com.lc.es.bean.Field;
import com.lc.es.bean.QueryBean;
import org.assertj.core.util.Lists;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.*;

import static com.lc.es.ElasticsearchUtils.getFieldMap;

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
//        testGetFields();

        // TODO
        // QueryBean relationParam = getRelationParam("vid1", "vid2", 1, Lists.list(1,2,3));
        // List<JSONObject> test = elasticsearchUtils.searchDocs("test",relationParam);
        Map<String, String> fieldMap = getFieldMap("field_keyword");
        System.out.println(fieldMap);
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

    public static <T0,T1> QueryBean getRelationParam(String param0,String param3,T0 id, T1 inIds) {
        QueryBean param = new QueryBean();
        return getRelationParamInOneRecord(param, param0,param3, id, inIds);
    }

    private static <T0, T1> QueryBean getRelationParamInOneRecord(QueryBean param, String param0,String param3,T0 id, T1 inIds) {
        boolean isIn = false;
        if (inIds != null){
            if (inIds instanceof Collection){
                if (!CollectionUtils.isEmpty((Collection<?>) inIds)){
                    isIn = true;
                }
            }else{
                isIn = true;
            }
        }

        if (isIn){
            QueryBean param1 = new QueryBean();
            param1.putTermField(param0, id);
            param1.putAndTermField(param3, inIds);
            param.putOrQueryBean(param1);
            QueryBean param2 = new QueryBean();
            param2.putTermField(param3, id);
            param2.putAndTermField(param0, inIds);
            param.putOrQueryBean(param2);

        } else {
            param.putOrTermField(param0, id);
            param.putOrTermField(param3, id);
        }

        return param;
    }

}
