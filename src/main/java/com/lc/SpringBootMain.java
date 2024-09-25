package com.lc;

import com.lc.es.ESConfig;
import com.lc.es.ElasticsearchUtils;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;
import java.io.IOException;

@SpringBootApplication
public class SpringBootMain {
    public static void main(String[] args) {
        SpringApplication.run(SpringBootMain.class,args);
    }

    @PostConstruct
    public void Test() throws IOException {
         testElasticsearchUtils();
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
