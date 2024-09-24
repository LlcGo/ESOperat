package com.lc;

import com.lc.es.ESConfig;
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
        ESConfig esConfig = new ESConfig();
        RestHighLevelClient restHighLevelClient = esConfig.getRestHighLevelClient();
        System.out.println(restHighLevelClient);
        GetIndexResponse test = restHighLevelClient.indices().get(new GetIndexRequest("test"), RequestOptions.DEFAULT);
        System.out.println(test);
    }

}
