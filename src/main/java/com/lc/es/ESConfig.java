package com.lc.es;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.apache.http.client.config.RequestConfig.Builder;
import org.junit.platform.commons.util.StringUtils;


public class ESConfig {

    private String httpAddresses = "192.168.66.175:19201";

    private String schema = "http";

    private String userName = "elastic";

    private String password = "123456";

    private int connectTimeOut = 10000;

    private int socketTimeOut = 300000;

    private int connectionRequestTime = 5000;

    private int maxConnectNum = 100;

    private int maxConnectPerRoute = 100;

    private RestHighLevelClient client;

    private RestClientBuilder builder;

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getConnectTimeOut() {
        return connectTimeOut;
    }

    public void setConnectTimeOut(int connectTimeOut) {
        this.connectTimeOut = connectTimeOut;
    }

    public int getSocketTimeOut() {
        return socketTimeOut;
    }

    public void setSocketTimeOut(int socketTimeOut) {
        this.socketTimeOut = socketTimeOut;
    }

    public int getConnectionRequestTime() {
        return connectionRequestTime;
    }

    public int getMaxConnectNum() {
        return maxConnectNum;
    }

    public void setConnectionRequestTime() {
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback(){
            @Override
            public Builder customizeRequestConfig(Builder builder) {
                builder.setConnectTimeout(connectTimeOut);
                builder.setSocketTimeout(socketTimeOut);
                builder.setConnectionRequestTimeout(connectionRequestTime);
                return builder;
            }
        });
    }



    public void setMaxConnectNum() {
        builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> {
            httpAsyncClientBuilder.setMaxConnTotal(maxConnectNum);
            httpAsyncClientBuilder.setMaxConnPerRoute(maxConnectPerRoute);

            if(StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(password)){
               final BasicCredentialsProvider basicCredentialsProvider = new BasicCredentialsProvider();
                basicCredentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(userName,password));
                httpAsyncClientBuilder.setDefaultCredentialsProvider(basicCredentialsProvider);
            }
            return httpAsyncClientBuilder;
        });

    }

    public int getMaxConnectPerRoute() {
        return maxConnectPerRoute;
    }

    public void setMaxConnectPerRoute(int maxConnectPerRoute) {
        this.maxConnectPerRoute = maxConnectPerRoute;
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public void setClient(RestHighLevelClient client) {
        this.client = client;
    }

    public RestClientBuilder getBuilder() {
        return builder;
    }

    public void setBuilder(RestClientBuilder builder) {
        this.builder = builder;
    }

    public String getHttpAddresses() {
        return httpAddresses;
    }

    public void setHttpAddresses(String httpAddresses) {
        this.httpAddresses = httpAddresses;
    }

    public RestHighLevelClient getRestHighLevelClient(){
        HttpHost[] httpHosts = new HttpHost[httpAddresses.split(",").length];
        int i = 0;
        for (String address : httpAddresses.split(",")) {
          httpHosts[i] = new HttpHost(address.split(":")[0],Integer.valueOf(address.split(":")[1]),schema);
          i++;
        }
        builder = RestClient.builder(httpHosts);
        setConnectionRequestTime();
        setMaxConnectNum();
        client = new RestHighLevelClient(builder);
        return client;
    }

    public void close() {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
