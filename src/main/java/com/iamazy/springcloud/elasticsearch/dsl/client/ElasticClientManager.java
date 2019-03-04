package com.iamazy.springcloud.elasticsearch.dsl.client;

import com.iamazy.springcloud.elasticsearch.dsl.cons.ClusterConstants;
import com.iamazy.springcloud.elasticsearch.dsl.cons.CodecConstants;
import com.iamazy.springcloud.elasticsearch.dsl.cons.CoreConstants;
import com.iamazy.springcloud.elasticsearch.dsl.ssl.SslContextKits;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.*;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.client.sniff.NodesSniffer;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.*;



/**
 * Copyright 2018-2019 iamazy Logic Ltd
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author iamazy
 * @date 2019/2/15
 * @descrition
 **/
@Slf4j
public class ElasticClientManager {

    private final static Map<String, RestHighLevelClient> REST_HIGH_LEVEL_CLIENT_MAP = new HashMap<>(0);
    private static String DEFAULT_TOKEN = CodecConstants.BASE_64.encodeAsString("iamazy:654321".getBytes());
    private static RequestOptions DEFAULT_REQUEST_OPTIONS = requestOptions(DEFAULT_TOKEN);
    private static Header[] defaultHeaders = new Header[]{new BasicHeader("Authorization", "Basic " + DEFAULT_TOKEN)};
    private Sniffer sniffer;
    private static SniffOnFailureListener sniffOnFailureListener = new SniffOnFailureListener();

    public ElasticClientManager(String cluster) {
        List<String> addresses = ClusterConstants.CLUSTER_2_NODES.get(cluster);
        if (!REST_HIGH_LEVEL_CLIENT_MAP.containsKey(cluster)) {
            RestClientBuilder restClientBuilder = RestClient.builder(addresses.stream().map(address -> {
                String[] addr = address.split(CoreConstants.COLON);
                return new HttpHost(addr[0], Integer.valueOf(addr[1]), "https");
            }).toArray(HttpHost[]::new))
                    .setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS)
                    .setMaxRetryTimeoutMillis(60000)
                    .setFailureListener(sniffOnFailureListener)
                    .setRequestConfigCallback(builder -> builder.setConnectTimeout(5000)
                            .setSocketTimeout(60000))
                    .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                        httpAsyncClientBuilder.setSSLHostnameVerifier((s, sslSession) -> true);
                        SSLContext sslContext = null;
                        try {
                            sslContext = SSLContext.getInstance("TLS");
                            sslContext.init(null, SslContextKits.TRUST_ALL_CERTS, new java.security.SecureRandom());
                        } catch (NoSuchAlgorithmException | KeyManagementException e) {
                            e.printStackTrace();
                        }
                        httpAsyncClientBuilder.setSSLContext(sslContext);
                        httpAsyncClientBuilder.setDefaultHeaders(Arrays.asList(defaultHeaders));
                        return httpAsyncClientBuilder;
                    });
            RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
            NodesSniffer nodesSniffer = new ElasticsearchNodesSniffer(restHighLevelClient.getLowLevelClient(),
                    ElasticsearchNodesSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT,
                    ElasticsearchNodesSniffer.Scheme.HTTPS);
            sniffer = Sniffer.builder(restHighLevelClient.getLowLevelClient()).setSniffIntervalMillis(1000).setNodesSniffer(nodesSniffer).build();
            sniffOnFailureListener.setSniffer(sniffer);
            log.info("开启elasticsearch集群节点嗅探功能!!!");
            REST_HIGH_LEVEL_CLIENT_MAP.put(cluster, restHighLevelClient);
        }

    }

    public RestHighLevelClient getRestHighLevelClient(String cluster) {
        return REST_HIGH_LEVEL_CLIENT_MAP.get(cluster);
    }


    public Map<String, Object> search(String cluster, SearchRequest searchRequest, String token) throws IOException {
        SearchResponse searchResponse = getRestHighLevelClient(cluster).search(searchRequest, requestOptions(token));
        List<Map<String, Object>> result = new ArrayList<>(0);
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            hit.getSourceAsMap().put("id", hit.getId());
            hit.getSourceAsMap().put("routing", hit.field("_routing").getValue());
            hit.getSourceAsMap().put("parent", ((Map) hit.getSourceAsMap().get("dataInfoJoin")).get("parent"));
            result.add(hit.getSourceAsMap());
        }

        Map<String, Object> body = new HashMap<>(0);
        if (searchResponse.getAggregations() != null) {
            Map<String, Object> aggMap = new LinkedHashMap<>(0);
            for (Aggregation aggregation : searchResponse.getAggregations()) {
                Terms buckets = searchResponse.getAggregations().get(aggregation.getName());
                Map<String, Object> countMap = new LinkedHashMap<>(0);
                for (Terms.Bucket bucket : buckets.getBuckets()) {
                    countMap.put(bucket.getKeyAsString(), bucket.getDocCount());
                }
                aggMap.put(aggregation.getName(), countMap);
            }
            body.put("aggregation", aggMap);
        }

        body.put("query", searchRequest.source().toString());
        body.put("took", searchResponse.getTook().getMillis());
        body.put("total", searchResponse.getHits().totalHits);
        body.put("size", searchResponse.getHits().getHits().length);
        if (result.size() > 0) {
            body.put("data", result);
        }
        return body;
    }

    public Map<String, Object> get(String cluster,String index,String type, String id, String routing, String token) throws IOException {
        GetRequest getRequest = new GetRequest(index, type, id).routing(routing);
        GetResponse getResponse = getRestHighLevelClient(cluster).get(getRequest, requestOptions(token));
        return getResponse.getSourceAsMap();
    }

    public static void ping() throws IOException {
        if (REST_HIGH_LEVEL_CLIENT_MAP.size() > 0) {
            for (Map.Entry<String, RestHighLevelClient> entry : REST_HIGH_LEVEL_CLIENT_MAP.entrySet()) {
                boolean ping = entry.getValue().ping(DEFAULT_REQUEST_OPTIONS);
                if (ping) {
                    log.debug("ping ··· " + entry.getKey() + " 成功!!!");
                } else {
                    log.error("集群:" + entry.getKey() + "嗅探失败,请检查原因!!!");
                }
            }
        }
    }


    private static RequestOptions requestOptions(String token) {
        //设置允许返回的最大字节数
        HttpAsyncResponseConsumerFactory responseConsumerFactory = new HttpAsyncResponseConsumerFactory
                .HeapBufferedResponseConsumerFactory(Integer.MAX_VALUE);
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setHttpAsyncResponseConsumerFactory(responseConsumerFactory);
        builder.addHeader("Authorization", "Basic " + token);
        return builder.build();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        sniffer.close();
    }


}
