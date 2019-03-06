package com.iamazy.springcloud.elasticsearch.dsl.client;

import com.iamazy.springcloud.elasticsearch.dsl.cons.ClusterConstants;
import com.iamazy.springcloud.elasticsearch.dsl.cons.CodecConstants;
import com.iamazy.springcloud.elasticsearch.dsl.cons.CoreConstants;
import com.iamazy.springcloud.elasticsearch.dsl.utils.HttpUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.*;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.client.sniff.NodesSniffer;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.range.ParsedRange;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.tophits.ParsedTopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static com.iamazy.springcloud.elasticsearch.dsl.ssl.SslContextKits.TRUST_ALL_CERTS;


/**
 * @author iamazy
 * @date 2019/2/15
 * @descrition
 **/
@Slf4j
public class ElasticClientManager {

    private final static Map<String, RestHighLevelClient> REST_HIGH_LEVEL_CLIENT_MAP = new HashMap<>(0);
    private static String DEFAULT_TOKEN = CodecConstants.BASE_64.encodeAsString("elastic:321++nsw".getBytes());
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
                            sslContext.init(null, TRUST_ALL_CERTS, new java.security.SecureRandom());
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
            if (hit.getSourceAsMap().get("dataInfoJoin") instanceof Map) {
                hit.getSourceAsMap().put("parent", ((Map) hit.getSourceAsMap().get("dataInfoJoin")).get("parent"));
            }
            result.add(hit.getSourceAsMap());
        }

        Map<String, Object> body = new HashMap<>(0);
        if (searchResponse.getAggregations() != null) {
            Map<String, Object> aggMap = new LinkedHashMap<>(0);
            for (Aggregation aggregation : searchResponse.getAggregations()) {
                if(aggregation instanceof ParsedTerms) {
                    Terms buckets = searchResponse.getAggregations().get(aggregation.getName());
                    Map<String, Object> countMap = new LinkedHashMap<>(0);
                    for (Terms.Bucket bucket : buckets.getBuckets()) {
                        countMap.put(bucket.getKeyAsString(), bucket.getDocCount());
                    }
                    aggMap.put(aggregation.getName(), countMap);
                }
                else if(aggregation instanceof ParsedTopHits){
                    TopHits topHits=searchResponse.getAggregations().get(aggregation.getName());
                    SearchHit[] hits = topHits.getHits().getHits();
                    List<Map<String,Object>> topHitList=new ArrayList<>(0);
                    for(SearchHit hit:hits){
                        topHitList.add(hit.getSourceAsMap());
                    }
                    aggMap.put(aggregation.getName(),topHitList);
                }
                else if(aggregation instanceof ParsedCardinality){
                    Cardinality cardinality=searchResponse.getAggregations().get(aggregation.getName());
                    aggMap.put(aggregation.getName(),cardinality.getValue());
                }
                else if(aggregation instanceof ParsedRange){
                    Range range=searchResponse.getAggregations().get(aggregation.getName());
                    List<Map<String,Object>> rangeList=new ArrayList<>(0);
                    for(Range.Bucket bucket:range.getBuckets()){
                        Map<String,Object> rangeItem=new HashMap<>(0);
                        rangeItem.put(bucket.getKeyAsString(),bucket.getDocCount());
                        rangeList.add(rangeItem);
                    }
                    aggMap.put(aggregation.getName(),rangeList);
                }
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


    /**
     * restful请求方式
     *
     * @param cluster
     * @param requestBody
     * @param params
     * @return 返回正确的结果就返回InputStream,返回错误的结果就返回错误信息
     */
    public Object query(String cluster, Map<String, String> requestBody, Map<String, String> params) {
        if (requestBody.containsKey("method") && requestBody.containsKey("url") && requestBody.containsKey("body") && requestBody.containsKey("token")) {
            String httpMethod = requestBody.get("method");
            if(!HttpUtils.validateHttpMethodName(httpMethod)){
                return "无效的HTTP请求方法!!!,只允许[GET]|[POST]|[PUT]|[DELETE]";
            }
            String url = requestBody.get("url");
            String body = requestBody.get("body");
            String token = requestBody.get("token");
            Request request = new Request(httpMethod.toUpperCase(), url);
            request.setOptions(requestOptions(token));
            request.setJsonEntity(body);
            if (MapUtils.isNotEmpty(params)) {
                for (Map.Entry<String, String> entry : params.entrySet()) {
                    request.addParameter(entry.getKey(), entry.getValue());
                }
            }
            try {
                Response response = getRestHighLevelClient(cluster).getLowLevelClient().performRequest(request);
                return response.getEntity().getContent();
            } catch (Exception e) {
                return "返回结果出错!!!";
            }
        }else{
            return "请求参数不完整!!!";
        }
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

    private BulkProcessor.Listener listener = new BulkProcessor.Listener() {
        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            int numberOfActions = request.numberOfActions();
            log.debug("Executing bulk [{}] with {} requests",
                    executionId, numberOfActions);
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request,
                              BulkResponse response) {
            if (response.hasFailures()) {
                log.warn("Bulk [{}] executed with failures", executionId);
                log.error(response.buildFailureMessage());
            } else {
                log.debug("Bulk [{}] completed in {} milliseconds",
                        executionId, response.getTook().getMillis());
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request,
                              Throwable failure) {
            log.error("Failed to execute bulk", failure);
        }
    };
}
