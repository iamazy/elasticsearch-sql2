package org.elasticsearch.plugin.iamazy.dsl.executors;

import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestStatus;


/**
 * @author iamazy
 * @date 2019/4/23
 * @descrition
 **/
@Slf4j
public class ElasticDefaultRestExecutor implements RestExecutor{


    @Override
    public void execute(Client client, ElasticSqlParseResult parseResult, RestChannel channel) {
        if(parseResult.toDelRequest()!=null){
            log.warn("elasticsearch-sql插件暂不支持delete by query!!!");
        }else {
            String json = client.search(parseResult.toRequest()).actionGet().toString();
            BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.OK, XContentType.JSON.mediaType(),json);
            channel.sendResponse(bytesRestResponse);
        }
    }

    @Override
    public String execute(Client client,ElasticSqlParseResult parseResult) {
        SearchRequest searchRequest=parseResult.toRequest();
        return client.search(searchRequest).actionGet().toString();
    }
}
