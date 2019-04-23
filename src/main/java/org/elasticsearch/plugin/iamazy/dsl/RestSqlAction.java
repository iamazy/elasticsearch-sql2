package org.elasticsearch.plugin.iamazy.dsl;

import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import io.github.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugin.iamazy.dsl.executors.ActionRequestRestExecutorFactory;
import org.elasticsearch.plugin.iamazy.dsl.executors.RestExecutor;
import org.elasticsearch.rest.*;

import java.io.IOException;
import java.util.*;

/**
 * @author iamazy
 * @date 2019/4/23
 * @descrition
 **/
@Slf4j
public class RestSqlAction extends BaseRestHandler {

    public RestSqlAction(Settings settings, RestController restController){
        super(settings);
        restController.registerHandler(RestRequest.Method.POST,"/_isql/_explain",this);
        restController.registerHandler(RestRequest.Method.GET,"/_isql/_explain",this);
        restController.registerHandler(RestRequest.Method.POST,"/_isql",this);
        restController.registerHandler(RestRequest.Method.GET,"/_isql",this);
    }


    @Override
    public String getName() {
        return "sql-action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) throws IOException {
        try(XContentParser parser=restRequest.contentOrSourceParamParser()){
            parser.mapStrings().forEach((k,v)->restRequest.params().putIfAbsent(k,v));
        }catch (IOException e){
            log.warn("Please use json format params, like: {\"sql\":\"SELECT * FROM test\"}");
        }

        String sql=restRequest.param("sql");
        if(StringUtils.isBlank(sql)){
            sql=restRequest.content().utf8ToString();
        }
        try {
            ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
            ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);

            if(restRequest.path().endsWith("/_explain")){
                final String jsonExplanation=parseResult.toDsl(parseResult.toRequest());
                return restChannel -> restChannel.sendResponse(new BytesRestResponse(RestStatus.OK, XContentType.JSON.mediaType(), jsonExplanation));
            }else{
                Map<String,String> params=restRequest.params();
                RestExecutor restExecutor= ActionRequestRestExecutorFactory.createExecutor(params.get("format"));
                return channel -> restExecutor.execute(nodeClient,parseResult,channel);
            }
        }catch (ElasticSql2DslException e){
            e.printStackTrace();
        }
        return null;
    }
}
