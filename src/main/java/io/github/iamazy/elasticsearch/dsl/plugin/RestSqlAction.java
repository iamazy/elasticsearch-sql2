package io.github.iamazy.elasticsearch.dsl.plugin;

import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import io.github.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.*;

import java.io.IOException;


/**
 * @author iamazy
 * @date 2019/4/23
 * @descrition
 **/
@Slf4j
public class RestSqlAction extends BaseRestHandler {

    RestSqlAction(Settings settings, RestController restController){
        super(settings);
        restController.registerHandler(RestRequest.Method.POST,"/_isql/_explain",this);
        restController.registerHandler(RestRequest.Method.GET,"/_isql/_explain",this);
        restController.registerHandler(RestRequest.Method.POST,"/_isql",this);
        restController.registerHandler(RestRequest.Method.GET,"/_isql",this);
    }


    @Override
    public String getName() {
        return "isql";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) throws IOException {
        try(XContentParser parser=restRequest.contentOrSourceParamParser()){
            parser.mapStrings().forEach((k,v)->restRequest.params().putIfAbsent(k,v));
        }catch (IOException e){
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST,XContentType.JSON.mediaType(),"please use json format params, like: {\"sql\":\"select * from test\"}"));
        }
        try {
            String sql=restRequest.param("sql");
            if(StringUtils.isBlank(sql)){
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST,XContentType.JSON.mediaType(),"{\"error\":\"sql语句不能为空!!!\"}"));
            }
            ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
            ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            if(restRequest.path().endsWith("/_explain")){
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder.value(parseResult.toRequest().source())));
            }else{
                return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK,builder.value(nodeClient.search(parseResult.toRequest()).actionGet())));
            }
        }catch (ElasticSql2DslException e){
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR,XContentType.JSON.mediaType(),"{\"error\":\""+e.getMessage()+"\"}"));
        }
    }

}
