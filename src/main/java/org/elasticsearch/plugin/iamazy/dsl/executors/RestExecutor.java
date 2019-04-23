package org.elasticsearch.plugin.iamazy.dsl.executors;

import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestChannel;


/**
 * @author iamazy
 * @date 2019/4/23
 * @descrition
 **/
public interface RestExecutor {

    void execute(Client client,ElasticSqlParseResult parseResult, RestChannel channel);
    String execute(Client client,ElasticSqlParseResult parseResult);
}
