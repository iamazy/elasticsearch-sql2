package org.elasticsearch.plugin.iamazy.dsl.executors;

import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestChannel;

/**
 * @author iamazy
 * @date 2019/4/23
 * @descrition
 **/
public class CsvResultRestExecutor implements RestExecutor {


    @Override
    public void execute(Client client, ElasticSqlParseResult parseResult, RestChannel channel) {

    }

    @Override
    public String execute(Client client, ElasticSqlParseResult parseResult) {
        return null;
    }
}
