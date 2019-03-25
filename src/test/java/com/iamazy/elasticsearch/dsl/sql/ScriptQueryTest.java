package com.iamazy.elasticsearch.dsl.sql;

import com.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import com.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.junit.Test;

/**
 * @author iamazy
 * @date 2019/3/6
 * @descrition
 **/
public class ScriptQueryTest {


    @Test
    public void scriptTest(){
        String sql="select * from device_search where script_query('if (ctx._source.user == \"kimchy\") {ctx._source.likes++;}','name:iamazy,age:23,gender:male')";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);
        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }

    @Test
    public void scriptQuery(){
        QueryBuilder queryBuilder= QueryBuilders.scriptQuery(new Script("params['_source']['markProcess'].size()==3"));
        System.out.println("fdsfds");
    }
}
