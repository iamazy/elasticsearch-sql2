package com.iamazy.springcloud.elasticsearch.dsl.sql;

import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.junit.Test;

/**
 * @author iamazy
 * @date 2019/3/6
 * @descrition
 **/
public class ScriptTest {

    @Test
    public void scriptTest(){
        String sql="select * from qq where script_query('if (ctx._source.user == \"kimchy\") {ctx._source.likes++;}','name:iamazy,age:23,gender:male')";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);
        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }
}
