package io.github.iamazy.elasticsearch.dsl.sql;

import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import io.github.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.junit.Test;

public class ScoreTest {


    @Test
    public void boostingTest(){
        String sql="select * from fruit query boosting(h#name='apple',h#weight>100,0.2)";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void functionScoreTest(){
        String sql="select * from fruit query function_score(h#name='a',script_score(name='ddd','fsdfsdf0','a:1,b:2'),random_score(age>90,101092339,'date'),weight(a>1,3),weight(b<4,4),weight(c='aa',5),weight(d is not null,9))";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }
}
