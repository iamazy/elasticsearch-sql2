package com.iamazy.elasticsearch.dsl.sql;

import com.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import com.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.junit.Test;

/**
 * @author iamazy
 * @date 2019/2/20
 * @descrition
 **/
public class SqlParserSelectFieldTest {

    @Test
    public void testParseFromMethodSource(){
        String sql="select * from fruit where match(name,'苹果','prefix_length:21') and term(weight,80)";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }





    @Test
    public void testHasParent(){
        String sql="select * from fruit where has_parent('vegetable',weight between 100 and 400)";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void testHasChild(){
        String sql="select * from device_search where has_child('imageInfo',portInfo.port in (10,20,30),1,4)";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }


    @Test
    public void testParseFlatTermsAgg(){
        String sql="select * from fruit where name is not null and color is not null group by terms(weight,5000),terms(category,100)  limit 0,0";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }


    @Test
    public void test2(){
        String sql="select * from fruit where query_string('苹果')";
        sql=String.format(sql,"device_search");
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

}
