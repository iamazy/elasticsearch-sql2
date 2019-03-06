package com.iamazy.springcloud.elasticsearch.dsl.sql;

import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.junit.Test;

/**
 * @author iamazy
 * @date 2019/3/6
 * @descrition
 **/
public class DeleteTest {

    @Test
    public void delete(){

        String sql="DELETE from fruits where match_all() limit 1100";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql, new String[]{"port"});

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toDelRequest().getSearchRequest()));
    }

    @Test
    public void query(){
        String sql="SELECT * FROM product.apple QUERY term(productName, 'iphone6s', 'boost:2.0f')";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql,new String[]{"productName"});

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }


    @Test
    public void nested(){
        String sql="SELECT * FROM product.apple WHERE fruits$fruit.name = 'usa'";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);
        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }

    @Test
    public void nested2(){
        String sql="SELECT * FROM product.apple WHERE ($buyers.buyerName = 'usa' or minPrice > 100)  and $buyers.productPrice < 200";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }
}
