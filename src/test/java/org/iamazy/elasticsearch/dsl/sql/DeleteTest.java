package org.iamazy.elasticsearch.dsl.sql;

import org.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import org.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.junit.Test;

/**
 * @author iamazy
 * @date 2019/3/4
 * @descrition
 **/
public class DeleteTest {

    @Test
    public void delete(){

        String sql="DELETE from fruits where match_all() limit 1100";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toDelRequest().getSearchRequest()));
    }

    @Test
    public void query(){
        String sql="SELECT * FROM product.apple QUERY term(productName, 'iphone6s', 'boost:2.0f')";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }




}
