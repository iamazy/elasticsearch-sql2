package com.iamazy.springcloud.elasticsearch.dsl.sql;

import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.junit.Test;

/**
 * @author iamazy
 * @date 2019/3/6
 * @descrition
 **/
public class AggTest {

    @Test
    public void cardinality()
    {
        String sql="SELECT * FROM product.apple WHERE $buyers.buyerName = 'usa' and $buyers.productPrice < 200 group by cardinality(name,100)";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }

    @Test
    public void range()
    {
        String sql="SELECT * FROM product.apple group by range(name,segment(1,2),segment(2,3))";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }

    @Test
    public void rangeDate()
    {
        String sql="SELECT * FROM product.apple group by range(name,segment('2018-12-31','2019-1-22'),segment('2019-3-21','2019-4-1'))";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }

    @Test
    public void topHits()
    {
        String sql="SELECT * FROM product.apple group by topHits('top111',1)";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }

    @Test
    public void topHits2()
    {
        String sql="SELECT * FROM product.apple group by topHits(1)";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }
}
