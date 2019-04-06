package io.github.iamazy.elasticsearch.dsl.sql;

import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import io.github.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.junit.Test;

/**
 * @author iamazy
 * @date 2019/3/25
 * @descrition
 **/
public class ScrollTest {


    @Test
    public void scroll(){

        //scroll by 前面表示scroll id过期时间，后面表示scroll id
       // String sql="select * from search order by lastModified routing by 'fdsfdsfdf' scroll by '2121m' limit 20,10";
        String sql="select * from search order by lastModified routing by 'fdsfdsfdf' scroll by '2121m','fdsfdsfdsfsdfdsf' limit 20,10";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }

}
