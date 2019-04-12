package io.github.iamazy.elasticsearch.dsl.sql;

import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import io.github.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.junit.Test;

/**
 * @author iamazy
 * @date 2019/4/12
 * @descrition
 **/
public class SqlTest {

    @Test
    public void test(){
        String sql="select * from aaa where  match_phrase(h#aaa$AA.text,'哈哈哈')  order by lastModified desc limit 0,20";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);
        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }
}
