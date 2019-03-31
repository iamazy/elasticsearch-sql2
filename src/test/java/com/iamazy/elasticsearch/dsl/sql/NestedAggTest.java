package com.iamazy.elasticsearch.dsl.sql;

import com.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import com.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.junit.Test;

/**
 * @author iamazy
 * @date 2019/3/25
 * @descrition
 **/
public class NestedAggTest {


    @Test
    public void nestedquery(){
        String nested="select * from port_info where aaa$ipInfo$vulVerifyList.vulType=20 group by nested(info)>(nested(info.device),terms(info.port)) limit 0,0";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(nested);
        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }
}
