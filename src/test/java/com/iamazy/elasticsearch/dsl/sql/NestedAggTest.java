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
    public void nested() {
        String sql = "select * from device_info where portInfo$deviceInfo.deviceCategory=10 group by terms(portInfo$deviceInfo.deviceCategory,100)>(cardinality(portInfo.deviceInfo.deviceType)>terms(deviceInfo.deviceBrand,100),terms(port,90)),topHits(1)";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }
}
