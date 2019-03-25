package com.iamazy.elasticsearch.dsl;

import com.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import com.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

//@RunWith(SpringRunner.class)
//@SpringBootTest
public class ElasticsearchSqlApplicationTests {

    @Test
    public void contextLoads() {

        String sql="select * from device_info group by terms(portInfo$deviceInfo.deviceCategory,100)>(cardinality(portInfo$deviceInfo.deviceType)>terms(deviceInfo.deviceBrand,100),terms(port,90))";
        ElasticSql2DslParser elasticSql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);

        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }

}
