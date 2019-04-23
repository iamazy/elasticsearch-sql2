package io.github.iamazy.elasticsearch.dsl.sql;

import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import io.github.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.junit.Test;

/**
 * @author iamazy
 * @date 2019/4/12
 * @descrition
 **/
public class SqlTest {

    @Test
    public void test() {
        String sql = "select * from aa.bb where   query_string('h#windows') and  term(h#deviceLocation.zhCountry,'俄罗斯') and  term(h#deviceLocation.zhCountry,'中国') group by terms(osInfo.osFamily,20),cardinality(osInfo.osFamily),terms(idxProtocol,20),cardinality(idxProtocol),terms(deviceLocation.zhCountry,20),cardinality(deviceLocation.zhCountry),nested(softInfo)>(terms(softInfo.softType,20),cardinality(softInfo.softType)),terms(deviceInfo.deviceCategory,20),cardinality(deviceInfo.deviceCategory),terms(deviceLocation.zhCity,20),cardinality(deviceLocation.zhCity),terms(deviceLocation.zhProvince,20),cardinality(deviceLocation.zhProvince),terms(deviceInfo.deviceModel,20),cardinality(deviceInfo.deviceModel),nested(idxVulVerifyList)>(terms(idxVulVerifyList.vulType,20),cardinality(idxVulVerifyList.vulType)),terms(deviceInfo.deviceType,20),cardinality(deviceInfo.deviceType),terms(idxPort,20),cardinality(idxPort),terms(deviceInfo.deviceBrand,20),cardinality(deviceInfo.deviceBrand) limit 0,0";
        ElasticSql2DslParser elasticSql2DslParser = new ElasticSql2DslParser();
        ElasticSqlParseResult elasticSqlParseResult = elasticSql2DslParser.parse(sql);
        System.out.println(elasticSqlParseResult.toPrettyDsl(elasticSqlParseResult.toRequest()));
    }

}
