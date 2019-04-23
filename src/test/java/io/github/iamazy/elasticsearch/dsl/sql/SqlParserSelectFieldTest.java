package io.github.iamazy.elasticsearch.dsl.sql;

import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import io.github.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.junit.Test;

/**
 * @author iamazy
 * @date 2019/2/20
 * @descrition
 **/
public class SqlParserSelectFieldTest {

    @Test
    public void testParseFromMethodSource(){
        String sql="select * from fruit query match(h#name,'苹果','prefix_length:21,boost:2.0f') and term(weight,80)";
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


    /**
     * 高亮显示 在字段前面用h#标识
     */
    @Test
    public void testParseFlatTermsAgg(){
        String sql="select * from fruit where match(h#$aaaaa.bb,'fdsfdsfdsf') and fuzzy(h#bbb,'fdsfdf') and h#name is not null and color is not null group by terms(weight,5000),terms(category,100)  limit 0,0";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }


    @Test
    public void testHighlighter(){
        String sql="select * from fruit where h#$macInfo.name='0xsdfs' limit 0,0";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void testHighlighter2(){
        String sql="select * from device_info where match_phrase(deviceLocation.zhProvince,'首尔') or match_phrase(h#$aaa$ipInfo.mac,'0x10192j')  order by lastModified desc limit 0,10";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void test2(){
        String sql="select * from fruit where query_string('h#苹果','fields:deviceInfo.device*,deviceLocation.address','analyzer:ik_smart,phrase_slop:1')";
        sql=String.format(sql,"device_search");
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void test3(){
        String sql="select * from aa";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

}
