package com.iamazy.springcloud.elasticsearch.dsl.sql;

import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.junit.Test;

/**
 * Copyright 2018-2019 iamazy Logic Ltd
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author iamazy
 * @date 2019/2/20
 * @descrition
 **/
public class SqlParserSelectFieldTest {

    @Test
    public void testParseFromMethodSource(){
        String sql="select * from fruits where match(name,'apple','prefix_length:21') and term(color,'red')";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql,new String[]{"name","color"});
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void testParseLimit(){
        String sql="select * from fruits where match_phrase(name,'apple') and term(color,'red') limit 2,9";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql,new String[]{"name","color"});
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void testParseTermsAgg(){
        String sql="select * from fruits where name is not null and color is not null group by terms(weight,5000),terms(color,600)";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql,new String[]{"name","color"});
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void testParseDeviceExists(){
        String sql="select * from fruits where name in ( 'apple','orange' ) and color in ( 'red' ) and lastModified between 1533398400000 and 1550799200976 group by terms(color,50) order by lastModified desc limit 0,10";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void testTerms(){
        String sql="select * from fruits where color in ('red','yellow','green') limit 2,9";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }


    @Test
    public void testHasParent(){
        String sql="select * from fruits where has_parent('vegetable',weight between 10.1 and 10.5)";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void testHasChild(){
        String sql="select * from fruits where has_child('apple',color in ('red','green','yellow'),1,4)";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }




    @Test
    public void testParseFlatTermsAgg(){
        String sql="select * from fruits where name is not null and color is not null group by terms(weight,5000),terms(price,100)";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql,new String[]{"name","color"});
        parseResult.setTopStatsAgg(true);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }





    @Test
    public void test2(){
        String sql="select * from %s where match_all() and port in (80,801,92)";
        sql=String.format(sql,"device_search");
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        parseResult.setTopStatsAgg(true);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

}
