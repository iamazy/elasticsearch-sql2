package com.iamazy.elasticsearch.dsl.sql;

import com.iamazy.elasticsearch.dsl.sql.model.ElasticSqlParseResult;
import com.iamazy.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
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
        String sql="select * from device_search where match(portInfo.deviceInfo.deviceBrand,'大华','prefix_length:21') and term(portInfo.port,80)";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql,new String[]{"portInfo.port","portInfo.deviceInfo.deviceBrand"});
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void testParseLimit(){
        String sql="select portInfo.deviceInfo.deviceBrand,portInfo.port,ipInfo.ip from device_search where match_phrase(portInfo.deviceInfo.deviceBrand,'大华') and term(portInfo.port,80) limit 2,9";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql,new String[]{"portInfo.port","portInfo.deviceInfo.deviceBrand"});
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void testParseTermsAgg(){
        String sql="select * from device_search where portInfo.port is not null and ipInfo.ip is not null group by terms(portInfo.port,5000),terms(ipInfo.ip,600)";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql,new String[]{"portInfo.port","portInfo.deviceInfo.deviceBrand"});
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void testParseDeviceExists(){
        String sql="select * from device_search where deviceBrand in ( 'dahua','xiaohua' ) and vulType in ( 'ERROR' ) and deviceCategory in ( 'SS','VSS' ) and  resType ='vul_info' and vulInfo.vulExist='true' and portInfo.deviceInfo.deviceCategory not in ('Monitor','Network Equipment','OA','Voice and Video','Room Wiring') and lastModified between 1533398400000 and 1550799200976 group by terms(portInfo.port,50) order by lastModified desc limit 0,10";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }


    @Test
    public void testHasParent(){
        String sql="select * from device_search where has_parent('ipInfo',ipInfo.ip between '10.10.2.221' and '221.221.221.221')";
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

    @Test
    public void testVulInfo(){
        String sql="select * from device_search where (deviceCategory in ( 'SS','VSS' ) and  resType ='vul_info' and vulInfo.vulExist='true' and portInfo.deviceInfo.deviceCategory not in ('Monitor','Network Equipment','OA','Voice and Video','Room Wiring') and lastModified between 1533398400000 and 1550737921037 ) or (portInfo.port >=80 and ipInfo.ip='10.10.2.222') order by lastModified desc  limit 0,10";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void testQuerySize(){
        String sql="select * from device_search where resType='vul_info' group by terms(vulInfo.vulType,50) limit 0,0";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void testParseFlatTermsAgg(){
        String sql="select * from device_search where portInfo.port is not null and ipInfo.ip is not null group by terms(portInfo.port,5000),terms(ipInfo.ip,100)";
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }



    @Test
    public void testQueryString(){
        String sql="select * from device_search where resType='port_info' and portInfo.deviceInfo.deviceLocation.zhCountry in ( '埃塞俄比亚','安提瓜和巴布达','中国' )  and portInfo.deviceInfo.deviceCategory in ( 'VCS','RSS' )  and vulInfo.vulType in ( '越权访问','未授权访问' )  and portInfo.port in ( '3702' )  and portInfo.deviceInfo.deviceLocation.zhProvince in ( '江苏','辽宁' )  and portInfo.deviceInfo.deviceType in ( 'AP','Router' )  and portInfo.protocol in ( 'MOXA NPORT','ONVIF' )  and portInfo.deviceInfo.deviceBrand in ( '3COM','3M' )  and portInfo.deviceInfo.deviceModel in ( '20-20','2104' )  and lastModified between 1533398400000 and 1550816881862 and portInfo.deviceInfo.deviceCategory not in ('Monitor','Network Equipment','OA','Voice and Video','Room Wiring')  order by lastModified desc  limit 0,10\n";
        sql=String.format(sql,"");
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

    @Test
    public void test2(){
        String sql="select * from device_search where query_string('大华')";
        sql=String.format(sql,"device_search");
        ElasticSql2DslParser sql2DslParser=new ElasticSql2DslParser();
        ElasticSqlParseResult parseResult = sql2DslParser.parse(sql);
        System.out.println(parseResult.toPrettyDsl(parseResult.toRequest()));
    }

}
