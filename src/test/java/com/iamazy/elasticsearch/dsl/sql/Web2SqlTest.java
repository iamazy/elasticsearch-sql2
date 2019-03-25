package com.iamazy.elasticsearch.dsl.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
 * @date 2019/2/21
 * @descrition
 **/
public class Web2SqlTest {

    ObjectMapper objectMapper=new ObjectMapper();
    @Test
    public void web2SqlTest() throws IOException {

        Map map=objectMapper.readValue("{\"page\":1,\"pagesize\":10,\"filter\":{\"deviceCategory\":[\"SS\",\"VSS\"],\"deviceBrand\":[\"dahua\",\"xiaohua\"],\"vulType\":[\"ERROR\"]}}",Map.class);

        StringBuilder sqlBuilder = new StringBuilder("select * from {} where ");
        if (map.containsKey("filter")) {
            Map<String, List<String>> filter = (Map<String, List<String>>) map.get("filter");
            Set<Map.Entry<String, List<String>>> set = filter.entrySet().stream().filter(item -> item.getValue() != null && item.getValue().size() > 0).collect(Collectors.toSet());
            for (Map.Entry<String, List<String>> entry : set) {
                sqlBuilder.append(entry.getKey()).append(" in ( ");
                for (String item : entry.getValue()) {
                    sqlBuilder.append("'").append(item).append("',");
                }
                sqlBuilder.deleteCharAt(sqlBuilder.lastIndexOf(","));
                sqlBuilder.append(" )");
                sqlBuilder.append(" and ");
            }

            if (filter.containsKey("vulType")) {
                sqlBuilder.append(" resType ='vul_info' and vulInfo.vulExist='true'");
            } else {
                sqlBuilder.append(" resType ='portInfo'");
            }
        }
        sqlBuilder.append(" and portInfo.deviceInfo.deviceCategory not in ('Monitor','Network Equipment','OA','Voice and Video','Room Wiring') ");
        sqlBuilder.append("and lastModified between 1533398400000 and ").append(System.currentTimeMillis());

        sqlBuilder.append(" order by lastModified desc ");
        if (map.containsKey("page") && map.containsKey("pagesize")) {
            int pagesize = Integer.valueOf(map.get("pagesize").toString());
            int limitStart = (Integer.valueOf(map.get("page").toString()) - 1) * pagesize;
            int limitEnd = limitStart + pagesize;
            sqlBuilder.append(" limit ").append(limitStart).append(",").append(limitEnd);
        } else {
            sqlBuilder.append(" limit ").append(0).append(",").append(50);
        }

        System.out.println(sqlBuilder);
    }
}
