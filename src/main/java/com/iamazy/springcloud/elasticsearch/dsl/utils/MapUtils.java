package com.iamazy.springcloud.elasticsearch.dsl.utils;

import org.apache.commons.lang3.StringUtils;


import java.util.HashMap;
import java.util.Map;

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
 * @date 2019/3/3
 * @descrition
 **/
public class MapUtils {


    public static Map<String, String> flat(Map<String,?> map, String parentKey) {
        String parent = parentKey;
        Map<String, String> dataInfo = new HashMap<>(0);
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            if (!(entry.getValue() instanceof Map)) {
                if (StringUtils.isNotBlank(parent)) {
                    dataInfo.put(parent + "." + entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : "");
                } else {
                    dataInfo.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : "");
                }
            } else {
                Map<String, ?> childMap = (Map<String, ?>) entry.getValue();
                if (StringUtils.isNotBlank(parent)) {
                    parent = parent + "." + entry.getKey();
                } else {
                    parent = entry.getKey();
                }
                dataInfo.putAll(flat(childMap, parent));
                if(parent.contains(".")) {
                    parent = parent.substring(0, parent.lastIndexOf("."));
                }else{
                    parent="";
                }

            }
        }
        return dataInfo;
    }

}
