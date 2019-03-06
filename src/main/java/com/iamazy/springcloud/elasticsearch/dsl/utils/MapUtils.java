package com.iamazy.springcloud.elasticsearch.dsl.utils;


import org.apache.commons.lang3.StringUtils;
import java.util.HashMap;
import java.util.Map;


/**
 * @author iamazy
 * @date 2019/3/3
 * @descrition
 **/
public class MapUtils {

    private static final String DOT=".";

    @SuppressWarnings("unchecked")
    public static Map<String, String> flat(Map<String,?> map, String parentKey) {
        String parent = parentKey;
        Map<String, String> dataInfo = new HashMap<>(0);
        for (Map.Entry<String, ?> entry : map.entrySet()) {
            if (!(entry.getValue() instanceof Map)) {
                if (StringUtils.isNotBlank(parent)) {
                    dataInfo.put(parent + DOT + entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : StringUtils.EMPTY);
                } else {
                    dataInfo.put(entry.getKey(), entry.getValue() != null ? entry.getValue().toString() : StringUtils.EMPTY);
                }
            } else {
                Map<String, ?> childMap = (Map<String, ?>) entry.getValue();
                if (StringUtils.isNotBlank(parent)) {
                    parent = parent + DOT + entry.getKey();
                } else {
                    parent = entry.getKey();
                }
                dataInfo.putAll(flat(childMap, parent));
                if(parent.contains(DOT)) {
                    parent = parent.substring(0, parent.lastIndexOf(DOT));
                }else{
                    parent=StringUtils.EMPTY;
                }

            }
        }
        return dataInfo;
    }
}
