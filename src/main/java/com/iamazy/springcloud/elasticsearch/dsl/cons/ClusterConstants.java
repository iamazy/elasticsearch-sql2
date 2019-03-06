package com.iamazy.springcloud.elasticsearch.dsl.cons;


import java.util.*;

/**
 * @author iamazy
 * @date 2019/2/18
 * @descrition
 **/
public class ClusterConstants {


    public static Map<String, List<String>> CLUSTER_2_NODES = new HashMap<String, List<String>>(){
        {
            put("elastic-test", Collections.singletonList("127.0.0.1"));
        }
    };
}
