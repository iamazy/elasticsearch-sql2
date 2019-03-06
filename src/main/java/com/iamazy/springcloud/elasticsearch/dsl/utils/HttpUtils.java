package com.iamazy.springcloud.elasticsearch.dsl.utils;


/**
 * @author iamazy
 * @date 2019/3/5
 * @descrition
 **/
public class HttpUtils {

    public static boolean validateHttpMethodName(String httpMethod){
        return "GET".equalsIgnoreCase(httpMethod)||
                "POST".equalsIgnoreCase(httpMethod)||
                "PUT".equalsIgnoreCase(httpMethod)||
                "DELETE".equalsIgnoreCase(httpMethod);
    }
}
