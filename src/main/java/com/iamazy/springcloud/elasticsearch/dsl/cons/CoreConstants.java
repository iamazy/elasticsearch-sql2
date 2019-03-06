package com.iamazy.springcloud.elasticsearch.dsl.cons;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author iamazy
 * @date 2019/2/20
 * @descrition
 **/
public interface CoreConstants {

    ObjectMapper OBJECT_MAPPER=new ObjectMapper();

    String COMMA = ",";
    String COLON = ":";
}