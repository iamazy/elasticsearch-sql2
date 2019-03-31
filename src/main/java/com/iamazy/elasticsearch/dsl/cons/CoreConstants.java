package com.iamazy.elasticsearch.dsl.cons;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author iamazy
 * @date 2019/3/25
 * @descrition
 **/
public interface CoreConstants {

    ObjectMapper OBJECT_MAPPER=new ObjectMapper();

    String COMMA = ",";
    String COLON = ":";
    String DOLLAR = "$";
    String DOT=".";
}
