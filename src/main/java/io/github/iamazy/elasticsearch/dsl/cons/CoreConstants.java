package io.github.iamazy.elasticsearch.dsl.cons;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author iamazy
 * @date 2019/3/25
 **/
public interface CoreConstants {

    ObjectMapper OBJECT_MAPPER=new ObjectMapper();

    String COMMA = ",";
    String COLON = ":";
    String DOLLAR = "$";
    String DOT=".";
    String POUND="#";
    String HIGHLIGHTER="h#";
    String DEFAULT_ES_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
}
