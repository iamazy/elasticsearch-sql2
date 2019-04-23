package org.elasticsearch.plugin.iamazy.dsl.executors;

import org.apache.commons.lang3.StringUtils;

/**
 * @author iamazy
 * @date 2019/4/23
 * @descrition
 **/
public class ActionRequestRestExecutorFactory {

    public static RestExecutor createExecutor(String format){
        if(StringUtils.isBlank(format)){
            return new ElasticDefaultRestExecutor();
        }else if("csv".equalsIgnoreCase(format)){
            return new CsvResultRestExecutor();
        }
        return null;
    }
}
