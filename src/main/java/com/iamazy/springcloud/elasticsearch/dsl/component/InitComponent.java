package com.iamazy.springcloud.elasticsearch.dsl.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;


/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/
@Slf4j
@Component
public class InitComponent implements InitializingBean {


    private ObjectMapper objectMapper=new ObjectMapper();

    @Override
    public void afterPropertiesSet() {
        log.info("初始化数据中...");

        log.info("初始化数据完成!!!");
    }

}
