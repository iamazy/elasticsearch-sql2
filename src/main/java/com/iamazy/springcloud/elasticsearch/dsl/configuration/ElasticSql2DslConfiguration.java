package com.iamazy.springcloud.elasticsearch.dsl.configuration;

import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.ElasticSql2DslParser;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


/**
 * @author iamazy
 * @date 2019/2/21
 * @descrition
 **/
@Configuration
public class ElasticSql2DslConfiguration {

    @Bean(name = "elasticSql2DslParser")
    public ElasticSql2DslParser elasticSql2DslParser(){
        return new ElasticSql2DslParser();
    }
}
