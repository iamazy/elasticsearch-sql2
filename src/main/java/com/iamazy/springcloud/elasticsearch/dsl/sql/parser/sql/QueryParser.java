package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.sql;


import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticDslContext;

@FunctionalInterface
public interface QueryParser {
    void parse(ElasticDslContext dslContext);
}
