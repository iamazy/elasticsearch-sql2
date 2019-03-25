package com.iamazy.elasticsearch.dsl.sql.parser.sql;


import com.iamazy.elasticsearch.dsl.sql.model.ElasticDslContext;

@FunctionalInterface
public interface QueryParser {
    void parse(ElasticDslContext dslContext);
}
