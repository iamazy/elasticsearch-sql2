package io.github.iamazy.elasticsearch.dsl.sql.parser.sql;


import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticDslContext;

@FunctionalInterface
public interface QueryParser {
    void parse(ElasticDslContext dslContext);
}
