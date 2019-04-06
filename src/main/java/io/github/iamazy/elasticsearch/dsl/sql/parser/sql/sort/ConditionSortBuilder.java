package io.github.iamazy.elasticsearch.dsl.sql.parser.sql.sort;

import org.elasticsearch.search.sort.FieldSortBuilder;

public interface ConditionSortBuilder {
    FieldSortBuilder buildSort(String idfName);
}