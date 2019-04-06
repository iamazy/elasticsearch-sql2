package io.github.iamazy.elasticsearch.dsl.sql.model;

import lombok.Getter;
import org.elasticsearch.search.aggregations.AggregationBuilder;

/**
 * @author iamazy
 * @date 2019/3/7
 * @descrition
 **/
@Getter
public class AggregationQuery {

    private AggregationBuilder aggregationBuilder;

    public AggregationQuery(AggregationBuilder aggregationBuilder){
        this.aggregationBuilder=aggregationBuilder;
    }
}
