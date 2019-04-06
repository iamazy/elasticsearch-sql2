package io.github.iamazy.elasticsearch.dsl.sql.parser.query.exact;

import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import io.github.iamazy.elasticsearch.dsl.sql.enums.SqlConditionOperator;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import io.github.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.index.query.QueryBuilders;


/**
 * @author iamazy
 */
public class InListQueryParser extends AbstractExactQueryParser {


    public AtomicQuery parseInListQuery(SQLInListExpr inListQueryExpr, String queryAs) {
        if (CollectionUtils.isEmpty(inListQueryExpr.getTargetList())) {
            throw new ElasticSql2DslException("[syntax error] In list expr target list cannot be blank");
        }

        Object[] targetInList = ElasticSqlArgConverter.convertSqlArgs(inListQueryExpr.getTargetList());
        SqlConditionOperator operator = inListQueryExpr.isNot() ? SqlConditionOperator.NotIn : SqlConditionOperator.In;

        return parseCondition(inListQueryExpr.getExpr(), operator, targetInList, queryAs, (queryFieldName, operator1, rightParamValues) -> {
            if (SqlConditionOperator.NotIn == operator1) {
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(queryFieldName, rightParamValues));
            }
            else {
                return QueryBuilders.termsQuery(queryFieldName, rightParamValues);
            }
        });
    }
}
