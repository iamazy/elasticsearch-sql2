package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.exact;

import com.alibaba.druid.sql.ast.expr.SQLInListExpr;
import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.SqlConditionOperator;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import com.iamazy.springcloud.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.SqlArgs;
import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.index.query.QueryBuilders;



public class InListQueryParser extends AbstractExactQueryParser {

    public InListQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public AtomicQuery parseInListQuery(SQLInListExpr inListQueryExpr, String queryAs, SqlArgs sqlArgs) {
        if (CollectionUtils.isEmpty(inListQueryExpr.getTargetList())) {
            throw new ElasticSql2DslException("[syntax error] In list expr target list cannot be blank");
        }

        Object[] targetInList = ElasticSqlArgConverter.convertSqlArgs(inListQueryExpr.getTargetList(), sqlArgs);
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
