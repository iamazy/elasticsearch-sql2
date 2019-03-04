package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.exact;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.SqlConditionOperator;
import com.iamazy.springcloud.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.SqlArgs;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.listener.ParseActionListener;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;


public class BinaryAtomicQueryParser extends AbstractAtomicExactQueryParser {

    public BinaryAtomicQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public AtomicQuery parseBinaryQuery(SQLBinaryOpExpr binQueryExpr, String queryAs, SqlArgs sqlArgs) {
        SQLBinaryOperator binaryOperator = binQueryExpr.getOperator();

        //EQ NEQ
        if (SQLBinaryOperator.Equality == binaryOperator || SQLBinaryOperator.LessThanOrGreater == binaryOperator || SQLBinaryOperator.NotEqual == binaryOperator) {
            Object targetVal = ElasticSqlArgConverter.convertSqlArg(binQueryExpr.getRight(), sqlArgs);

            SqlConditionOperator operator = SQLBinaryOperator.Equality == binaryOperator ? SqlConditionOperator.Equality : SqlConditionOperator.NotEqual;

            return parseCondition(binQueryExpr.getLeft(), operator, new Object[]{targetVal}, queryAs, (queryFieldName, operator1, rightParamValues) -> {
                QueryBuilder eqQuery = QueryBuilders.termQuery(queryFieldName, rightParamValues[0]);
                if (SqlConditionOperator.Equality == operator1) {
                    return eqQuery;
                }
                else {
                    return QueryBuilders.boolQuery().mustNot(eqQuery);
                }
            });
        }

        //GT GTE LT LTE
        if (SQLBinaryOperator.GreaterThan == binaryOperator || SQLBinaryOperator.GreaterThanOrEqual == binaryOperator
                || SQLBinaryOperator.LessThan == binaryOperator || SQLBinaryOperator.LessThanOrEqual == binaryOperator) {

            SqlConditionOperator operator = null;
            if (SQLBinaryOperator.GreaterThan == binaryOperator) {
                operator = SqlConditionOperator.GreaterThan;
            }
            else if (SQLBinaryOperator.GreaterThanOrEqual == binaryOperator) {
                operator = SqlConditionOperator.GreaterThanOrEqual;
            }
            else if (SQLBinaryOperator.LessThan == binaryOperator) {
                operator = SqlConditionOperator.LessThan;
            }
            else if (SQLBinaryOperator.LessThanOrEqual == binaryOperator) {
                operator = SqlConditionOperator.LessThanOrEqual;
            }

            Object targetVal = ElasticSqlArgConverter.convertSqlArg(binQueryExpr.getRight(), sqlArgs);
            return parseCondition(binQueryExpr.getLeft(), operator, new Object[]{targetVal}, queryAs, (queryFieldName, operator12, rightParamValues) -> {
                QueryBuilder rangeQuery = null;
                if (SqlConditionOperator.GreaterThan == operator12) {
                    rangeQuery = QueryBuilders.rangeQuery(queryFieldName).gt(rightParamValues[0]);
                }
                else if (SqlConditionOperator.GreaterThanOrEqual == operator12) {
                    rangeQuery = QueryBuilders.rangeQuery(queryFieldName).gte(rightParamValues[0]);
                }
                else if (SqlConditionOperator.LessThan == operator12) {
                    rangeQuery = QueryBuilders.rangeQuery(queryFieldName).lt(rightParamValues[0]);
                }
                else if (SqlConditionOperator.LessThanOrEqual == operator12) {
                    rangeQuery = QueryBuilders.rangeQuery(queryFieldName).lte(rightParamValues[0]);
                }
                return rangeQuery;
            });
        }

        //IS / IS NOT
        if (SQLBinaryOperator.Is == binaryOperator || SQLBinaryOperator.IsNot == binaryOperator) {
            if (!(binQueryExpr.getRight() instanceof SQLNullExpr)) {
                throw new ElasticSql2DslException("[syntax error] Is/IsNot expr right part should be null");
            }
            SqlConditionOperator operator = SQLBinaryOperator.Is == binaryOperator ? SqlConditionOperator.IsNull : SqlConditionOperator.IsNotNull;
            return parseCondition(binQueryExpr.getLeft(), operator, null, queryAs, (queryFieldName, operator13, rightParamValues) -> {
                ExistsQueryBuilder existsQuery = QueryBuilders.existsQuery(queryFieldName);
                if (SqlConditionOperator.IsNull == operator13) {
                    return QueryBuilders.boolQuery().mustNot(existsQuery);
                }
                return existsQuery;
            });
        }

        throw new ElasticSql2DslException(String.format("[syntax error] Can not support binary query type[%s]", binQueryExpr.toString()));
    }
}
