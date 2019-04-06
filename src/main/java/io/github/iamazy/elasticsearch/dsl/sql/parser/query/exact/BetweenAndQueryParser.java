package io.github.iamazy.elasticsearch.dsl.sql.parser.query.exact;

import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import io.github.iamazy.elasticsearch.dsl.sql.enums.SqlConditionOperator;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import io.github.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import org.elasticsearch.index.query.QueryBuilders;


/**
 * @author iamazy
 */
public class BetweenAndQueryParser extends AbstractExactQueryParser {


    public AtomicQuery parseBetweenAndQuery(SQLBetweenExpr betweenAndExpr, String queryAs) {
        Object from = ElasticSqlArgConverter.convertSqlArg(betweenAndExpr.getBeginExpr());
        Object to = ElasticSqlArgConverter.convertSqlArg(betweenAndExpr.getEndExpr());

        if (from == null || to == null) {
            throw new ElasticSql2DslException("[syntax error] Between Expr only support one of [number,date] arg type");
        }

        return parseCondition(betweenAndExpr.getTestExpr(), SqlConditionOperator.BetweenAnd, new Object[]{from, to}, queryAs, (queryFieldName, operator, rightParamValues) -> QueryBuilders.rangeQuery(queryFieldName).gte(rightParamValues[0]).lte(rightParamValues[1]));
    }
}
