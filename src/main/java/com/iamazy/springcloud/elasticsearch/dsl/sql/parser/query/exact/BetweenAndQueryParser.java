package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.exact;

import com.alibaba.druid.sql.ast.expr.SQLBetweenExpr;
import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.SqlConditionOperator;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import com.iamazy.springcloud.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.SqlArgs;
import org.elasticsearch.index.query.QueryBuilders;



public class BetweenAndQueryParser extends AbstractExactQueryParser {

    public BetweenAndQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    public AtomicQuery parseBetweenAndQuery(SQLBetweenExpr betweenAndExpr, String queryAs, SqlArgs sqlArgs) {
        Object from = ElasticSqlArgConverter.convertSqlArg(betweenAndExpr.getBeginExpr(), sqlArgs);
        Object to = ElasticSqlArgConverter.convertSqlArg(betweenAndExpr.getEndExpr(), sqlArgs);

        if (from == null || to == null) {
            throw new ElasticSql2DslException("[syntax error] Between Expr only support one of [number,date] arg type");
        }

        return parseCondition(betweenAndExpr.getTestExpr(), SqlConditionOperator.BetweenAnd, new Object[]{from, to}, queryAs, (queryFieldName, operator, rightParamValues) -> QueryBuilders.rangeQuery(queryFieldName).gte(rightParamValues[0]).lte(rightParamValues[1]));
    }
}
