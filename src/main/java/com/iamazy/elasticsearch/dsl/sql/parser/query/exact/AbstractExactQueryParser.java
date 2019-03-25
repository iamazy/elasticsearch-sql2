package com.iamazy.elasticsearch.dsl.sql.parser.query.exact;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.iamazy.elasticsearch.dsl.sql.enums.QueryFieldType;
import com.iamazy.elasticsearch.dsl.sql.enums.SqlConditionOperator;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.elasticsearch.dsl.sql.model.ElasticSqlQueryField;
import com.iamazy.elasticsearch.dsl.sql.parser.sql.QueryFieldParser;
import org.elasticsearch.index.query.QueryBuilder;

public abstract class AbstractExactQueryParser {

    protected ParseActionListener parseActionListener;

    public AbstractExactQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    protected AtomicQuery parseCondition(SQLExpr queryFieldExpr, SqlConditionOperator operator, Object[] params, String queryAs, IConditionExactQueryBuilder queryBuilder) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(queryFieldExpr, queryAs);

        AtomicQuery atomQuery = null;
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            QueryBuilder originalQuery = queryBuilder.buildQuery(queryField.getQueryFieldFullName(), operator, params);
            atomQuery = new AtomicQuery(originalQuery);
        }

        if (queryField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            QueryBuilder originalQuery = queryBuilder.buildQuery(queryField.getQueryFieldFullName(), operator, params);
            atomQuery = new AtomicQuery(originalQuery, queryField.getNestedDocContextPath());
        }

        if (atomQuery == null) {
            throw new ElasticSql2DslException(String.format("[syntax error] where condition field can not support type[%s]", queryField.getQueryFieldType()));
        }

        parseActionListener.onAtomicExactQueryConditionParse(queryField, params, operator);

        return atomQuery;
    }
}
