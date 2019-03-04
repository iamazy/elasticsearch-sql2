package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method;

import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.QueryFieldType;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticSqlQueryField;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.expr.FieldSpecificMethodExpression;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.sql.QueryFieldParser;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

public abstract class AbstractFieldSpecificMethodQueryParser extends ParameterizedMethodQueryParser implements FieldSpecificMethodExpression {

    protected ParseActionListener parseActionListener;

    public AbstractFieldSpecificMethodQueryParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    protected abstract QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams);

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        //ignore extra params, subclass can override if necessary
        return null;
    }

    @Override
    protected AtomicQuery parseMethodQueryWithExtraParams(MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(defineFieldExpr(invocation), invocation.getQueryAs());

        AtomicQuery atomicQuery = null;
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            QueryBuilder originalQuery = buildQuery(invocation, queryField.getQueryFieldFullName(), extraParamMap);
            atomicQuery = new AtomicQuery(originalQuery);
        }

        if (queryField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            QueryBuilder originalQuery = buildQuery(invocation, queryField.getQueryFieldFullName(), extraParamMap);
            atomicQuery = new AtomicQuery(originalQuery, queryField.getNestedDocContextPath());
        }

        if (atomicQuery == null) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] query field can not support type[%s]", queryField.getQueryFieldType()));
        }

        parseActionListener.onAtomicMethodQueryConditionParse(queryField, invocation.getSqlArgs().getArgs());

        return atomicQuery;
    }
}
