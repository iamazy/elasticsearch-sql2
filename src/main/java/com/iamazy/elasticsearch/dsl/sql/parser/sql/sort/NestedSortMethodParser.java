package com.iamazy.elasticsearch.dsl.sql.parser.sql.sort;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.google.common.collect.ImmutableList;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.model.ElasticSqlQueryField;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.elasticsearch.dsl.sql.parser.sql.BoolExpressionParser;
import com.iamazy.elasticsearch.dsl.sql.parser.sql.QueryFieldParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.sort.*;


import java.util.List;
import java.util.Map;

/**
 * nested sort(nestedField, sortMode, missingValue, defaultValue, filterExpression)
 * <p>
 * order by nested_sort($repaymentRecords.principal, 'min', 0, repaymentRecords.status='DONE') asc
 */
public class NestedSortMethodParser extends AbstractMethodSortParser {

    public static final List<String> NESTED_SORT_METHOD = ImmutableList.of("nested_sort", "nestedSort");

    @Override
    public List<String> defineMethodNames() {
        return NESTED_SORT_METHOD;
    }

    @Override
    public void checkMethodInvocation(MethodInvocation nestedSortMethodInvocation) throws ElasticSql2DslException {
        if (!isMatchMethodInvocation(nestedSortMethodInvocation)) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] No suck sort method[%s]", nestedSortMethodInvocation.getMethodName()));
        }

        if (nestedSortMethodInvocation.getParameterCount() > 4) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There is no %s args method named nested_sort",
                            nestedSortMethodInvocation.getParameterCount()));
        }

        SQLExpr sortModArg = nestedSortMethodInvocation.getParameter(1);
        if (!(sortModArg instanceof SQLCharExpr)) {
            throw new ElasticSql2DslException("[syntax error] The second arg of nested_sort method should be string");
        }

        String sortModeText = ((SQLCharExpr) sortModArg).getText();
        SortMode.fromString(sortModeText);
    }

    @Override
    protected SortBuilder parseMethodSortBuilder(MethodInvocation invocation, SortOrder order, Map<String, Object> extraParamMap) throws ElasticSql2DslException {
        String sortMode = invocation.getParameterAsString(1);
        Object defaultSortVal = invocation.getParameterAsObject(2);

        boolean hasFilterExpr = invocation.getParameterCount() == 4;

        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField sortField = queryFieldParser.parseConditionQueryField(invocation.getParameter(0), invocation.getQueryAs());

        return ParseSortBuilderHelper.parseBasedOnFieldSortBuilder(sortField, nestedFieldName -> {
            BoolQueryBuilder filter = null;
            if (hasFilterExpr) {
                SQLExpr filterExpr = invocation.getParameter(3);

                BoolExpressionParser boolExpressionParser = new BoolExpressionParser();

                String queryAs = invocation.getQueryAs();

                filter = boolExpressionParser.parseBoolQueryExpr(filterExpr, queryAs);
            }

            if(sortField.getNestedDocContextPath().size()==1) {
                return SortBuilders.fieldSort(nestedFieldName)
                        .missing(defaultSortVal).sortMode(SortMode.fromString(sortMode))
                        .setNestedSort(new NestedSortBuilder(sortField.getNestedDocContextPath().get(0)).setFilter(hasFilterExpr ? filter : null))
                        .order(order);
            }else if(sortField.getNestedDocContextPath().size()==2){
                return SortBuilders.fieldSort(nestedFieldName)
                        .missing(defaultSortVal).sortMode(SortMode.fromString(sortMode))
                        .setNestedSort(new NestedSortBuilder(sortField.getNestedDocContextPath().get(0)).setNestedSort(new NestedSortBuilder(sortField.getNestedDocContextPath().get(1)).setFilter(hasFilterExpr ? filter : null)))
                        .order(order);
            }else{
                throw new ElasticSql2DslException("[syntax error] can not support sql for 3 more nested sort aggregation");
            }
        });
    }
}
