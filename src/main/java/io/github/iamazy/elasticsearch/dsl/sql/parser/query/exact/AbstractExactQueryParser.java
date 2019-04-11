package io.github.iamazy.elasticsearch.dsl.sql.parser.query.exact;

import com.alibaba.druid.sql.ast.SQLExpr;
import io.github.iamazy.elasticsearch.dsl.cons.CoreConstants;
import io.github.iamazy.elasticsearch.dsl.sql.enums.QueryFieldType;
import io.github.iamazy.elasticsearch.dsl.sql.enums.SqlConditionOperator;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlQueryField;
import io.github.iamazy.elasticsearch.dsl.sql.parser.sql.QueryFieldParser;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;

public abstract class AbstractExactQueryParser {


    AtomicQuery parseCondition(SQLExpr queryFieldExpr, SqlConditionOperator operator, Object[] params, String queryAs, IConditionExactQueryBuilder queryBuilder) {

        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(queryFieldExpr, queryAs);
        if(CoreConstants.HIGHLIGHTER.equalsIgnoreCase(queryField.getQueryFieldFullName())){
            throw new ElasticSql2DslException("[syntax error] the query field can not equals to 'h#'");
        }
        AtomicQuery atomQuery = null;
        String field=null;
        boolean highlighter=false;
        if(queryField.getQueryFieldFullName().startsWith(CoreConstants.HIGHLIGHTER)){
            field=queryField.getQueryFieldFullName().substring(CoreConstants.HIGHLIGHTER.length());
            queryField.setQueryFieldFullName(field);
            highlighter=true;
        }
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            QueryBuilder originalQuery = queryBuilder.buildQuery(queryField.getQueryFieldFullName(), operator, params);
            atomQuery = new AtomicQuery(originalQuery);
        }
        else if (queryField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            QueryBuilder originalQuery = queryBuilder.buildQuery(queryField.getQueryFieldFullName(), operator, params);
            atomQuery = new AtomicQuery(originalQuery, queryField.getNestedDocContextPath());
        }

        if (atomQuery == null) {
            throw new ElasticSql2DslException(String.format("[syntax error] where condition field can not support type[%s]", queryField.getQueryFieldType()));
        }

        if(highlighter&& StringUtils.isNotBlank(field)) {
            atomQuery.getHighlighter().add(field);
        }

        return atomQuery;
    }
}
