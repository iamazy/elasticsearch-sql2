package io.github.iamazy.elasticsearch.dsl.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.Lists;
import io.github.iamazy.elasticsearch.dsl.cons.CoreConstants;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlQueryField;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlQueryFields;
import io.github.iamazy.elasticsearch.dsl.sql.model.QueryFieldReferenceNode;
import io.github.iamazy.elasticsearch.dsl.sql.model.QueryFieldReferencePath;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class QueryFieldParser {
    private static final String NESTED_DOC_IDF = CoreConstants.DOLLAR;

    private static List<QueryFieldReferenceNode> parseQueryFieldExprToRefPath(SQLExpr queryFieldExpr) {
        List<QueryFieldReferenceNode> referencePathNodes = Lists.newLinkedList();

        if (queryFieldExpr instanceof SQLIdentifierExpr) {
            QueryFieldReferenceNode referenceNode = buildReferenceNode(((SQLIdentifierExpr) queryFieldExpr).getName());
            referencePathNodes.add(referenceNode);
            return referencePathNodes;
        }

        if (queryFieldExpr instanceof SQLPropertyExpr) {
            List<String> queryFieldTextList = Lists.newLinkedList();

            SQLExpr tmpLoopExpr = queryFieldExpr;
            String expr = tmpLoopExpr.toString();
            boolean checkSyntax = !expr.contains(NESTED_DOC_IDF)
                    || StringUtils.countMatches(expr, NESTED_DOC_IDF) == 1
                    || (StringUtils.countMatches(expr, NESTED_DOC_IDF) == 2 && expr.indexOf(NESTED_DOC_IDF) != expr.lastIndexOf(NESTED_DOC_IDF));

            if (!checkSyntax) {
                throw new ElasticSql2DslException("[syntax error] nested doc query can not support this syntax");
            }

            while ((tmpLoopExpr instanceof SQLPropertyExpr)) {
                queryFieldTextList.add(((SQLPropertyExpr) tmpLoopExpr).getName());
                tmpLoopExpr = ((SQLPropertyExpr) tmpLoopExpr).getOwner();
            }

            if (tmpLoopExpr instanceof SQLIdentifierExpr) {
                queryFieldTextList.add(((SQLIdentifierExpr) tmpLoopExpr).getName());
            }

            Collections.reverse(queryFieldTextList);
            for (String strRefNode : queryFieldTextList) {
                if (StringUtils.countMatches(strRefNode, NESTED_DOC_IDF) == 2 && strRefNode.indexOf(NESTED_DOC_IDF) != strRefNode.lastIndexOf(NESTED_DOC_IDF)) {
                    Integer lastIndexOf = strRefNode.lastIndexOf(NESTED_DOC_IDF);
                    String[] split = new String[]{strRefNode.substring(0, lastIndexOf), strRefNode.substring(lastIndexOf + 1)};
                    for (String item : split) {
                        QueryFieldReferenceNode referenceNode = buildReferenceNode(item);
                        referenceNode.setNestedDocReference(true);
                        referencePathNodes.add(referenceNode);
                    }
                } else {
                    QueryFieldReferenceNode referenceNode = buildReferenceNode(strRefNode);
                    referencePathNodes.add(referenceNode);
                }
            }
            return referencePathNodes;
        }
        throw new ElasticSql2DslException(String.format("[syntax error] can not support query field type[%s]", queryFieldExpr.toString()));
    }

    private static QueryFieldReferenceNode buildReferenceNode(String strRefNodeName) {
        QueryFieldReferenceNode referenceNode;
        if (strRefNodeName.contains(NESTED_DOC_IDF)) {
            if (NESTED_DOC_IDF.equals(strRefNodeName)) {
                throw new ElasticSql2DslException("[syntax error] nested doc query field can not be blank");
            }
            if (StringUtils.countMatches(strRefNodeName, NESTED_DOC_IDF) == 1) {
                if (strRefNodeName.startsWith(NESTED_DOC_IDF)) {
                    referenceNode = new QueryFieldReferenceNode(strRefNodeName.substring(1), true);
                } else if (strRefNodeName.startsWith(CoreConstants.HIGHLIGHTER + NESTED_DOC_IDF)) {
                    String[] item = strRefNodeName.split("\\$");
                    referenceNode = new QueryFieldReferenceNode(item[0] + item[1], true);
                } else {
                    referenceNode = new QueryFieldReferenceNode(strRefNodeName.replace(NESTED_DOC_IDF, CoreConstants.DOT), true);
                }
            } else if (StringUtils.countMatches(strRefNodeName, NESTED_DOC_IDF) == 2 && strRefNodeName.indexOf(NESTED_DOC_IDF) != strRefNodeName.lastIndexOf(NESTED_DOC_IDF)) {
                if (strRefNodeName.startsWith(NESTED_DOC_IDF)) {
                    referenceNode = new QueryFieldReferenceNode(strRefNodeName.substring(NESTED_DOC_IDF.length()).replace(NESTED_DOC_IDF, CoreConstants.DOT), true);
                } else if (strRefNodeName.startsWith(CoreConstants.HIGHLIGHTER + NESTED_DOC_IDF)) {
                    throw new ElasticSql2DslException("[syntax error] nested doc query can not support this syntax");
                } else {
                    referenceNode = new QueryFieldReferenceNode(strRefNodeName.replace(NESTED_DOC_IDF, CoreConstants.DOT), true);
                }
            } else {
                throw new ElasticSql2DslException("[syntax error] nested doc query can not support this syntax");
            }
        } else {
            referenceNode = new QueryFieldReferenceNode(strRefNodeName, false);
        }
        return referenceNode;
    }

    ElasticSqlQueryField parseSelectQueryField(SQLExpr queryFieldExpr, String queryAs) {
        if (queryFieldExpr instanceof SQLAllColumnExpr) {
            return ElasticSqlQueryFields.newMatchAllRootDocField();
        }
        QueryFieldReferencePath referencePath = buildQueryFieldRefPath(queryFieldExpr, queryAs);

        StringBuilder fullPathQueryFieldNameBuilder = new StringBuilder();
        for (QueryFieldReferenceNode referenceNode : referencePath.getReferenceNodes()) {
            fullPathQueryFieldNameBuilder.append(referenceNode.getReferenceNodeName());
            fullPathQueryFieldNameBuilder.append(CoreConstants.DOT);
        }
        if (fullPathQueryFieldNameBuilder.length() > 0) {
            fullPathQueryFieldNameBuilder.deleteCharAt(fullPathQueryFieldNameBuilder.length() - 1);
        }

        return ElasticSqlQueryFields.newSqlSelectField(fullPathQueryFieldNameBuilder.toString());
    }

    public ElasticSqlQueryField parseConditionQueryField(SQLExpr queryFieldExpr, String queryAs) {
        QueryFieldReferencePath referencePath = buildQueryFieldRefPath(queryFieldExpr, queryAs);
        StringBuilder queryFieldPrefixBuilder = new StringBuilder();

        ArrayList<String> longestNestedDocContextPrefix = new ArrayList<>(0);
        String longestInnerDocContextPrefix = StringUtils.EMPTY;

        for (Iterator<QueryFieldReferenceNode> nodeIt = referencePath.getReferenceNodes().iterator(); nodeIt.hasNext(); ) {
            QueryFieldReferenceNode referenceNode = nodeIt.next();
            queryFieldPrefixBuilder.append(referenceNode.getReferenceNodeName());

            if (referenceNode.isNestedDocReference()) {

                String prefix = queryFieldPrefixBuilder.toString();
                if (prefix.startsWith(CoreConstants.HIGHLIGHTER)) {
                    longestNestedDocContextPrefix.add(prefix.substring(CoreConstants.HIGHLIGHTER.length()));
                } else {
                    longestNestedDocContextPrefix.add(prefix);
                }
            }

            if (nodeIt.hasNext()) {
                longestInnerDocContextPrefix = queryFieldPrefixBuilder.toString();
            }

            queryFieldPrefixBuilder.append(CoreConstants.DOT);
        }
        if (queryFieldPrefixBuilder.length() > 0) {
            queryFieldPrefixBuilder.deleteCharAt(queryFieldPrefixBuilder.length() - 1);
        }

        String queryFieldFullRefPath = queryFieldPrefixBuilder.toString();

        //nested doc field
        if (longestNestedDocContextPrefix.size() != 0 && StringUtils.isNotBlank(longestNestedDocContextPrefix.get(0))) {
            if (longestNestedDocContextPrefix.get(0).length() < queryFieldFullRefPath.length()) {
                if (longestNestedDocContextPrefix.size() == 2 && StringUtils.isNotBlank(longestNestedDocContextPrefix.get(1)) && longestNestedDocContextPrefix.get(1).length() < queryFieldFullRefPath.length()) {
                    return ElasticSqlQueryFields.newNestedDocQueryField(longestNestedDocContextPrefix, queryFieldFullRefPath);
                }
                return ElasticSqlQueryFields.newNestedDocQueryField(longestNestedDocContextPrefix, queryFieldFullRefPath);

            }
            throw new ElasticSql2DslException(String.format("[syntax error] nested doc field[%s] parse error!", queryFieldFullRefPath));
        }

        //root doc field
        if (referencePath.getReferenceNodes().size() == 1) {
            if (StringUtils.isNotBlank(queryAs) && queryAs.equalsIgnoreCase(queryFieldFullRefPath)) {
                throw new ElasticSql2DslException(String.format("[syntax error] ambiguous query field[%s], queryAs[%s]", queryFieldFullRefPath, queryAs));
            }
            return ElasticSqlQueryFields.newRootDocQueryField(queryFieldFullRefPath);
        }

        if (longestInnerDocContextPrefix.length() < queryFieldFullRefPath.length()) {
            String innerDocFieldName = queryFieldFullRefPath.substring(longestInnerDocContextPrefix.length() + 1);
            return ElasticSqlQueryFields.newInnerDocQueryField(longestInnerDocContextPrefix, innerDocFieldName);
        }
        throw new ElasticSql2DslException(String.format("[syntax error] query field[%s] parse error!", queryFieldFullRefPath));
    }

    private QueryFieldReferencePath buildQueryFieldRefPath(SQLExpr queryFieldExpr, String queryAs) {
        QueryFieldReferencePath referencePath = new QueryFieldReferencePath();

        List<QueryFieldReferenceNode> referenceNodeList = parseQueryFieldExprToRefPath(queryFieldExpr);
        if (CollectionUtils.isEmpty(referenceNodeList)) {
            throw new ElasticSql2DslException("[parse_query_field] referenceNodes is empty!");
        }
        QueryFieldReferenceNode firstRefNode = referenceNodeList.get(0);

        if (referenceNodeList.size() == 1) {
            referencePath.addReferenceNode(firstRefNode);
            return referencePath;
        }

        if (StringUtils.isNotBlank(queryAs) && !firstRefNode.isNestedDocReference() && queryAs.equalsIgnoreCase(firstRefNode.getReferenceNodeName())) {
            referenceNodeList.remove(0);
        }

        for (QueryFieldReferenceNode referenceNode : referenceNodeList) {
            referencePath.addReferenceNode(referenceNode);
        }

        return referencePath;
    }
}
