package com.iamazy.springcloud.elasticsearch.dsl.sql.helper;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.SqlArgs;

import java.util.List;

/**
 * Copyright 2018-2019 iamazy Logic Ltd
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/
public class ElasticSqlArgConverter {
    private ElasticSqlArgConverter(){}

    public static Object[] convertSqlArgs(List<SQLExpr> exprList, SqlArgs sqlArgs) {
        Object[] values = new Object[exprList.size()];
        for (int idx = 0; idx < exprList.size(); idx++) {
            values[idx] = convertSqlArg(exprList.get(idx), sqlArgs, true);
        }
        return values;
    }

    public static Object convertSqlArg(SQLExpr expr, SqlArgs sqlArgs) {
        return convertSqlArg(expr, sqlArgs, true);
    }

    public static Object convertSqlArg(SQLExpr expr, SqlArgs sqlArgs, boolean recognizeDateArg) {
        if (expr instanceof SQLVariantRefExpr) {
            SQLVariantRefExpr varRefExpr = (SQLVariantRefExpr) expr;
            //parse date
            if (recognizeDateArg && ElasticSqlDateParseHelper.isDateArgObjectValue(sqlArgs.get(varRefExpr.getIndex()))) {
                return ElasticSqlDateParseHelper.formatDefaultEsDateObjectValue(sqlArgs.get(varRefExpr.getIndex()));
            }
            return sqlArgs.get(varRefExpr.getIndex());
        }

        if (expr instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) expr).getNumber().longValue();
        }
        if (expr instanceof SQLNumberExpr) {
            return ((SQLNumberExpr) expr).getNumber().doubleValue();
        }

        if (expr instanceof SQLCharExpr) {
            Object textObject = ((SQLCharExpr) expr).getValue();

            if (recognizeDateArg && (textObject instanceof String) && ElasticSqlDateParseHelper.isDateArgStringValue((String) textObject)) {
                return ElasticSqlDateParseHelper.formatDefaultEsDateStringValue((String) textObject);
            }
            return textObject;
        }

        if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodExpr = (SQLMethodInvokeExpr) expr;

            if (ElasticSqlDateParseHelper.isDateMethod(methodExpr)) {
                ElasticSqlMethodInvokeHelper.checkDateMethod(methodExpr);
                String patternArg = (String) ElasticSqlArgConverter.convertSqlArg(methodExpr.getParameters().get(0), sqlArgs, false);
                String timeValArg = (String) ElasticSqlArgConverter.convertSqlArg(methodExpr.getParameters().get(1), sqlArgs, false);
                return ElasticSqlDateParseHelper.formatDefaultEsDate(patternArg, timeValArg);
            }
        }

        throw new ElasticSql2DslException(
                String.format("[syntax error] Arg type[%s] can not support.",
                        expr.toString()));
    }
}




























