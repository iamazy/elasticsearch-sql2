package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.score;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.github.iamazy.elasticsearch.dsl.cons.ElasticConstants;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import io.github.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodQueryParser;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.expr.AbstractParameterizedMethodExpression;
import io.github.iamazy.elasticsearch.dsl.sql.parser.sql.BoolExpressionParser;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author iamazy
 * @date 2019/4/10
 * @descrition
 **/
public class FunctionScoreQueryParser extends AbstractParameterizedMethodExpression implements MethodQueryParser {

    private static List<String> FUNCTION_SCORE_METHOD = ImmutableList.of("function_score");

    private static final List<String> SCRIPT_SCORE_METHOD = ImmutableList.of("script_score");

    private static final List<String> RANDOM_SCORE_METHOD = ImmutableList.of("random_score");

    private static final List<String> FIELD_VALUE_FACTOR_METHOD = ImmutableList.of("field_value_factor");

    private static final List<String> WEIGHT_METHODS = ImmutableList.of("weight");


    private boolean checkScriptScoreMethod(SQLMethodInvokeExpr invokeExpr) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(SCRIPT_SCORE_METHOD, invokeExpr.getMethodName());
    }

    private boolean checkRandomScoreMethod(SQLMethodInvokeExpr invokeExpr) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(RANDOM_SCORE_METHOD, invokeExpr.getMethodName());
    }

    private boolean checkWeightMethod(SQLMethodInvokeExpr methodInvokeExpr){
        return ElasticSqlMethodInvokeHelper.isMethodOf(WEIGHT_METHODS, methodInvokeExpr.getMethodName());
    }

    private boolean checkFieldValueFactorMethod(SQLMethodInvokeExpr invokeExpr) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(FIELD_VALUE_FACTOR_METHOD, invokeExpr.getMethodName());
    }

    private FunctionScoreQueryBuilder.FilterFunctionBuilder parseScriptScoreMethod(SQLMethodInvokeExpr methodInvokeExpr) {
        ScriptScoreFunctionBuilder scriptScoreFunctionBuilder;
        switch (methodInvokeExpr.getParameters().size()) {
            case 3: {
                if (!(methodInvokeExpr.getParameters().get(0) instanceof SQLBinaryOpExpr && methodInvokeExpr.getParameters().get(1) instanceof SQLCharExpr && methodInvokeExpr.getParameters().get(2) instanceof SQLCharExpr)) {
                    throw new ElasticSql2DslException("Failed to parse query method script_score!");
                }
                SQLBinaryOpExpr queryExpr = (SQLBinaryOpExpr) methodInvokeExpr.getParameters().get(0);
                BoolExpressionParser boolExpressionParser = new BoolExpressionParser();
                BoolQueryBuilder boolQueryBuilder = boolExpressionParser.parseBoolQueryExpr(queryExpr, null);
                SQLCharExpr sourceExpr = (SQLCharExpr) methodInvokeExpr.getParameters().get(1);
                SQLCharExpr paramsExpr = (SQLCharExpr) methodInvokeExpr.getParameters().get(2);
                scriptScoreFunctionBuilder = ScoreFunctionBuilders.scriptFunction(parseScript(sourceExpr, paramsExpr));
                return new FunctionScoreQueryBuilder.FilterFunctionBuilder(boolQueryBuilder, scriptScoreFunctionBuilder);
            }
            case 2: {
                if (!(methodInvokeExpr.getParameters().get(0) instanceof SQLCharExpr && methodInvokeExpr.getParameters().get(1) instanceof SQLCharExpr)) {
                    throw new ElasticSql2DslException("Failed to parse query method script_score!");
                }
                SQLCharExpr sourceExpr = (SQLCharExpr) methodInvokeExpr.getParameters().get(0);
                SQLCharExpr paramsExpr = (SQLCharExpr) methodInvokeExpr.getParameters().get(1);
                scriptScoreFunctionBuilder = ScoreFunctionBuilders.scriptFunction(parseScript(sourceExpr, paramsExpr));
                return new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptScoreFunctionBuilder);
            }
            case 1:
            default: {
                if (!(methodInvokeExpr.getParameters().get(0) instanceof SQLCharExpr)) {
                    throw new ElasticSql2DslException("Failed to parse query method script_score!");
                }
                SQLCharExpr sourceExpr = (SQLCharExpr) methodInvokeExpr.getParameters().get(0);
                Script script = new Script(ScriptType.INLINE, "painless", sourceExpr.getText(), Collections.emptyMap());
                scriptScoreFunctionBuilder = ScoreFunctionBuilders.scriptFunction(script);
                return new FunctionScoreQueryBuilder.FilterFunctionBuilder(scriptScoreFunctionBuilder);
            }
        }
    }

    private Script parseScript(SQLCharExpr sourceExpr, SQLCharExpr paramsExpr) {
        if (StringUtils.isBlank(paramsExpr.getText())) {
            return new Script(ScriptType.INLINE, "painless", sourceExpr.getText(), Collections.emptyMap());
        } else {
            return new Script(ScriptType.INLINE, "painless", sourceExpr.getText(), generateParamsMap(paramsExpr));
        }
    }

    private FunctionScoreQueryBuilder.FilterFunctionBuilder parseRandomScoreMethod(SQLMethodInvokeExpr methodInvokeExpr) {
        RandomScoreFunctionBuilder randomScoreFunctionBuilder = ScoreFunctionBuilders.randomFunction();
        if (methodInvokeExpr.getParameters().size() > 3 || methodInvokeExpr.getParameters().size() == 0) {
            throw new ElasticSql2DslException("Failed to parse query method random_score!");
        }
        if (methodInvokeExpr.getParameters().size() == 1 && methodInvokeExpr.getParameters().get(0) instanceof SQLCharExpr) {
            randomScoreFunctionBuilder.setField(((SQLCharExpr) methodInvokeExpr.getParameters().get(0)).getText());
        } else if (methodInvokeExpr.getParameters().size() == 1 && methodInvokeExpr.getParameters().get(0) instanceof SQLNumberExpr) {
            throw new ElasticSql2DslException("Failed to parse query method random_score,require that a [field] parameter is provided when a [seed] is set");
        } else if (methodInvokeExpr.getParameters().get(0) instanceof SQLIntegerExpr && methodInvokeExpr.getParameters().get(1) instanceof SQLCharExpr) {
            randomScoreFunctionBuilder.seed(((SQLIntegerExpr) methodInvokeExpr.getParameters().get(0)).getNumber().longValue());
            randomScoreFunctionBuilder.setField(((SQLCharExpr) methodInvokeExpr.getParameters().get(1)).getText());
        } else if (methodInvokeExpr.getParameters().get(0) instanceof SQLBinaryOpExpr && methodInvokeExpr.getParameters().get(2) instanceof SQLCharExpr && methodInvokeExpr.getParameters().get(1) instanceof SQLIntegerExpr) {
            BoolExpressionParser boolExpressionParser = new BoolExpressionParser();
            BoolQueryBuilder boolQueryBuilder = boolExpressionParser.parseBoolQueryExpr(methodInvokeExpr.getParameters().get(0), null);
            randomScoreFunctionBuilder.seed(((SQLIntegerExpr) methodInvokeExpr.getParameters().get(1)).getNumber().longValue());
            randomScoreFunctionBuilder.setField(((SQLCharExpr) methodInvokeExpr.getParameters().get(2)).getText());
            return new FunctionScoreQueryBuilder.FilterFunctionBuilder(boolQueryBuilder, randomScoreFunctionBuilder);
        }
        return new FunctionScoreQueryBuilder.FilterFunctionBuilder(randomScoreFunctionBuilder);
    }

    private FunctionScoreQueryBuilder.FilterFunctionBuilder parseFieldValueFactorMethod(SQLMethodInvokeExpr methodInvokeExpr) {
        if(methodInvokeExpr.getParameters().size()==2 && methodInvokeExpr.getParameters().get(0) instanceof SQLBinaryOpExpr && methodInvokeExpr.getParameters().get(1) instanceof SQLCharExpr){
            SQLBinaryOpExpr binaryOpExpr = (SQLBinaryOpExpr) methodInvokeExpr.getParameters().get(0);
            SQLCharExpr charExpr = (SQLCharExpr) methodInvokeExpr.getParameters().get(1);
            FieldValueFactorFunctionBuilder fieldValueFactorFunctionBuilder = setFieldValueFactorParams(charExpr);
            BoolExpressionParser boolExpressionParser=new BoolExpressionParser();
            BoolQueryBuilder boolQueryBuilder = boolExpressionParser.parseBoolQueryExpr(binaryOpExpr, null);
            return new FunctionScoreQueryBuilder.FilterFunctionBuilder(boolQueryBuilder,fieldValueFactorFunctionBuilder);
        }
        else if (methodInvokeExpr.getParameters().size() == 1 && methodInvokeExpr.getParameters().get(0) instanceof SQLCharExpr) {
            SQLCharExpr expr = (SQLCharExpr) methodInvokeExpr.getParameters().get(0);
            FieldValueFactorFunctionBuilder fieldValueFactorFunctionBuilder = setFieldValueFactorParams(expr);
            return new FunctionScoreQueryBuilder.FilterFunctionBuilder(fieldValueFactorFunctionBuilder);
        }else {
            throw new ElasticSql2DslException("Failed to parse query method field_value_factor!");
        }
    }

    private FieldValueFactorFunctionBuilder setFieldValueFactorParams(SQLCharExpr expr){
        Map<String, Object> paramsMap = generateParamsMap(expr);
        if (!paramsMap.containsKey(ElasticConstants.FILED)) {
            throw new ElasticSql2DslException("Failed to parse query method field_value_factor without [field] parameter");
        }
        FieldValueFactorFunctionBuilder fieldValueFactorFunctionBuilder = ScoreFunctionBuilders.fieldValueFactorFunction(paramsMap.get(ElasticConstants.FILED).toString());
        if (paramsMap.containsKey(ElasticConstants.FACTOR)) {
            fieldValueFactorFunctionBuilder.factor(Float.valueOf(paramsMap.get(ElasticConstants.FACTOR).toString()));
        }
        if (paramsMap.containsKey(ElasticConstants.MODIFIER)) {
            fieldValueFactorFunctionBuilder.modifier(parseModifier(paramsMap.get(ElasticConstants.MODIFIER).toString()));
        }
        if(paramsMap.containsKey(ElasticConstants.MISSING)){
            fieldValueFactorFunctionBuilder.missing(Double.valueOf(paramsMap.get(ElasticConstants.MISSING).toString()));
        }
        return fieldValueFactorFunctionBuilder;
    }

    private FunctionScoreQueryBuilder.FilterFunctionBuilder parseWeightMethod(SQLMethodInvokeExpr methodInvokeExpr){
        if(methodInvokeExpr.getParameters().size()==1 && methodInvokeExpr.getParameters().get(0) instanceof SQLIntegerExpr){
            SQLIntegerExpr expr=(SQLIntegerExpr)methodInvokeExpr.getParameters().get(0);
            WeightBuilder weightBuilder=ScoreFunctionBuilders.weightFactorFunction(expr.getNumber().floatValue());
            return new FunctionScoreQueryBuilder.FilterFunctionBuilder(weightBuilder);
        }
        else if(methodInvokeExpr.getParameters().size()==2 && methodInvokeExpr.getParameters().get(0) instanceof SQLBinaryOpExpr && methodInvokeExpr.getParameters().get(1) instanceof SQLIntegerExpr){
            SQLIntegerExpr expr=(SQLIntegerExpr)methodInvokeExpr.getParameters().get(1);
            WeightBuilder weightBuilder=ScoreFunctionBuilders.weightFactorFunction(expr.getNumber().floatValue());
            BoolExpressionParser boolExpressionParser=new BoolExpressionParser();
            BoolQueryBuilder boolQueryBuilder = boolExpressionParser.parseBoolQueryExpr(methodInvokeExpr.getParameters().get(0), null);
            return new FunctionScoreQueryBuilder.FilterFunctionBuilder(boolQueryBuilder,weightBuilder);
        }else{
            throw new ElasticSql2DslException("Failed to parse query method weight!");
        }
    }


    @Override
    public AtomicQuery parseMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException {
        BoolExpressionParser boolExpressionParser = new BoolExpressionParser();
        FunctionScoreQueryBuilder functionScoreQueryBuilder = null;
        List<FunctionScoreQueryBuilder.FilterFunctionBuilder> filterFunctionBuilders = new ArrayList<>(0);
        if (invocation.getParameterCount() == 1) {
            BoolQueryBuilder boolQueryBuilder = boolExpressionParser.parseBoolQueryExpr(invocation.getFirstParameter(), invocation.getQueryAs());
            functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(boolQueryBuilder);

        } else if (invocation.getParameterCount() >= 2) {

            BoolQueryBuilder boolQueryBuilder=null;
            if(invocation.getFirstParameter() instanceof SQLBinaryOpExpr) {
                boolQueryBuilder = boolExpressionParser.parseBoolQueryExpr(invocation.getFirstParameter(), invocation.getQueryAs());
            }

            for (SQLExpr expr : invocation.getParameters()) {
                if (expr instanceof SQLMethodInvokeExpr) {
                    SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) expr;
                    if (checkScriptScoreMethod(methodInvokeExpr)) {
                        filterFunctionBuilders.add(parseScriptScoreMethod(methodInvokeExpr));
                    }
                    if (checkRandomScoreMethod(methodInvokeExpr)) {
                        filterFunctionBuilders.add(parseRandomScoreMethod(methodInvokeExpr));
                    }
                    if(checkFieldValueFactorMethod(methodInvokeExpr)){
                        filterFunctionBuilders.add(parseFieldValueFactorMethod(methodInvokeExpr));
                    }
                    if(checkWeightMethod(methodInvokeExpr)){
                        filterFunctionBuilders.add(parseWeightMethod(methodInvokeExpr));
                    }
                }
            }
            if(boolQueryBuilder!=null) {
                functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(boolQueryBuilder, filterFunctionBuilders.toArray(new FunctionScoreQueryBuilder.FilterFunctionBuilder[0]));
            }else{
                functionScoreQueryBuilder = QueryBuilders.functionScoreQuery(filterFunctionBuilders.toArray(new FunctionScoreQueryBuilder.FilterFunctionBuilder[0]));
            }
            if(invocation.getParameter(invocation.getParameterCount()-1) instanceof SQLCharExpr){
                setExtraMatchQueryParam(functionScoreQueryBuilder, generateParameterMap(invocation));
            }
        }
        AtomicQuery atomicQuery = new AtomicQuery(functionScoreQueryBuilder);
        atomicQuery.getHighlighter().addAll(boolExpressionParser.getHighlighter());
        return atomicQuery;
    }

    @Override
    public List<String> defineMethodNames() {
        return FUNCTION_SCORE_METHOD;
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        SQLExpr sqlExpr = invocation.getParameter(invocation.getParameterCount() - 1);
        if (sqlExpr instanceof SQLCharExpr) {
            return ((SQLCharExpr) sqlExpr).getText();
        } else {
            return StringUtils.EMPTY;
        }
    }


    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        //因为function_score里面可以添加无数的function,所以这里无法对function_score函数进行验证
    }

    private FunctionScoreQuery.ScoreMode parseScoreMode(String mode) {
        switch (mode.toLowerCase()) {
            case "min": {
                return FunctionScoreQuery.ScoreMode.MIN;
            }
            case "max": {
                return FunctionScoreQuery.ScoreMode.MAX;
            }
            case "avg": {
                return FunctionScoreQuery.ScoreMode.AVG;
            }
            case "sum": {
                return FunctionScoreQuery.ScoreMode.SUM;
            }
            case "first": {
                return FunctionScoreQuery.ScoreMode.FIRST;
            }
            case "multi":
            case "multiply":
            default: {
                return FunctionScoreQuery.ScoreMode.MULTIPLY;
            }
        }
    }

    private FieldValueFactorFunction.Modifier parseModifier(String modifier) {
        switch (modifier.toLowerCase()) {
            case "log": {
                return FieldValueFactorFunction.Modifier.LOG;
            }
            case "log1p": {
                return FieldValueFactorFunction.Modifier.LOG1P;
            }
            case "log2p": {
                return FieldValueFactorFunction.Modifier.LOG2P;
            }
            case "ln": {
                return FieldValueFactorFunction.Modifier.LN;
            }
            case "ln1p": {
                return FieldValueFactorFunction.Modifier.LN1P;
            }
            case "ln2p": {
                return FieldValueFactorFunction.Modifier.LN2P;
            }
            case "square": {
                return FieldValueFactorFunction.Modifier.SQUARE;
            }
            case "sqrt": {
                return FieldValueFactorFunction.Modifier.SQRT;
            }
            case "reciprocal": {
                return FieldValueFactorFunction.Modifier.RECIPROCAL;
            }
            case "none":
            default: {
                return FieldValueFactorFunction.Modifier.NONE;
            }
        }
    }


    private CombineFunction parseBoostMode(String mode) {
        switch (mode.toLowerCase()) {
            case "min": {
                return CombineFunction.MIN;
            }
            case "max": {
                return CombineFunction.MAX;
            }
            case "avg": {
                return CombineFunction.AVG;
            }
            case "replace": {
                return CombineFunction.REPLACE;
            }
            case "sum": {
                return CombineFunction.SUM;
            }
            case "multi":
            case "multiply":
            default: {
                return CombineFunction.MULTIPLY;
            }
        }
    }

    private void setExtraMatchQueryParam(FunctionScoreQueryBuilder functionScoreQueryBuilder, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey(ElasticConstants.BOOST)) {
            String val = extraParamMap.get(ElasticConstants.BOOST);
            functionScoreQueryBuilder.boost(Float.valueOf(val));
        }
        if (extraParamMap.containsKey(ElasticConstants.SCORE_MODE)) {
            String val = extraParamMap.get(ElasticConstants.SCORE_MODE);
            functionScoreQueryBuilder.scoreMode(parseScoreMode(val));
        }
        if (extraParamMap.containsKey(ElasticConstants.MAX_BOOST)) {
            String val = extraParamMap.get(ElasticConstants.MAX_BOOST);
            functionScoreQueryBuilder.maxBoost(Float.valueOf(val));
        }
        if (extraParamMap.containsKey(ElasticConstants.BOOST_MODE)) {
            String val = extraParamMap.get(ElasticConstants.BOOST_MODE);
            functionScoreQueryBuilder.boostMode(parseBoostMode(val));
        }
        if (extraParamMap.containsKey(ElasticConstants.MIN_SCORE)) {
            String val = extraParamMap.get(ElasticConstants.MIN_SCORE);
            functionScoreQueryBuilder.setMinScore(Float.valueOf(val));
        }
    }

    private Map<String, Object> generateParamsMap(SQLCharExpr expr) {
        Map<String, Object> extraParamMap = Maps.newHashMap();
        for (String paramPair : expr.getText().split(COMMA)) {
            String[] paramPairArr = paramPair.split(COLON);
            if (paramPairArr.length == 2) {
                extraParamMap.put(paramPairArr[0].trim(), paramPairArr[1].trim());
            } else {
                throw new ElasticSql2DslException("Failed to parse query method extra param string!");
            }
        }
        return extraParamMap;
    }

}
