package com.iamazy.elasticsearch.dsl.sql.parser.aggs.search;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.iamazy.elasticsearch.dsl.sql.enums.QueryFieldType;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import com.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import com.iamazy.elasticsearch.dsl.sql.model.AggregationQuery;
import com.iamazy.elasticsearch.dsl.sql.model.ElasticSqlQueryField;
import com.iamazy.elasticsearch.dsl.sql.model.RangeSegment;
import com.iamazy.elasticsearch.dsl.sql.model.SqlArgs;
import com.iamazy.elasticsearch.dsl.sql.parser.aggs.AbstractGroupByMethodAggregationParser;
import com.iamazy.elasticsearch.dsl.sql.parser.aggs.GroupByAggregationParser;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.elasticsearch.dsl.sql.parser.sql.QueryFieldParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.AbstractRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.joda.time.DateTime;

import java.util.List;

/**
 * @author iamazy
 * @date 2019/3/7
 * @descrition
 **/
public class RangeAggAggregationParser extends AbstractGroupByMethodAggregationParser {

    private static final List<String> AGG_RANGE_METHOD = ImmutableList.of("range", "range_agg");

    private static DateTime getDateRangeVal(String date) {
        return DateTime.parse(date);
    }



    @Override
    public AggregationQuery parseAggregationMethod(MethodInvocation invocation) throws ElasticSql2DslException {
        List<RangeSegment> rangeSegments = parseRangeSegments(invocation.getMethodInvokeExpr(), invocation.getSqlArgs());
        SQLExpr rangeFieldExpr = invocation.getMethodInvokeExpr().getParameters().get(0);

        return new AggregationQuery(parseRangeAggregation(invocation.getQueryAs(), rangeFieldExpr, rangeSegments));
    }

    @Override
    public List<String> defineMethodNames() {
        return AGG_RANGE_METHOD;
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }


    private AggregationBuilder parseRangeAggregation(String queryAs, SQLExpr rangeFieldExpr, List<RangeSegment> rangeSegments) {

        QueryFieldParser queryFieldParser = new QueryFieldParser();

        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(rangeFieldExpr, queryAs);
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            return createRangeBuilder(queryField.getQueryFieldFullName(), rangeSegments);
        }
        else if(queryField.getQueryFieldType()==QueryFieldType.NestedDocField){
            if(queryField.getNestedDocContextPath().size()==1) {
                return AggregationBuilders.nested(queryField.getQueryFieldFullName() + "_range", queryField.getNestedDocContextPath().get(0)).subAggregation(createRangeBuilder(queryField.getQueryFieldFullName(), rangeSegments));
            }else if(queryField.getNestedDocContextPath().size()==2){
                return AggregationBuilders.nested(queryField.getNestedDocContextPath().get(0)+"_nested", queryField.getNestedDocContextPath().get(0)).subAggregation(AggregationBuilders.nested(queryField.getNestedDocContextPath().get(1)+"_nested",queryField.getNestedDocContextPath().get(1)).subAggregation(createRangeBuilder(queryField.getQueryFieldFullName(), rangeSegments)));
            }else{
                throw new ElasticSql2DslException("[syntax error] can not support sql for 3 more nested aggregation");
            }
        }
        else{
            throw new ElasticSql2DslException(String.format("[syntax error] can not support range aggregation for field type[%s]", queryField.getQueryFieldType()));
        }


    }

    private List<RangeSegment> parseRangeSegments(SQLMethodInvokeExpr rangeMethodExpr, SqlArgs args) {
        List<RangeSegment> rangeSegmentList = Lists.newArrayList();
        for (int pIdx = 1; pIdx < rangeMethodExpr.getParameters().size(); pIdx++) {
            SQLMethodInvokeExpr segMethodExpr = (SQLMethodInvokeExpr) rangeMethodExpr.getParameters().get(pIdx);

            ElasticSqlMethodInvokeHelper.checkRangeItemAggMethod(segMethodExpr);

            Object from = ElasticSqlArgConverter.convertSqlArg(segMethodExpr.getParameters().get(0), args, true);
            Object to = ElasticSqlArgConverter.convertSqlArg(segMethodExpr.getParameters().get(1), args, true);

            rangeSegmentList.add(new RangeSegment(from, to,
                    from instanceof Number ? RangeSegment.SegmentType.Numeric : RangeSegment.SegmentType.Date));
        }
        return rangeSegmentList;
    }

    private AbstractRangeBuilder createRangeBuilder(String rangeFieldName, List<RangeSegment> rangeSegments) {
        AbstractRangeBuilder rangeBuilder = null;
        RangeSegment.SegmentType segType = rangeSegments.get(0).getSegmentType();

        if (segType == RangeSegment.SegmentType.Numeric) {
            RangeAggregationBuilder numericRangeBuilder = AggregationBuilders.range(GroupByAggregationParser.AGG_BUCKET_KEY_PREFIX + rangeFieldName+"_range").field(rangeFieldName);
            for (RangeSegment segment : rangeSegments) {
                String key = String.format("%s-%s", segment.getFrom().toString(), segment.getTo().toString());
                numericRangeBuilder.addRange(key, Double.valueOf(segment.getFrom().toString()), Double.valueOf(segment.getTo().toString()));
            }
            rangeBuilder = numericRangeBuilder;
        }

        if (segType == RangeSegment.SegmentType.Date) {

            DateRangeAggregationBuilder dateRangeBuilder = AggregationBuilders.dateRange(GroupByAggregationParser.AGG_BUCKET_KEY_PREFIX + rangeFieldName+"_range").field(rangeFieldName);
            for (RangeSegment segment : rangeSegments) {

                DateTime fromDate = getDateRangeVal(segment.getFrom().toString());
                DateTime toDate = getDateRangeVal(segment.getTo().toString());

                String key = String.format("[%s]-[%s]", formatDateRangeAggKey(fromDate), formatDateRangeAggKey(toDate));
                dateRangeBuilder.addRange(key, fromDate, toDate);
            }
            rangeBuilder = dateRangeBuilder;
        }
        return rangeBuilder;
    }

    private String formatDateRangeAggKey(DateTime date) {
        final String dateRangeKeyPattern = "yyyy-MM-dd HH:mm:ss";
        return new DateTime(date).toString(dateRangeKeyPattern);
    }
}
