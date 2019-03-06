package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.google.common.collect.Lists;
import com.iamazy.springcloud.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.QueryFieldType;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import com.iamazy.springcloud.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticDslContext;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticSqlQueryField;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.RangeSegment;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.SqlArgs;
import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.range.AbstractRangeBuilder;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.joda.time.DateTime;

import java.util.List;


/**
 * @author iamazy
 */
public class QueryGroupByParser implements QueryParser {

    private static final Integer MAX_GROUP_BY_SIZE = 10000;

    private static final String AGG_BUCKET_KEY_PREFIX = "";


    public QueryGroupByParser() { }

    public static DateTime getDateRangeVal(String date) {
        return DateTime.parse(date);
    }

    @Override
    public void parse(ElasticDslContext dslContext) {

        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) ((SQLQueryExpr)dslContext.getSqlObject()).getSubQuery().getQuery();
        SQLSelectGroupByClause sqlGroupBy = queryBlock.getGroupBy();
        if (sqlGroupBy != null && CollectionUtils.isNotEmpty(sqlGroupBy.getItems())) {
            String queryAs = dslContext.getParseResult().getQueryAs();

            List<AggregationBuilder> aggregationList = Lists.newArrayList();
            for (SQLExpr groupByItem : sqlGroupBy.getItems()) {
                if (!(groupByItem instanceof SQLMethodInvokeExpr)) {
                    throw new ElasticSql2DslException("[syntax error] group by item must be an agg method call");
                }
                SQLMethodInvokeExpr aggMethodExpr = (SQLMethodInvokeExpr) groupByItem;

                //Terms Aggregation
                if (ElasticSqlMethodInvokeHelper.isMethodOf(ElasticSqlMethodInvokeHelper.AGG_TERMS_METHOD, aggMethodExpr.getMethodName())) {
                    ElasticSqlMethodInvokeHelper.checkTermsAggMethod(aggMethodExpr);

                    SQLExpr termsFieldExpr = aggMethodExpr.getParameters().get(0);
                    SQLExpr shardSizeExpr = null;
                    if (aggMethodExpr.getParameters().size() == 2) {
                        shardSizeExpr = aggMethodExpr.getParameters().get(1);
                    }
                    AggregationBuilder termsBuilder = parseTermsAggregation(queryAs, dslContext.getSqlArgs(), termsFieldExpr, shardSizeExpr);
                    aggregationList.add(termsBuilder);
                }

                //Histogram Aggregation
                if(ElasticSqlMethodInvokeHelper.isMethodOf(ElasticSqlMethodInvokeHelper.AGG_DATE_HISTOGRAM_METHOD,aggMethodExpr.getMethodName())){
                    ElasticSqlMethodInvokeHelper.checkDateHistogramAggMethod(aggMethodExpr);
                    SQLExpr dateHistogram=aggMethodExpr.getParameters().get(0);
                    //TODO
                }

                //Cardinality Aggregation
                if(ElasticSqlMethodInvokeHelper.isMethodOf(ElasticSqlMethodInvokeHelper.AGG_CARDINALITY_METHOD,aggMethodExpr.getMethodName())){
                    ElasticSqlMethodInvokeHelper.checkCardinalityAggMethod(aggMethodExpr);
                    SQLExpr cardinalityFieldExpr=aggMethodExpr.getParameters().get(0);
                    SQLExpr precisionThreshold=null;
                    if(aggMethodExpr.getParameters().size()==2){
                        precisionThreshold=aggMethodExpr.getParameters().get(1);
                    }
                    AggregationBuilder cardinalityBuilder=parseCardinalityAggregation(queryAs,dslContext.getSqlArgs(),cardinalityFieldExpr,precisionThreshold);
                    aggregationList.add(cardinalityBuilder);
                }

                //TopHit Aggregation
                if(ElasticSqlMethodInvokeHelper.isMethodOf(ElasticSqlMethodInvokeHelper.AGG_TOPHITS_METHOD,aggMethodExpr.getMethodName())){
                    ElasticSqlMethodInvokeHelper.checkTopHitsAggMethod(aggMethodExpr);
                    AggregationBuilder topHitsBuilder = parseTopHitsAggregation( aggMethodExpr);
                    aggregationList.add(topHitsBuilder);
                }


                //Range Aggregation
                if (ElasticSqlMethodInvokeHelper.isMethodOf(ElasticSqlMethodInvokeHelper.AGG_RANGE_METHOD, aggMethodExpr.getMethodName())) {
                    ElasticSqlMethodInvokeHelper.checkRangeAggMethod(aggMethodExpr);

                    List<RangeSegment> rangeSegments = parseRangeSegments(aggMethodExpr, dslContext.getSqlArgs());
                    SQLExpr rangeFieldExpr = aggMethodExpr.getParameters().get(0);

                    AggregationBuilder rangeBuilder = parseRangeAggregation(queryAs, rangeFieldExpr, rangeSegments);
                    aggregationList.add(rangeBuilder);
                }
            }
            dslContext.getParseResult().setGroupBy(aggregationList);
        }

    }

    private AggregationBuilder parseTermsAggregation(String queryAs, SqlArgs args, SQLExpr termsFieldExpr, SQLExpr shardSizeExpr) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();

        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(termsFieldExpr, queryAs);
        if (queryField.getQueryFieldType() != QueryFieldType.RootDocField && queryField.getQueryFieldType() != QueryFieldType.InnerDocField) {
            throw new ElasticSql2DslException(String.format("[syntax error] can not support terms aggregation for field type[%s]", queryField.getQueryFieldType()));
        }

        if (shardSizeExpr != null) {
            Number termBuckets = (Number) ElasticSqlArgConverter.convertSqlArg(shardSizeExpr, args);
            return createTermsBuilder(queryField.getQueryFieldFullName(), termBuckets.intValue());
        }
        return createTermsBuilder(queryField.getQueryFieldFullName());
    }

    private AggregationBuilder parseCardinalityAggregation(String queryAs,SqlArgs args,SQLExpr cardinalityFieldExpr,SQLExpr precisionThreshold) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();

        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(cardinalityFieldExpr, queryAs);
        if (queryField.getQueryFieldType() != QueryFieldType.RootDocField && queryField.getQueryFieldType() != QueryFieldType.InnerDocField) {
            throw new ElasticSql2DslException(String.format("[syntax error] can not support cardinality aggregation for field type[%s]", queryField.getQueryFieldType()));
        }
        if(precisionThreshold!=null){
            Number threshold = (Number) ElasticSqlArgConverter.convertSqlArg(precisionThreshold, args);
            return createCardinalityBuilder(queryField.getQueryFieldFullName(),threshold.longValue());
        }
        return createCardinalityBuilder(queryField.getQueryFieldFullName());
    }

    private AggregationBuilder parseTopHitsAggregation(SQLMethodInvokeExpr topHitsFieldExpr) {
        if(topHitsFieldExpr.getParameters().size()==1&&topHitsFieldExpr.getParameters().get(0) instanceof SQLIntegerExpr){
            SQLIntegerExpr sizeExpr=(SQLIntegerExpr) topHitsFieldExpr.getParameters().get(0);
            return createTopHitsBuilder(sizeExpr.getNumber().intValue());
        }
        else if(topHitsFieldExpr.getParameters().size()==2&&topHitsFieldExpr.getParameters().get(0) instanceof SQLCharExpr
                &&topHitsFieldExpr.getParameters().get(1) instanceof SQLIntegerExpr){
            SQLCharExpr nameExpr=(SQLCharExpr) topHitsFieldExpr.getParameters().get(0);
            SQLIntegerExpr sizeExpr=(SQLIntegerExpr) topHitsFieldExpr.getParameters().get(1);
            return createTopHitsBuilder(nameExpr.getText(),sizeExpr.getNumber().intValue());
        }
        else{
            throw new ElasticSql2DslException("[syntax error] can not support top_hits aggregation for field count bigger than 2");
        }
    }

    private AggregationBuilder parseRangeAggregation(String queryAs, SQLExpr rangeFieldExpr, List<RangeSegment> rangeSegments) {

        QueryFieldParser queryFieldParser = new QueryFieldParser();

        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(rangeFieldExpr, queryAs);
        if (queryField.getQueryFieldType() != QueryFieldType.RootDocField && queryField.getQueryFieldType() != QueryFieldType.InnerDocField) {
            throw new ElasticSql2DslException(String.format("[syntax error] can not support range aggregation for field type[%s]", queryField.getQueryFieldType()));
        }

        return createRangeBuilder(queryField.getQueryFieldFullName(), rangeSegments);
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

    private TermsAggregationBuilder createTermsBuilder(String termsFieldName, int termBuckets) {
        return AggregationBuilders.terms(AGG_BUCKET_KEY_PREFIX + termsFieldName+"_terms")
                .field(termsFieldName)
                .minDocCount(1).shardMinDocCount(1)
                .shardSize(termBuckets << 1).size(termBuckets).order(BucketOrder.count(false));
    }

    private TermsAggregationBuilder createTermsBuilder(String termsFieldName) {
        return createTermsBuilder(termsFieldName, MAX_GROUP_BY_SIZE);
    }

    private CardinalityAggregationBuilder createCardinalityBuilder(String fieldName) {
        return AggregationBuilders.cardinality(fieldName+"_cardinality").field(fieldName);
    }

    private TopHitsAggregationBuilder createTopHitsBuilder(int size) {
        return AggregationBuilders.topHits("agg_top_hits").size(size);
    }

    private TopHitsAggregationBuilder createTopHitsBuilder(String name,int size) {
        return AggregationBuilders.topHits(name).size(size);
    }

    private CardinalityAggregationBuilder createCardinalityBuilder(String fieldName,long threshold) {
        return AggregationBuilders.cardinality(fieldName+"_cardinality").field(fieldName).precisionThreshold(threshold);
    }

    private AbstractRangeBuilder createRangeBuilder(String rangeFieldName, List<RangeSegment> rangeSegments) {
        AbstractRangeBuilder rangeBuilder = null;
        RangeSegment.SegmentType segType = rangeSegments.get(0).getSegmentType();

        if (segType == RangeSegment.SegmentType.Numeric) {
            RangeAggregationBuilder numericRangeBuilder = AggregationBuilders.range(AGG_BUCKET_KEY_PREFIX + rangeFieldName+"_range").field(rangeFieldName);
            for (RangeSegment segment : rangeSegments) {
                String key = String.format("%s-%s", segment.getFrom().toString(), segment.getTo().toString());
                numericRangeBuilder.addRange(key, Double.valueOf(segment.getFrom().toString()), Double.valueOf(segment.getTo().toString()));
            }
            rangeBuilder = numericRangeBuilder;
        }

        if (segType == RangeSegment.SegmentType.Date) {

            DateRangeAggregationBuilder dateRangeBuilder = AggregationBuilders.dateRange(AGG_BUCKET_KEY_PREFIX + rangeFieldName+"_range").field(rangeFieldName);
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
