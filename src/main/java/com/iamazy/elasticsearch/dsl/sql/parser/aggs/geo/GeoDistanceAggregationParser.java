package com.iamazy.elasticsearch.dsl.sql.parser.aggs.geo;


import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.iamazy.elasticsearch.dsl.cons.CoreConstants;
import com.iamazy.elasticsearch.dsl.sql.enums.QueryFieldType;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import com.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import com.iamazy.elasticsearch.dsl.sql.model.AggregationQuery;
import com.iamazy.elasticsearch.dsl.sql.model.ElasticSqlQueryField;
import com.iamazy.elasticsearch.dsl.sql.model.RangeSegment;
import com.iamazy.elasticsearch.dsl.sql.model.SqlArgs;
import com.iamazy.elasticsearch.dsl.sql.parser.aggs.AbstractGroupByMethodAggregationParser;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.elasticsearch.dsl.sql.parser.sql.QueryFieldParser;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;

import java.util.List;


/**
 * @author iamazy
 * @date 2019/3/7
 * @descrition
 **/
public class GeoDistanceAggregationParser extends AbstractGroupByMethodAggregationParser {


    private static final String START="*";

    private static final List<String> AGG_GEO_DISTANCE_METHOD = ImmutableList.of("geo_distance","geoDistance");

    private static final List<String> AGG_GEO_DISTANCE_RANGE_METHOD = ImmutableList.of("range");

    private static final List<String> AGG_GEO_DISTANCE_ORIGIN_METHOD = ImmutableList.of("origin");


    private void checkRangeItemAggMethod(SQLMethodInvokeExpr aggInvokeExpr) {
        if (!ElasticSqlMethodInvokeHelper.isMethodOf(AGG_GEO_DISTANCE_RANGE_METHOD, aggInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] Sql not support method:" + aggInvokeExpr.getMethodName());
        }
    }

    private void checkOriginMethod(SQLMethodInvokeExpr aggInvokeExpr) {
        if (!ElasticSqlMethodInvokeHelper.isMethodOf(AGG_GEO_DISTANCE_ORIGIN_METHOD, aggInvokeExpr.getMethodName())) {
            throw new ElasticSql2DslException("[syntax error] Sql not support method:" + aggInvokeExpr.getMethodName());
        }
    }

    private List<RangeSegment> parseRangeSegments(SQLMethodInvokeExpr rangeMethodExpr) {
        List<RangeSegment> rangeSegmentList = Lists.newArrayList();
        for (int pIdx = 2; pIdx < rangeMethodExpr.getParameters().size(); pIdx++) {
            SQLMethodInvokeExpr segMethodExpr = (SQLMethodInvokeExpr) rangeMethodExpr.getParameters().get(pIdx);

            checkRangeItemAggMethod(segMethodExpr);

            Object from = ElasticSqlArgConverter.convertSqlArg(segMethodExpr.getParameters().get(0), false);
            Object to = ElasticSqlArgConverter.convertSqlArg(segMethodExpr.getParameters().get(1), false);
            boolean isSatisfy=(from instanceof Number||from.equals(START))&&((to instanceof Number)||to.equals(START));
            if(isSatisfy) {
                if(!(from.equals(START)&&to.equals(START))) {
                    rangeSegmentList.add(new RangeSegment(from, to, RangeSegment.SegmentType.Geo));
                }
            }
        }
        return rangeSegmentList;
    }

    private RangeSegment parseOriginMethod(SQLMethodInvokeExpr methodInvokeExpr) {
        Object from = ElasticSqlArgConverter.convertSqlArg(methodInvokeExpr.getParameters().get(0), false);
        Object to = ElasticSqlArgConverter.convertSqlArg(methodInvokeExpr.getParameters().get(1), false);
        return new RangeSegment(from,to,RangeSegment.SegmentType.Geo);
    }

    private AggregationBuilder parseGeoDistanceAggregation(String queryAs, SQLExpr rangeFieldExpr,RangeSegment originMethod, List<RangeSegment> rangeSegments) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(rangeFieldExpr, queryAs);
        if (queryField.getQueryFieldType() != QueryFieldType.RootDocField && queryField.getQueryFieldType() != QueryFieldType.InnerDocField) {
            throw new ElasticSql2DslException(String.format("[syntax error] can not support range aggregation for field type[%s]", queryField.getQueryFieldType()));
        }

        return createGeoDistanceBuilder(queryField.getQueryFieldFullName(),originMethod, rangeSegments);
    }

    private DistanceUnit parseDistanceUnit(String unit){
        switch (unit.toLowerCase()){
            case "in":{
                return DistanceUnit.INCH;
            }
            case "yd":{
                return DistanceUnit.YARD;
            }
            case "ft":{
                return DistanceUnit.FEET;
            }
            case "km":{
                return DistanceUnit.KILOMETERS;
            }
            case "nmi":{
                return DistanceUnit.NAUTICALMILES;
            }
            case "mm":{
                return DistanceUnit.MILLIMETERS;
            }
            case "cm":{
                return DistanceUnit.CENTIMETERS;
            }
            case "mi":{
                return DistanceUnit.MILES;
            }
            case "m":
            default:{
                return DistanceUnit.METERS;
            }
        }
    }

    private AggregationBuilder createGeoDistanceBuilder(String queryField,RangeSegment origin, List<RangeSegment> rangeSegments){
        GeoPoint originPoint=new GeoPoint((Double)origin.getFrom(),(Double) origin.getTo());
        GeoDistanceAggregationBuilder geoDistanceAggregationBuilder;
        if(queryField.contains(CoreConstants.POUND)){
            String[] fieldUnit=queryField.split(CoreConstants.POUND);
            String field=fieldUnit[0];
            String unit=fieldUnit[1];
            DistanceUnit distanceUnit=parseDistanceUnit(unit);
            geoDistanceAggregationBuilder=AggregationBuilders.geoDistance(field+"_"+"geo_distance",originPoint).field(field).unit(distanceUnit);
        }else {
            geoDistanceAggregationBuilder = AggregationBuilders.geoDistance(queryField + "_" + "geo_distance", originPoint).field(queryField);
        }
        for(RangeSegment rangeSegment:rangeSegments){
            if(rangeSegment.getFrom() instanceof Number&&rangeSegment.getTo() instanceof Number) {
                Double lat=(Double) rangeSegment.getFrom();
                Double lon=(Double) rangeSegment.getTo();
                geoDistanceAggregationBuilder.addRange(lat,lon);
            }
            else if(rangeSegment.getFrom().equals(START)&&rangeSegment.getTo() instanceof Number){
                Double lon=(Double) rangeSegment.getTo();
                geoDistanceAggregationBuilder.addUnboundedTo(lon);
            }
            else if(rangeSegment.getTo().equals(START)&&rangeSegment.getFrom() instanceof Number){
                Double lat=(Double) rangeSegment.getFrom();
                geoDistanceAggregationBuilder.addUnboundedFrom(lat);
            }
        }
        return geoDistanceAggregationBuilder;
    }

    @Override
    public AggregationQuery parseAggregationMethod(MethodInvocation invocation) throws ElasticSql2DslException {
        SQLMethodInvokeExpr originMethod = (SQLMethodInvokeExpr) invocation.getMethodInvokeExpr().getParameters().get(1);
        checkOriginMethod(originMethod);
        RangeSegment origin=parseOriginMethod(originMethod);
        List<RangeSegment> rangeSegments = parseRangeSegments(invocation.getMethodInvokeExpr());
        SQLExpr fieldExpr = invocation.getMethodInvokeExpr().getParameters().get(0);
        return new AggregationQuery(parseGeoDistanceAggregation(invocation.getQueryAs(),fieldExpr,origin,rangeSegments));
    }

    @Override
    public List<String> defineMethodNames() {
        return AGG_GEO_DISTANCE_METHOD;
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        if(invocation.getParameterCount()>=3) {
            return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
        }
        return false;
    }
}
