package org.iamazy.elasticsearch.dsl.sql.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/
@Getter
@AllArgsConstructor
public class RangeSegment {

    private Object from;
    private Object to;
    private SegmentType segmentType;



    public enum SegmentType{
        /**
         * 时间类型
         */
        Date,
        /**
         * 数字类型
         */
        Numeric,
        /**
         * Ip类型
         */
        Ip,
        /**
         * 地理类型
         */
        Geo
    }
}
