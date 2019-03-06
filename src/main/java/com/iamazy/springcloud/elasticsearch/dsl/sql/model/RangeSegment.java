package com.iamazy.springcloud.elasticsearch.dsl.sql.model;

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
        Date,Numeric
    }
}
