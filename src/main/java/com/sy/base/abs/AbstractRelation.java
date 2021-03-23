package com.sy.base.abs;

import com.opencsv.bean.CsvBindByName;

/**
 *  @Author Shi Yan
 *  @Date 2020/7/31 10:53 上午
 */
public abstract class AbstractRelation {

    @CsvBindByName(column = "startId")
    public String startId;

    @CsvBindByName(column = "endId")
    public String endId;

    @CsvBindByName(column = "type")
    public String type;
}
