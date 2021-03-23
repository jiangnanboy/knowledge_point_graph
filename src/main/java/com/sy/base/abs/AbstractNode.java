package com.sy.base.abs;

import com.opencsv.bean.CsvBindByName;

/**
 * @Author Shi Yan
 * @Date 2020/7/30 5:25 下午
 */
public abstract class AbstractNode {

    @CsvBindByName(column = "id")
    public String id;

    @CsvBindByName(column = "label")
    public String label;

    @CsvBindByName(column = "name")
    public String name;
}
