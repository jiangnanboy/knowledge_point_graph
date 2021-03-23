package com.sy.manipulation.cypher.node;

import com.opencsv.bean.CsvBindByName;
import com.sy.base.abs.AbstractNode;

/**
 * @Author Shi Yan
 * @Date 2020/8/20 16:24
 */
public class NodeQuestion extends AbstractNode {

    @CsvBindByName(column = "answer")
    public String answer;

    @CsvBindByName(column = "parse")
    public String parse;

    //平台题型
    @CsvBindByName(column = "type")
    public String type;

    @CsvBindByName(column = "ability")
    public String ability;

    @CsvBindByName(column = "complexity")
    public String complexity;

}
