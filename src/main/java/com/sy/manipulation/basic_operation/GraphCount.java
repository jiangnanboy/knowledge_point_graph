package com.sy.manipulation.basic_operation;

import com.sy.base.abs.AbstractGraph;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;

import java.util.List;

/**
 * @Author Shi Yan
 * @Date 2020/9/3 11:27
 */
public class GraphCount extends AbstractGraph {

    public GraphCount(Driver driver) {
        super(driver);
    }

    /**
     * 统计label为nodeLabel的节点数量
     * @param nodeLabel
     */
    public int countNode(String nodeLabel) {
        int nodeCount =0;
        try(Session session = driver.session()) {
            List<Record> listRecord = session.readTransaction(tx -> tx.run("match(:" + nodeLabel + ") return count(*)")).list();
            nodeCount = listRecord.get(0).get(0).asInt();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return nodeCount;
    }

}
