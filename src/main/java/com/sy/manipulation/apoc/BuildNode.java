package com.sy.manipulation.apoc;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

import org.neo4j.driver.v1.summary.ResultSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author Shi Yan
 * @Date 2020/9/1 17:00
 */
public class BuildNode {

    private static Logger LOGGER = LoggerFactory.getLogger(BuildNode.class);

    private Driver driver = null;

    public BuildNode(Driver driver) {
        this.driver = driver;
    }

    /**
     * 构建node
     * @param file
     * @param nodeLabel
     * @return
     */
    public void createNode(String file, String nodeLabel) {
        /**
         * 或者用以下方式导入：
         * :auto USING PERIODIC COMMIT 1000
         * LOAD CSV WITH HEADERS FROM "file:/node/question_node.csv"
         * AS line
         * CREATE (q:Question) set q=line
         */
        try(Session session = driver.session()) {
            session.writeTransaction(tx -> tx.run(" call apoc.periodic.iterate(" +
                    "'call apoc.load.csv(\"" + file + "\",{header:true,sep:\",\",ignore:[\"label\"]}) yield map as row return row'," +
                    "'create(p:" + nodeLabel +") set p=row'," +
                    "{batchSize:1000,iterateList:true, parallel:true}) "));//失败的节点会跳过
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }
        System.out.println("create node " + nodeLabel + " done!");
        LOGGER.info("create node " + nodeLabel + " done!");
    }

    /**
     * 构建索引
     * @param nodeLabel
     * @param property
     */
    public boolean createIndex(String nodeLabel, String property) {
        ResultSummary summary = null;
        try(Session session = driver.session()) {
            summary = session.writeTransaction(tx -> tx.run("create index on :" + nodeLabel+ "( " + property+ ")")).summary();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }
        LOGGER.info("create index on " + property + " done!");
        return summary.counters().indexesAdded() == 1;
    }

    /**
     * 构建唯一索引
     * @param nodeLabel
     * @param property
     * @return
     */
    public boolean createOnlyIndex(String nodeLabel, String property) {
        ResultSummary summary = null;
        try(Session session = driver.session()) {
            summary = session.writeTransaction(tx -> tx.run("create constraint on (s:" + nodeLabel + ") assert s." + property + " is unique")).summary();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }
        System.out.println("create only index on " + property + " done!");
        LOGGER.info("create index on " + property + " done!");
        return summary.counters().constraintsAdded() == 1;
    }

    /**
     * 构建全文索引
     * @param nodeLable
     * @param property
     * @return
     */
    public void createAllTextIndex(String nodeLable, String property) {
    }

}
