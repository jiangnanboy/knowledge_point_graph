package com.sy.mainclass;

import com.sy.manipulation.basic_operation.GraphSearch;
import com.sy.manipulation.cypher.BuildNode;
import com.sy.manipulation.cypher.BuildRelation;
import com.sy.init.InitNeo4j;

import com.sy.type.NodeLabel;
import com.sy.type.RelationType;
import com.sy.util.FilePath;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;

import java.util.List;

/**
 * 利用cypher构建node与relation
 * @Author Shi Yan
 * @Date 2020/7/24 1:11 下午
 */
public class BuildGraphCypher {
    public static void main(String[] args) {
        Driver driver = InitNeo4j.initDriver();
        createNode(driver);
        createRelation(driver);
        InitNeo4j.closeDriver();
    }

    /**
     * 构建 node
     * @param driver
     */
    public static void createNode(Driver driver) {
        createNode(driver, FilePath.NODE_KNOWLEDGE_POINT_PATH, NodeLabel.KnowledgePoint.name());
    }

    /**
     * 构建 node
     * @param driver
     * @param csvFile
     * @param nodeLable
     */
    public static void createNode(Driver driver, String csvFile, String nodeLable) {
        BuildNode createNode = new BuildNode(driver);
        createNode.createKGPNode(csvFile, nodeLable);
    }

    /**
     * 构建 relation
     * @param driver
     */
    public static void createRelation(Driver driver) {
        createRelation(driver, FilePath.RELATION_INCLUDE_PATH, NodeLabel.KnowledgePoint.name(), RelationType.BELONG_TO.toString());
    }

    /**
     * 构建 relation
     * @param driver
     * @param csvFile
     * @param nodeLabel
     * @param relType
     */
    public static void createRelation(Driver driver, String csvFile, String nodeLabel, String relType) {
        BuildRelation createRelation = new BuildRelation(driver);
        createRelation.createKGPoint(csvFile, nodeLabel, relType);
    }

}
