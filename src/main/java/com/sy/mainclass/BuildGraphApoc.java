package com.sy.mainclass;

import com.sy.init.InitNeo4j;
import com.sy.manipulation.apoc.BuildNode;
import com.sy.manipulation.apoc.BuildRelation;
import com.sy.manipulation.basic_operation.GraphInsert;
import com.sy.type.NodeLabel;
import com.sy.type.RelationType;
import org.neo4j.driver.v1.Driver;

/**
 * 利用apoc构建node与relation
 * @Author Shi Yan
 * @Date 2020/9/3 11:20
 */
public class BuildGraphApoc {

    public static void main(String[] args) {
        Driver driver = InitNeo4j.initDriver();
        createNode(driver, "/node/question_9_node.csv", NodeLabel.Question.name());
        createRelation(driver, "/relation/question_category_9_include_relation.csv", NodeLabel.Question.name(), NodeLabel.KnowledgePoint.name(), RelationType.INCLUDE_A.name());
        InitNeo4j.closeDriver();
    }

    /**
     * 构建 node
     * @param driver
     */
    public static void createNode(Driver driver) {
        createNode(driver, "/node/knowledge_point_node.csv", NodeLabel.KnowledgePoint.name());
    }

    /**
     * 构建 node
     * @param driver
     * @param file
     * @param nodeLabel
     */
    public static void createNode(Driver driver, String file, String nodeLabel) {
        BuildNode buildNode = new BuildNode(driver);
        buildNode.createNode(file, nodeLabel);
        buildNode.createOnlyIndex(nodeLabel, "id");
    }

    /**
     * 构建 relation
     * @param driver
     */
    public static void createRelation(Driver driver) {
        createRelation(driver, "/relation/belongto_relation.csv", NodeLabel.KnowledgePoint.name(), NodeLabel.KnowledgePoint.name(), RelationType.BELONG_TO.name());
    }

    /**
     * 构建relation
     * @param driver
     * @param file
     * @param startNodeLabel
     * @param endNodeLabel
     * @param relType
     */
    public static void createRelation(Driver driver, String file, String startNodeLabel, String endNodeLabel, String relType) {
        BuildRelation buildRelation = new BuildRelation(driver);
        buildRelation.createRelationWithProperty(file, startNodeLabel, endNodeLabel, relType);
    }

    /**
     * 插入属性及值
     * @param driver
     */
    public static void insertProperty(Driver driver) {
        //insertProperty(driver, RelationType.INCLUDE_A.name(), "stage", "小学");
        insertProperty(driver, RelationType.INCLUDE_A.name(), "course", "语文");
    }

    /**
     * 插入属性及值
     * @param driver
     * @param relType
     * @param name
     * @param value
     */
    public static void insertProperty(Driver driver, String relType, String name, String value) {
        GraphInsert insert = new GraphInsert(driver);
        insert.insertRelationProperty(relType, name, value);
    }
}
