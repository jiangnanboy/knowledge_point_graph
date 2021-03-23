package com.sy.manipulation.basic_operation;

import com.sy.base.abs.AbstractGraph;
import org.neo4j.driver.v1.*;

import java.util.List;

/**
 * @Author Shi Yan
 * @Date 2020/8/7 16:00
 */
public class GraphSearch extends AbstractGraph {

    public GraphSearch(Driver driver) {
        super(driver);
    }

    /**
     * 查找某学科根节点所有的知识点(如返回根节点[id]为"高中数学"的所有知识点)
     * @param id
     * @return
     */
    public List<Record> findCourseAllKnowledgePoint(String id) {
        List<Record> listRecord = null;
        try(Session session = driver.session()) {
            listRecord = session.readTransaction(tx ->
                    tx.run("match path=(source:KnowledgePoint)-[BELONG_TO*..]->(target:KnowledgePoint) where target.id = $id " +
                                    "return relationships(path) as relationships, nodes(path) as nodes",
                            Values.parameters("id", id))).list();
        }
        return listRecord;
    }

    /**
     * 查找该知识点的所有父知识点
     * @param id
     * @return
     */
    public List<Record> findKnowledgePointAndAllFather(String id) {

        List<Record> listRecord = null;
        try (Session session = driver.session()){
            listRecord = session.readTransaction(tx ->

                    tx.run("match path=(source:KnowledgePoint)-[:BELONG_TO*..]->(target:KnowledgePoint)  where source.id = $id " +
                            "return relationships(path) as relationships, nodes(path) as nodes", // order by length(path) desc limit 1
                            Values.parameters("id", id))).list();
        }
        return listRecord;
    }

    /**
     * 查找该题目包含的知识点
     * @param id
     * @return
     */
    public List<Record> findQuestionIncludeKnowledgePoint(String id) {
        List<Record> listRecord = null;
        try(Session session = driver.session()) {
            listRecord = session.readTransaction(tx ->
                    tx.run("match path=(q:Question)-[:INCLUDE_A]->(:KnowledgePoint) where q.id = $id " +
                                    "return relationships(path) as relationships, nodes(path) as nodes",
                            Values.parameters("id", id))).list();
        }
        return listRecord;
    }

    /**
     * 查找该题目包含的知识点及其所有的父知识点
     * @param id
     * @return
     */
    public List<Record> findQuestionIncludeKnowledgePointAndAllFather(String id) {
        List<Record> listRecord = null;
        try(Session session = driver.session()) {
            listRecord = session.readTransaction(tx ->
                    tx.run("match path=(q:Question)-[:INCLUDE_A]->(:KnowledgePoint)-[:BELONG_TO*..]->(:KnowledgePoint) where q.id = $id " +
                                    "return relationships(path) as relationships, nodes(path) as nodes",
                            Values.parameters("id", id))).list();
        }
        return listRecord;
    }

}
