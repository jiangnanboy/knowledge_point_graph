package com.sy.manipulation.cypher;

import com.sy.base.abs.AbstractCsvRead;

import com.sy.manipulation.cypher.node.NodeBook;
import com.sy.manipulation.cypher.node.NodeGrade;
import com.sy.manipulation.cypher.node.NodeKnowledgePoint;
import com.sy.manipulation.cypher.node.NodeStage;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.neo4j.driver.v1.Values.parameters;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;


/**
 * @Author Shi Yan
 * @Date 2020/8/20 9:22 上午
 */
public class BuildNode extends AbstractCsvRead {

    private static Logger LOGGER = LoggerFactory.getLogger(BuildNode.class);

    public BuildNode(Driver drive) {
        super(drive);
    }

    /**
     * 创建知识点节点
     * @param csvFile
     * @param nodeLabel
     */
    public void createKGPNode(String csvFile, String nodeLabel) {
        Path fPath = Paths.get(csvFile);
        try (BufferedReader br = Files.newBufferedReader(fPath); Session session = driver.session();){
            Iterator<NodeKnowledgePoint> iterator = readCsv(br, NodeKnowledgePoint.class);
            while(iterator.hasNext()) {
                NodeKnowledgePoint kg = iterator.next();
                session.writeTransaction(tx -> tx.run("create(:"+ kg.label + "{name:{name}, id:{id}})", parameters("name", kg.name, "id", kg.id)));
            }
            session.writeTransaction(tx -> tx.run("create index on :" + nodeLabel + "(id)"));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        LOGGER.info("Create node of knowledge_point done!");
    }

    /**
     * 创建 book node
     * @param csvFile
     */
    public void createBookNode(String csvFile) {
        Path fPath = Paths.get(csvFile);
        try(BufferedReader br = Files.newBufferedReader(fPath); Session session = driver.session(); ) {
            Iterator<NodeBook> iterator = readCsv(br, NodeBook.class);
            while (iterator.hasNext()) {
                NodeBook book = iterator.next();
                session.writeTransaction(tx -> tx.run("create (b:Book {name:{name}, id:{id}})", parameters("name", book.name, "id", book.id)));
            }
            session.writeTransaction(tx -> tx.run("create index on :Book(id)"));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        LOGGER.info("Create knowledge point done!");
    }

    /**
     * 创建 stage node
     * @param csvFile
     */
    public void createStageNode(String csvFile) {
        Path fPath = Paths.get(csvFile);
        try(BufferedReader br = Files.newBufferedReader(fPath);Session session = driver.session(); ) {
            Iterator<NodeStage> iterator = readCsv(br, NodeStage.class);
            while (iterator.hasNext()) {
                NodeStage stage = iterator.next();
                session.writeTransaction(tx -> tx.run("create (s:Stage {name:{name}, id:{id}})", parameters("name", stage.name, "id", stage.id)));
            }
            session.writeTransaction(tx -> tx.run("create index on :Stage(id)"));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        LOGGER.info("Create knowledge point done!");
    }

    /**
     * 创建 grade node
     * @param csvFile
     */
    public void createGradeNode(String csvFile) {
        Path fPath = Paths.get(csvFile);
        try(BufferedReader br = Files.newBufferedReader(fPath);Session session = driver.session(); ) {
            Iterator<NodeGrade> iterator = readCsv(br, NodeGrade.class);
            while (iterator.hasNext()) {
                NodeGrade grade = iterator.next();
                session.writeTransaction(tx -> tx.run("create (g:Grade {name:{name}, id:{id}})",
                        parameters("name", grade.name, "id", grade.id)));
            }
            session.writeTransaction(tx -> tx.run("create index on :Grade(id)"));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        LOGGER.info("Create knowledge point done!");
    }

}
