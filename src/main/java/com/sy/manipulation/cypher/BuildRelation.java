package com.sy.manipulation.cypher;

import com.sy.base.abs.AbstractCsvRead;

import com.sy.manipulation.cypher.relation.*;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

/**
 * Created by YanShi on 2020/8/20 1:10 下午
 */
public class BuildRelation extends AbstractCsvRead {

    private static Logger LOGGER = LoggerFactory.getLogger(BuildRelation.class);

    public BuildRelation(Driver driver) {
        super(driver);
    }

    /**
     * 创建知识点之间的关系
     * @param csvFile
     * @param nodeLabel
     * @param relationType
     */
    public void createKGPoint(String csvFile, String nodeLabel, String relationType) {
        Path fPath = Paths.get(csvFile);
        try(BufferedReader br = Files.newBufferedReader((fPath));Session session = driver.session();){
            Iterator<RelationInclude> iterator = readCsv(br, RelationInclude.class);
            while (iterator.hasNext()) {
                RelationInclude include = iterator.next();
                session.writeTransaction(tx -> tx.run("match (s:" + nodeLabel + "), (e:" + nodeLabel + ") where s.id = " +
                        "\"" + include.startId + "\" and e.id = \"" + include.endId + "\" merge (s)-[:" + include.type + "]->(e)"));
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        LOGGER.info("Create relationship of knowledge_point done!");
    }

    /**
     * 创建 the relatin of stage
     * @param csvFile
     */
    public void createStageRel(String csvFile) {
        Path fPath = Paths.get(csvFile);
        try(BufferedReader br = Files.newBufferedReader((fPath));Session session = driver.session();){
            Iterator<RelationStage> iterator = readCsv(br, RelationStage.class);
            while (iterator.hasNext()) {
                RelationStage stage = iterator.next();
                session.writeTransaction(tx -> tx.run("match (m:Movie), (p:Person) where m.id = " +
                        "\"" + stage.startId + "\" and p.id = \"" + stage.endId + "\" merge (m)-[:actor]->(p)"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("create relationship of stage done!");
    }

    /**
     * 创建 the relation of grade
     * @param csvFile
     */
    public void createGradeRel(String csvFile) {
        Path fPath = Paths.get(csvFile);
        try(BufferedReader br = Files.newBufferedReader((fPath));Session session = driver.session();){
            Iterator<RelationGrade> iterator = readCsv(br, RelationGrade.class);
            while (iterator.hasNext()) {
                RelationGrade grade = iterator.next();
                session.writeTransaction(tx -> tx.run("match (m:Movie), (p:Person) where m.id = " +
                        "\"" + grade.startId + "\" and p.id = \"" + grade.endId + "\" merge (m)-[:composer]->(p)"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("create relationship of grade done!");
    }

    /**
     * 创建 the relation of semester
     * @param csvFile
     */
    public void createSemesterRel(String csvFile) {
        Path fPath = Paths.get(csvFile);
        try(BufferedReader br = Files.newBufferedReader((fPath));Session session = driver.session();){
            Iterator<RelationSemester> iterator = readCsv(br, RelationSemester.class);
            while (iterator.hasNext()) {
                RelationSemester semester = iterator.next();
                session.writeTransaction(tx -> tx.run("match (m:Movie), (p:Person) where m.id = " +
                        "\"" + semester.startId + "\" and p.id = \"" + semester.endId +"\" merge (m)-[:director]->(p)"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("create relationship of semester done!");
    }

    /**
     * 创建 the relation of course
     * @param csvFile
     */
    public void createCourseRel(String csvFile) {
        Path fPath = Paths.get(csvFile);
        try(BufferedReader br = Files.newBufferedReader((fPath));Session session = driver.session();){
            Iterator<RelationCourse> iterator = readCsv(br, RelationCourse.class);
            while (iterator.hasNext()) {
                RelationCourse course = iterator.next();
                session.writeTransaction(tx -> tx.run("match (m:Movie), (c:Country)" +
                        "where m.id = \"" + course.startId + "\" AND c.id = \"" + course.endId + "\" merge (m)-[:district]->(c)"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("create relationship of course done!");
    }

}

