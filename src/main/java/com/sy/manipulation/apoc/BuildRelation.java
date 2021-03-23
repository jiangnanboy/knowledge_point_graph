package com.sy.manipulation.apoc;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.neo4j.driver.v1.Values.parameters;

/**
 * @Author Shi Yan
 * @Date 2020/9/1 17:01
 */
public class BuildRelation {
    private static Logger LOGGER = LoggerFactory.getLogger(BuildNode.class);

    private Driver driver = null;

    public BuildRelation(Driver driver) {this.driver = driver;}

    /**
     * 构建relation
     * @param file
     * @param startNodeLabel
     * @param endNodeLabel
     * @param relType
     * @return
     */
    public boolean createRelation(String file, String startNodeLabel, String endNodeLabel, String relType) {
        ResultSummary summary = null;
        try (Session session = driver.session()){
            summary = session.writeTransaction(tx -> tx.run("call apoc.periodic.iterate(" +
                    "'call apoc.load.csv(\""+ file + "\",{header:true,sep:\",\",ignore:[\"type\"]}) yield map as row match (start:" + startNodeLabel + "{id:row.startId}), (end:" + endNodeLabel + "{id:row.endId}) return start,end'," +
                    "'merge (start)-[:" + relType + "]->(end)'," +
                    "{batchSize:1000,iterateList:true, parallel:false});")).summary();
        } catch(Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }
        System.out.println("create relation of " + relType + " done!");
        LOGGER.info("create relation of " + relType + " done!");
        return summary.counters().relationshipsCreated() == 1;
    }

    /**
     * 构建relation
     * @param file
     * @param startNodeLabel
     * @param endNodeLabel
     * @param relType
     * @return
     */
    public boolean createRelationWithProperty(String file, String startNodeLabel, String endNodeLabel, String relType) {
        ResultSummary summary = null;
        try (Session session = driver.session()){
            summary = session.writeTransaction(tx -> tx.run("call apoc.periodic.iterate(" +
                    "'call apoc.load.csv(\""+ file + "\",{header:true,sep:\",\",ignore:[\"type\"]}) yield map as row match (start:" + startNodeLabel + "{id:row.startId}), (end:" + endNodeLabel + "{id:row.endId}) return start,end,row.stage as stage,row.course as course'," +
                    "'merge (start)-[r:" + relType + "]->(end) set r.course=course,r.stage=stage'," +
                    "{batchSize:1000,iterateList:true, parallel:false});")).summary();
        } catch(Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage(), e);
        }
        System.out.println("create relation of " + relType + " done!");
        LOGGER.info("create relation of " + relType + " done!");
        return summary.counters().relationshipsCreated() == 1;
    }

}
