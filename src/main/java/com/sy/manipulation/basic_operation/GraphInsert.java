package com.sy.manipulation.basic_operation;

import com.sy.base.abs.AbstractGraph;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

/**
 * @Author Shi Yan
 * @Date 2020/9/11 15:43
 */
public class GraphInsert extends AbstractGraph {
    public GraphInsert(Driver driver) {
        super(driver);
    }

    /**
     * 插入relation属性和值
     * @param relationType
     * @param name
     * @param value
     */
    public void insertRelationProperty(String relationType, String name, String value) {
        try(Session session = driver.session()) {
            session.writeTransaction(tx -> tx.run("call apoc.periodic.commit(" +
                    "'match ()-[include:"+ relationType +"]->() where not exists(include."+ name +") with include limit {limit} set include."+ name +"= \""+ value +"\" return count(*)'," +
                    "{limit:1000})"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入node属性和值
     * @param nodeLabel
     * @param name
     * @param value
     */
    public void insertNodeProperty(String nodeLabel, String name, String value) {
        try(Session session = driver.session()) {
            session.writeTransaction(tx -> tx.run("call apoc.periodic.commit(" +
                    "'match (n:"+ nodeLabel +") where not exists(n."+ name +") with n limit {limit} set n."+ name +"= \""+ value +"\" return count(*)'," +
                    "{limit:1000}"));
        } catch(Exception e) {
            e.printStackTrace();
        }
    }


}

