package com.sy.init;

import com.sy.util.PropertiesReader;
import org.neo4j.driver.v1.*;

/**
 * @Author Shi Yan
 * @Date 2020/7/24 9:22 上午
 */
public class InitNeo4j {
    private static Driver driver = null;

    static final String URL;
    static final String NAME;
    static final String PASSWORD;

    static {
        URL = PropertiesReader.get("neo4j_url");
        NAME = PropertiesReader.get("neo4j_name");
        PASSWORD = PropertiesReader.get("neo4j_password");
    }

    /**
     * 初始化 driver
     * @return
     */
    public static Driver initDriver() {
        if(null == driver) {
            driver = GraphDatabase.driver(URL, AuthTokens.basic(NAME, PASSWORD));
        }
        return driver;
    }

    /**
     * 关闭 driver
     */
    public static void closeDriver() {
        if(null != driver) {
            driver.close();
        }
    }

}
