package com.sy.manipulation.basic_operation;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.beans.Encoder;
import java.util.Map;

/**
 * @Author Shi Yan
 * @Date 2020/9/16 17:37
 */
public class GraphToJson {
    /**
     * 查询出的数据转为json格式
     */
    public static void toJson() {
        JsonObject msgObj = new JsonObject();
        msgObj.addProperty("test", "123");
        msgObj.addProperty("test1", "1233");
        System.out.println(msgObj.toString());
        String msgStr = msgObj.toString();
        Gson gson = new Gson();
        JsonObject obj = gson.fromJson(msgStr, JsonObject.class);
        System.out.println(obj.get("test"));
        for(Map.Entry<String, JsonElement> set : obj.entrySet()) {
            System.out.println(set.getKey() + " : " + set.getValue());
        }
    }
}
