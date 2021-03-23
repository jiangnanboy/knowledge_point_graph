package com.sy.init;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author Shi Yan
 * @Date 2020/8/11 17:42
 */
public class InitSchema {

    public static Encoder<String> stringEncoder() {
        return Encoders.STRING();
    }

    public static StructType initSchema() {
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType,false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("label", DataTypes.StringType, false, Metadata.empty())
        });
        return schema;
    }

    public static StructType initSchema(String[] filedName) {
        List<StructField> fileds = new ArrayList<>();
        for(String filed:filedName) {
            fileds.add(DataTypes.createStructField(filed, DataTypes.StringType, false, Metadata.empty()));
        }
        StructType schema = DataTypes.createStructType(fileds);
        return schema;
    }

}
