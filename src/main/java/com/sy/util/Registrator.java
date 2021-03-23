package com.sy.util;

import com.esotericsoftware.kryo.Kryo;
import com.sy.base.abs.AbstractMysqlDataProcess;
import com.sy.dataprocess.MysqlDataProcess;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * @Author Shi Yan
 * @Date 2020/8/17 10:44
 */
public class Registrator implements KryoRegistrator{
    @Override
    public void registerClasses(Kryo kryo) {
        //在kyro序列化为中注册自定义类
        kryo.register(MysqlDataProcess.class);
        kryo.register(AbstractMysqlDataProcess.class);
        //kryo.register(ProcessData.class,new FieldSerializer(kryo, ProcessData.class));
    }
}
