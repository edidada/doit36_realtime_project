package com.test.bigdata.rtmk.demo9.groovy;

import groovy.lang.GroovyClassLoader;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class TestDiaoyong {

    public static void main(String[] args) throws IOException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {

        String code = FileUtils.readFileToString(new File("d:/a.groovy"), "utf-8");

        GroovyClassLoader groovyClassLoader = new GroovyClassLoader();
        Class aClass = groovyClassLoader.parseClass(code); // 编译  和 加载

        /*Method sayHello = aClass.getMethod("sayHello", new Class[]{String.class});
        Object obj = aClass.newInstance();
        sayHello.invoke(obj,"zhangsan");*/


        HelloInterface helloworld = (HelloInterface) aClass.newInstance();
        helloworld.sayHello("刘亦菲");

    }

}
