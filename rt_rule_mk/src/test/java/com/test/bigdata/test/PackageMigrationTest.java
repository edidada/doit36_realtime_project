package com.test.bigdata.test;

import com.test.bigdata.rtmk.demo5.RuleCalculator;
import com.test.bigdata.rtmk.demo9.groovy.HelloInterface;
import groovy.lang.GroovyClassLoader;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PackageMigrationTest {
    @Test
    public void dynamicRuleSourceUsesTheMigratedJavaInterface() throws Exception {
        Path source = Path.of("src/main/java/com/test/bigdata/rtmk/demo9/groovy/RuleMode005Calculator.groovy");
        try (GroovyClassLoader loader = new GroovyClassLoader(getClass().getClassLoader())) {
            Class<?> rule = loader.parseClass(Files.readString(source));
            assertTrue(RuleCalculator.class.isAssignableFrom(rule));
            assertEquals("com.test.bigdata.rtmk.demo9.groovy.RuleModel005Calculator", rule.getName());
        }
    }

    @Test
    public void dynamicallyLoadedCodeCanImplementTheMigratedHelloInterface() throws Exception {
        String source = "package com.test.bigdata.rtmk.demo9.groovy; "
                + "class MigrationHello implements HelloInterface { void sayHello(String name) {} }";
        try (GroovyClassLoader loader = new GroovyClassLoader(getClass().getClassLoader())) {
            Object instance = loader.parseClass(source).getDeclaredConstructor().newInstance();
            assertTrue(instance instanceof HelloInterface);
            ((HelloInterface) instance).sayHello("migration-check");
        }
    }
}
