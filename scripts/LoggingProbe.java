package com.test.bigdata.tools;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

/** Source-file launcher: checks the actual runtime, including classes inside fat connector JARs. */
public final class LoggingProbe {
    public static void main(String[] args) throws Exception {
        String module = args[0];
        String provider = LoggerFactory.getILoggerFactory().getClass().getName();
        if (!provider.equals("org.apache.logging.slf4j.Log4jLoggerFactory")) {
            throw new IllegalStateException("Unexpected SLF4J provider: " + provider);
        }
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        String version = LoggerContext.class.getPackage().getImplementationVersion();
        if (!"2.26.1".equals(version)) { throw new IllegalStateException("Unexpected Log4j core: " + version); }
        var binders = Collections.list(ClassLoader.getSystemClassLoader().getResources("org/slf4j/impl/StaticLoggerBinder.class"));
        int expectedBinders = module.equals("batch_jobs") ? 0 : 1;
        if (binders.size() != expectedBinders) { throw new IllegalStateException("Duplicate/missing SLF4J 1.x binder: " + binders); }
        if (context.getConfiguration().getAppender("File") == null) {
            throw new IllegalStateException("XML File appender is missing");
        }
        LoggerFactory.getLogger(LoggingProbe.class).info("LOGGING_PROBE module={} provider={} core={} coreSource={} xml={}",
                module, provider, version, LoggerContext.class.getProtectionDomain().getCodeSource().getLocation(),
                context.getConfiguration().getConfigurationSource().getLocation());
        context.stop();
        Path log = Path.of(System.getProperty("realtime.log.dir", "logs/runtime"), module + ".log");
        if (!Files.readString(log).contains("LOGGING_PROBE module=" + module)) {
            throw new IllegalStateException("Probe log was not saved: " + log);
        }
    }
}
