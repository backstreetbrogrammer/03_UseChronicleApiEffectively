package com.backstreetbrogrammer.chronicle;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChronicleQueueIntroductionTest {

    String basePath;
    File queueDir;
    ChronicleQueue queue;

    @BeforeEach
    void setUp() throws IOException {
        /*
         * creates a directory in target/ folder
         */
        //basePath = OS.getTarget() + "/getting-started";
        //queue = SingleChronicleQueueBuilder.single(basePath).build();

        /*
         * creates a directory in Temp/ folder
         */
        basePath = "chronicle-queue";
        queueDir = Files.createTempDirectory(basePath).toFile();
        queue = SingleChronicleQueueBuilder.single(queueDir).build();
    }

    @Test
    void testWriteAndRead() {
        writeToQueue();

        // Read from the queue
        ExcerptTailer tailer = queue.createTailer();
        tailer.readDocument(w -> System.out.println("msg: " + w.read(() -> "msg").text()));

        assertEquals("TestMessage2", tailer.readText());
    }

    private void writeToQueue() {
        // Obtain an ExcerptAppender
        ExcerptAppender appender = queue.acquireAppender();

        // Writes: {msg: TestMessage}
        appender.writeDocument(w -> w.write("msg").text("TestMessage1"));

        // Writes: TestMessage
        appender.writeText("TestMessage2");
    }

    @AfterEach
    void tearDown() throws IOException {
        queue.close();

        // if I don't want to persist the state for multiple runs
        //   - delete the dir
        FileUtils.deleteDirectory(queueDir);
    }
}
