package me.yuqiao.test;

import me.yuqiao.demo.windowing.util.SessionWindowingData;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Test;

/**
 * Integration test for streaming programs in Java examples.
 */
public class StreamingDemoITCase extends AbstractTestBase {

    @Test
    public void testSessionWindowing() throws Exception {
        final String resultPath = getTempDirPath("result");
        me.yuqiao.demo.windowing.SessionWindowing.main(
                new String[]{"--output", resultPath});
        compareResultsByLinesInMemory(SessionWindowingData.EXPECTED, resultPath);
    }
}
