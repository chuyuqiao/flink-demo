package me.yuqiao.test;

import me.yuqiao.demo.windowing.util.SessionWindowingData;
import org.apache.flink.test.testdata.WordCountData;
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

    @Test
    public void testWindowWordCount() throws Exception {
        final String windowSize = "250";
        final String slideSize = "150";
        final String textPath = createTempFile("text.txt", WordCountData.TEXT);
        final String resultPath = getTempDirPath("result");

        me.yuqiao.demo.windowing.WindowWordCount.main(
                new String[] {
                        "--input", textPath,
                        "--output", resultPath,
                        "--window", windowSize,
                        "--slide", slideSize
                });

        // since the parallel tokenizers might have different speed
        // the exact output can not be checked just whether it is well-formed
        // checks that the result lines look like e.g. (faust, 2)
        checkLinesAgainstRegexp(resultPath, "^\\([a-z]+,(\\d)+\\)");
    }
}
