package test.utils;

import org.apache.commons.io.FileUtils;

import java.io.File;

public class TimeMeasure {

    public static void measure(String name, ThrowingRunnable runnable) throws Exception {
        new File("data").delete();
        new File("metadata").delete();

        long start = System.currentTimeMillis();
        runnable.run();
        long totalTime = System.currentTimeMillis() - start;
        File outputfile = new File("data");
        System.out.println("======================================================");
        System.out.println("Output file is " + (outputfile.length() + " bytes long."));
        System.out.println("CRC32: " + FileUtils.checksumCRC32(outputfile));
        System.out.println();
        System.out.println(name + " took " + totalTime + " ms");
        System.out.println("Total compression time " + Compressors.getLzmaTotalMs()+" ms");
        System.out.println("Parallelism " + Compressors.getLzmaTotalMs() * 1.0 / totalTime);
    }
}
