package test.singlethreaded;

import org.apache.commons.io.FileUtils;
import test.utils.Parameters;
import test.utils.TimeMeasure;
import test.utils.Compressors;

import java.io.*;
import java.util.ArrayList;

public class SingleThreaded {
    public static void main(String[] args) throws Exception {
        TimeMeasure.measure("Single Threaded", () ->
                new SingleThreaded().singleThreaded());
    }

    String name = Parameters.fileToBackup;

    public void singleThreaded() throws IOException {
        long millis = System.currentTimeMillis();
        long totalRead = 0;
        int block = 0;
        File file = new File(name);
        ArrayList<String> metadata = new ArrayList<>();
        try (
                FileInputStream fileInputStream = new FileInputStream(file);
                FileOutputStream data = new FileOutputStream("data");
        ) {
            byte[] buffer = new byte[Parameters.blockSize];
            while (fileInputStream.available() > 0) {
                int read = fileInputStream.readNBytes(buffer, 0, buffer.length);
                byte[] compressed = Compressors.compressLzma(buffer, read);
                data.write(compressed);
                metadata.add(block + " " + compressed.length);
                block++;
                totalRead += read;
                System.out.println("Read and compressed " + totalRead + " of " + file.length() + " after " + (System.currentTimeMillis() - millis));
            }
        }
        FileUtils.writeLines(new File("metadata"), metadata);
    }
}
