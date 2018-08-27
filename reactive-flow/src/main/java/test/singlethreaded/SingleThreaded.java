package test.singlethreaded;

import test.utils.Parameters;
import test.utils.TimeMeasure;
import test.utils.Compressors;

import java.io.*;

public class SingleThreaded {
    public static void main(String[] args) throws Exception {
        TimeMeasure.measure("Single Threaded", () ->
                new SingleThreaded().singleThreaded());
    }
    String name = Parameters.fileToBackup;

    public void singleThreaded() throws IOException {
        long millis = System.currentTimeMillis();
        long totalRead = 0;
        File file = new File(name);
        try(
            FileInputStream fileInputStream = new FileInputStream(name);
            FileOutputStream data = new FileOutputStream("data");
        ) {
            byte[] buffer = new byte[Parameters.blockSize];
            while (fileInputStream.available() > 0) {
                int read = fileInputStream.readNBytes(buffer, 0, buffer.length);
                totalRead += read;
                System.out.println("Read and compressed " + totalRead + " of " + file.length() + " after " + (System.currentTimeMillis() - millis));
                data.write(Compressors.compressLzma(buffer, read));
            }
        }
    }
}
