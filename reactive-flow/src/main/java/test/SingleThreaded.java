package test;

import java.io.*;

public class SingleThreaded {
    public static void main(String[] args) throws IOException {
        new SingleThreaded().singleThreaded();
    }
//            String name = "C:\\backup\\backup-demo\\shakespeare.txt";
    String name = "C:\\backup\\dest1\\volume\\volume_000000.json";

    public void singleThreaded() throws IOException {
        long millis = System.currentTimeMillis();
        long totalRead = 0;
        File file = new File(name);
        FileInputStream fileInputStream = new FileInputStream(name);
        byte[] buffer = new byte[1024 * 1024];
        while (fileInputStream.available() > 0) {
            int read = fileInputStream.readNBytes(buffer, 0, buffer.length);
            totalRead += read;
            System.out.println("Read and compressed "+totalRead+" of "+ file.length()+" after " +(System.currentTimeMillis() - millis));
            Compressors.compressLzma(buffer, read);
        }
    }
}
