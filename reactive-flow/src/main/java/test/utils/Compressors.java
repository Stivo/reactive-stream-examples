package test.utils;

import org.springframework.util.StreamUtils;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.UnsupportedOptionsException;
import org.tukaani.xz.XZOutputStream;
import test.utils.Indexed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class Compressors {

    private static AtomicLong lzmaTotalMs = new AtomicLong();

    public static byte[] compressLzma(byte[] value, int length) {
        long start = System.currentTimeMillis();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value, 0, length);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        var opt = new LZMA2Options();
        try {
            opt.setDictSize(1024 * 1024 * 1);
        } catch (UnsupportedOptionsException e) {
            e.printStackTrace();
        }

        try (XZOutputStream gzipOutputStream = new XZOutputStream(out, opt)){
            StreamUtils.copy(byteArrayInputStream, gzipOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] bytes = out.toByteArray();
        lzmaTotalMs.addAndGet(System.currentTimeMillis() - start);
        return bytes;
    }

    public static long getLzmaTotalMs() {
        return lzmaTotalMs.get();
    }

    public static Indexed<byte[]> compressLzmaIndexed(Indexed<byte[]> indexed, int read) {
        return new Indexed<>(compressLzma(indexed.getValue(), read), indexed.getIndex());
    }
}
