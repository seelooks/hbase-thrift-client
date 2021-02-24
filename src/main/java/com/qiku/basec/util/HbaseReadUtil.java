package com.qiku.basec.util;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;

/**
 * @Description:
 * @Auther: v-wangbo-os@360os.com
 * @Date: 2021/2/24 15:27
 */

public class HbaseReadUtil {

    public static List<String> byteBuffersToStrs(List<ByteBuffer> buffer) {
        List<String> strs = new ArrayList<>();
        if (buffer == null || buffer.isEmpty()){
            return strs;
        }
        for (ByteBuffer item : buffer){
            String str = byteBufferToStr(item);
            strs.add(str);
        }
        return strs;
    }



    public static String byteBufferToStr(ByteBuffer buffer) {
        try {
            Charset charset = Charset.forName("UTF-8");
            CharsetDecoder decoder = charset.newDecoder();
            CharBuffer charBuffer = decoder.decode(buffer);
            buffer.flip();
            return charBuffer.toString();
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

}