package com.database.global;

/**
 * Created by zoe on 2016/12/5.
 */
public class Utils {
    public static byte[] shortToBytes( short value )
    {
        byte[] src = new byte[2];
        int len = src.length;
        for(int i =0;i < len; i++){
            src[i] = (byte) ((value >> (len - 1 - i)) & 0xFF);
        }
        return src;
    }
    public static byte[] intToBytes( int value )
    {
        byte[] src = new byte[4];
        int len = src.length;
        for(int i =0;i < len; i++){
            src[i] = (byte) ((value >> (len - 1 - i)) & 0xFF);
        }
        return src;
    }
    public static byte[] longToBytes( long value )
    {
        byte[] src = new byte[8];
        int len = src.length;
        for(int i =0;i < len; i++){
            src[i] = (byte) ((value >> (len - 1 - i)) & 0xFF);
        }
        return src;
    }

    public static byte[] fillShort(short value, byte[] bytes,int start )
    {
        if(bytes.length < start + 2 - 1)
            return null;

        for(int i = 0; i < 2; i++){
            bytes[start+i] = (byte) ((value >> 8*(2 - 1 - i)) & 0xFF);
        }

        return bytes;
    }
    public static byte[] fillInt(int value, byte[] bytes,int start )
    {
        if(bytes.length < start + 4 - 1)
            return null;

        for(int i = 0;i < 4; i++){
            bytes[start+i] = (byte) ((value >> 8*(4 - 1 - i)) & 0xFF);
        }

        return bytes;
    }
    public static byte[] fillLong(long value, byte[] bytes,int start)
    {
        if(bytes.length < start + 8 - 1)
            return null;

        for(int i =0; i <  8; i++){
            bytes[start+i] = (byte) ((value >> 8*(8 - 1 - i)) & 0xFF);
        }

        return bytes;
    }
    public static byte[] fillString(String value, byte[] bytes,int start)
    {
        int len = value.length();
        if(bytes.length < start + len - 1)
            return null;

        byte[] strBytes = value.getBytes();

        for(int i  = 0;i < len; i++){
           bytes[start+i] = strBytes[i];
        }

        return bytes;
    }
    public static byte[] fillBytes(byte[] value, byte[] bytes,int start)
    {
        int len = value.length;
        if(bytes.length < start + len )
            return null;

        for(int i =0;i < len; i++){
            bytes[start+i] = value[i];
        }

        return bytes;
    }

    public static int loadIntFromBytes(byte[] data, int start){
        assert(start + 4 > data.length);

        int intValue = 0;
        for(int i = 0; i< 4; i++){
            intValue |= (data[start+i]&0xFF) << 8*(4 - 1 - i) ;
        }

        return intValue;
    }

    public static short loadShortFromBytes(byte[] data, int start){
        assert(start + 2 > data.length);

        short shortValue = 0;
        for(int i = 0; i<  2; i++){
            shortValue |= (data[start+i]&0xFF) << 8*(2 - 1 - i) ;
        }

        return shortValue;
    }
    public static long loadLongFromBytes(byte[] data, int start){
        assert(start + 8 > data.length);

        int low = 0;
        int high = 0;
        long longValue = 0;
        for(int i = 0; i< 8; i++){
            if(i<4) {
                low |= (data[start + i] & 0xFF) << 8 * (8 - 1 - i);
            } else {
                high |= (data[start + i] & 0xFF) << 8 * (8 - 1 - i);
            }
        }
        longValue = (long)low<<32&0xffffffff00000000l | ((long)high&0x00000000ffffffffl);
        return longValue;
    }
    public static String loadStrFromBytes(byte[] data, int start, int len){
        assert(start + len > data.length);

        byte[] strBytes = new byte[len];
        for(int i = 0; i< len; i++){
            strBytes[i] = data[start + i];
        }

        return  new String(strBytes);
    }
}
