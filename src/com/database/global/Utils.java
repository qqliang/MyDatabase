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

    /**
     * 填充变长的整型值（可以是short 、int）
     * @param value 所要填充的整型值
     * @param bytes
     * @param start
     * @return  该值实际长度,返回-1表示错误(值已经超出32bit整型范围)
     */
    public static int fillVarInt(int value, byte[] bytes, int start){
        if(value > Integer.MAX_VALUE)
            return -1;
        //1B
        if((value & ~0x7f) == 0){
            bytes[start] = (byte)value;
            return 1;
        }
        //2B
        if( (value & ~0x3fff)==0 ){
            bytes[start] = (byte)((value >> 7) | 0x80);
            bytes[start + 1] = (byte)(value & 0x7f);
            return 2;
        }
        //3B
        if( (value & ~0x1fffff)==0 ){
            bytes[start] = (byte)((value>>14) | 0x80);
            bytes[start + 1] = (byte)((value>>7) | 0x80);
            bytes[start + 2] = (byte)(value & 0x7f);
            return 3;
        }
        //4B
        if( (value & ~0x0fffffff) == 0 ){
            bytes[start] = (byte)((value>>21) | 0x80);
            bytes[start + 1] = (byte)((value>>14) | 0x80);
            bytes[start + 2] = (byte)((value>>7) | 0x80);
            bytes[start + 3] = (byte)(value & 0x7f);
            return 4;
        }else{
            bytes[start] = (byte)((value>>28) | 0x80);
            bytes[start + 1] = (byte)((value>>21) | 0x80);
            bytes[start + 2] = (byte)((value>>14) | 0x80);
            bytes[start + 3] = (byte)((value>>7) | 0x80);
            bytes[start + 4] = (byte)(value & 0x7f);
            return 5;

        }
    }
    /**
     * 填充变长的长整型值
     * @param value 所要填充的整型值
     * @param bytes
     * @param start
     * @return  该值实际长度,返回-1表示错误(值已经超出32bit整型范围)
     */
    public static int fillVarLong(long value, byte[] bytes, int start){
        if(value > Long.MAX_VALUE)
            return -1;
        if(value <= Integer.MAX_VALUE || value>= Integer.MIN_VALUE)
            return fillVarInt((int)value, bytes, start);
        //4B
        if( (value & ~0x0fffffff) == 0 ){
            for(int i = 0 ; i < 3 ; i ++){
                bytes[start + i ] = (byte)((value>> (4-1-i)*7 ) | 0x80);
            }
            bytes[start + 3] = (byte)(value & 0x7f);
            return 4;
        }
        if ((value & ~0x0fffffffffL) == 0){
            for(int i = 0 ; i < 4 ; i ++){
                bytes[start + i ] = (byte)((value>> (5-1-i)*7 ) | 0x80);
            }
            bytes[start + 4] = (byte)(value & 0x7f);
            return 5;
        }
        if ((value & ~0x0fffffffffffL) == 0){
            for(int i = 0 ; i < 5 ; i ++){
                bytes[start + i ] = (byte)((value>> (6-1-i)*7 ) | 0x80);
            }
            bytes[start + 5] = (byte)(value & 0x7f);
            return 6;
        }
        //7B
        if ((value & ~0x0fffffffffffffL) == 0){
            for(int i = 0 ; i < 6 ; i ++){
                bytes[start + i ] = (byte)((value>> (7-1-i)*7 ) | 0x80);
            }
            bytes[start + 6] = (byte)(value & 0x7f);
            return 7;
        }
        if ((value & ~0x0fffffffffffffffL) == 0){
            for(int i = 0 ; i < 7 ; i ++){
                bytes[start + i ] = (byte)((value>> (8-1-i)*7 ) | 0x80);
            }
            bytes[start + 7] = (byte)(value & 0x7f);
            return 8;
        }else{
            for(int i = 0 ; i < 8 ; i ++){
                bytes[start + i ] = (byte)((value>> (9-1-i)*7 ) | 0x80);
            }
            bytes[start + 8] = (byte)(value & 0x7f);
            return 9;
        }

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
        assert(start + 4 < data.length);

        int intValue = 0;
        for(int i = 0; i< 4; i++){
            intValue |= (data[start+i]&0xFF) << 8*(4 - 1 - i) ;
        }

        return intValue;
    }

    /**
     * 加载变长int数据
     * @param data
     * @param start
     * @return
     */
    public static int loadVarIntFromBytes(byte[] data, int start){
        assert(start < data.length);

        int intValue = 0;
        // 1 B
        if((data[start] & 0x80) == 0 ){
            intValue = data[start];
            return intValue;
        }
        //2 B
        if((data[start + 1] & 0x80) == 0 ){
            intValue |= ((data[start] & 0x7f) << 7) | data[1];
            return intValue;
        }
        //3 B
        if((data[start + 2] & 0x80) == 0 ){
            intValue |= ((data[start] & 0x7f) << 14)
                        | ((data[start + 1] & 0x7f) << 7)
                        | data[start + 2];
            return intValue;
        }
        //4B
        if((data[start + 3] & 0x80) == 0 ){
            for(int i = 0; i< 4; i++){
                intValue |= (data[start+i]&0x7F) << 7*(4 - 1 - i) ;
            }
            return intValue;
        }
        //5B  need modify
        if((data[start + 4] & 0x80) == 0 ){
            for(int i = 0; i< 5; i++){
                intValue |= (data[start+i]&0x7F) << 7*(5 - 1 - i) ;
            }
            return intValue;
        }else{
            System.out.println("ERROR: Utils.loadVarIntFromBytes()");
            return -1;
        }
    }

    public static short loadShortFromBytes(byte[] data, int start){
        assert(start + 2 > data.length);

        short shortValue = 0;
        for(int i = 0; i<  2; i++){
            shortValue |= (data[start+i]&0xFF) << 8*(2 - 1 - i) ;
        }

        return shortValue;
    }

    /**
     * 加载变长short类型的整型数
     * @param data
     * @param start
     * @return
     */
    public static short loadVarShortFromBytes(byte[] data, int start){
        assert(start < data.length);

        short shortValue = 0;
        //poses 1 B
        if((data[start] & 0x80) == 0 ){
            shortValue = data[start];
            return shortValue;
        }
        //poses 2 B
        if((data[start + 1] & 0x80) == 0 ){
            shortValue |= ((data[start] & 0x7f) << 7) | data[1];
            return shortValue;
        }
        //poses 3 B
        if((data[start + 2] & 0x80) == 0 ){
            shortValue |= ((data[start] & 0x7f) << 14)
                    | ((data[start + 1] & 0x7f) << 7)
                    | data[start + 2];
            return shortValue;
        }else{
            System.out.println("ERROR: Utils.loadVarShortFromBytes()");
            return -1;
        }
    }
    /**
     * 加载变长long类型的整型数
     * @param data
     * @param start
     * @return
     */
    public static long loadLongFromBytes(byte[] data, int start){
        assert(start + 8 > data.length);

        int low = 0;
        int high = 0;
        long longValue = 0L;
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

    /**
     * @param data
     * @param start
     * @return
     */
    public static long loadVarLongFromBytes(byte[] data, int start){
        assert(start < data.length);

        Long longValue = 0L;
        //poses 1-4 B
        if((data[start] & 0x80) == 0
                || (data[start + 1] & 0x80) == 0
                || (data[start + 2] & 0x80) == 0
                || (data[start + 3] & 0x80) == 0 ){
            return loadVarIntFromBytes(data, start);
        }
        //poses 5 B
        if((data[start + 4] & 0x80) == 0){
            for(int i = 0; i< 5; i++){
                longValue |= (data[start+i]&0x7F) << 7*(5 - 1 - i) ;
            }
            return longValue;
        }
        //poses 6 B
        if((data[start + 5] & 0x80) == 0){
            for(int i = 0; i< 6; i++){
                longValue |= (data[start+i]&0x7F) << 7*(6 - 1 - i) ;
            }
            return longValue;
        }
        //poses 7 B
        if((data[start + 6] & 0x80) == 0){
            for(int i = 0; i< 7; i++){
                longValue |= (data[start+i]&0x7F) << 7*(7 - 1 - i) ;
            }
            return longValue;
        }
        //poses 8 B
        if((data[start + 7] & 0x80) == 0){
            for(int i = 0; i< 8; i++){
                longValue |= (data[start+i]&0x7F) << 7*(8 - 1 - i) ;
            }
            return longValue;
        }
        // 9B
        if((data[start + 8] & 0x80) == 0){
            for(int i = 0; i< 9; i++){
                longValue |= (data[start+i]&0x7F) << 7*(9 - 1 - i) ;
            }
            return longValue;
        }else{
            for(int i = 0; i< 10; i++){
                longValue |= (data[start+i]&0x7F) << 7*(10 - 1 - i) ;
            }
            return longValue;
        }
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
