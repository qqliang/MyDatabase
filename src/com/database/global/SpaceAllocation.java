package com.database.global;

/**
 * Created by zoe on 2016/12/2.
 */
public class SpaceAllocation {
    public static int PAGE_SIZE = 1024;
    public static int SECTOR_SIZE = 512;
    public static int PAGE_RESERVED = 0;
    public static int PAGE_HEADER_SIZE = 50;
    public static int RECORD_HEADER = 4+4;      //rowid，hdrsz
}
