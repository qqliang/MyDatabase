package com.database.pager;

/**
 * Created by zoe on 2016/12/8.
 */
public class Position {
    public static int PGNO_IN_PAGE = 0 ;
    public static byte PGTYPE_IN_PAGE = 4;
    public static int OFFSET_IN_PAGE = 5;
    public static int OVERFLOWPGNO_IN_PAGE = 9;
    public static int CHILDPAGE_IN_PAGE = 13;
    public static byte CELLNUM_IN_PAGE = 17;
    public static int CELL_IN_PAGE = 18;

    public static int ROWID_IN_RECORD = 0;
    public static int HEADER_IN_RECORD = 4;
}
