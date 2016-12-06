package com.database.pager;

/**
 * Created by zoe on 2016/12/2.
 */

import com.database.global.SpaceAllocation;
import com.database.global.Utils;


/**
 * 页面对象
 */
public class Page {
    private int pgno;            //页号
    private int offset;         //指向有数据的地方
    private int overflowPgno;
    private boolean hasOverflow;
    private boolean isOverflow;

    private Page overflow;      //溢出页面
    private int size;
    private byte[] data;
    private int sectorSize;
    private int reserved;
    private int headerSize;
    private int[] rowID;          //系统分配的rowid

    public Page(){
        this.size = SpaceAllocation.PAGE_SIZE;
        this.sectorSize = SpaceAllocation.SECTOR_SIZE;
        this.data = new byte[this.size];
        this.pgno = 0;
        this.overflow = null;
//        this.reserved = SpaceAllocation.PAGE_RESERVED;
        this.offset = this.size;
        this.headerSize = SpaceAllocation.PAGE_HEADER_SIZE;
        this.isOverflow = false;
        this.hasOverflow = false;
    }


    public int getOffset() {
        return offset;
    }

    public int getOverflowPgno() {
        return overflowPgno;
    }

    public boolean isHasOverflow() {
        return hasOverflow;
    }

    public void setHasOverflow(boolean hasOverflow) {
        this.hasOverflow = hasOverflow;
    }

    public boolean isOverflow() {
        return isOverflow;
    }

    public void setOverflow(boolean overflow) {
        isOverflow = overflow;
    }

    public void setOverflowPgno(int overflowPgno) {
        this.overflowPgno = overflowPgno;
    }



    public void setOffset(int offset) {
        this.offset = offset;
    }

    /**
     *  设置指定位置数据
     * @param bytes    要添加的数据
     */
    public byte[] fillData(byte[] bytes){
        if(bytes == null || bytes.length == 0)
            return null;

        int usable = getUsable() - bytes.length;
        if(usable < 0){
            /**
             * 添加溢出页面
             */
        }else{
            int start = this.offset - bytes.length;
            this.data = Utils.fillBytes(bytes, this.data, start);
            this.offset =  start;

            this.data = Utils.fillShort((short)offset, this.data,0);
            this.data[4] = this.isOverflow?(byte)1:0;
            this.data[5] = this.isHasOverflow()?(byte)1:0;
            this.data = Utils.fillInt(this.overflowPgno,this.data,6);
        }
        return this.data;
    }

    public void fillRoot(byte[] header, int start){
        assert(start + header.length <= this.headerSize);

       this.data =  Utils.fillBytes(header, this.data, start);
    }
    /**
     * @return 当前页面可用空间
     */
    public int getUsable(){
        return this.offset - this.headerSize ;
    }
    public Page getOverflow() {
        return overflow;
    }

    public void setOverflowPage(Page overflow) {
        this.overflow = overflow;
    }

    public int getSectorSize() {
        return sectorSize;
    }

    public void setSectorSize(int sectorSize) {
        this.sectorSize = sectorSize;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public byte[] getData() {
        return data;
    }
    public void setData(byte[] data) {
        this.data = data;
    }

    public int getPgno() {
        return pgno;
    }

    public void setPgno(int pgno) {
        this.pgno = pgno;
    }

}
