package com.database.pager;

/**
 * Created by zoe on 2016/12/2.
 */

import com.database.global.SpaceAllocation;
import com.database.global.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * 页面对象
 */
public class Page {
    //根页面头
    private byte order;
    private int maxRowID;

    //普通页面头
    private int pgno;                                   //页号
    private byte pageType;                              //页面类型，值由PageType类定义
    private int offset;                                 //指向有数据的地方
    private int pChild;                                 //Cell中的孩子指针（页号）
    private int overflowPgno;                           //溢出页号
    private byte nCell;                                  //当前页面中cell的数量
    private List<Integer> cells;                         //Cell中：rowid

    public void setnCell(byte nCell) {
        this.nCell = nCell;
        this.data[Position.CELLNUM_IN_PAGE] = this.nCell;
    }

    //其他
    private int size;
    private byte[] data;
    //数据域。内部结点：存储页号；叶子结点：存储记录
    private int sectorSize;
    private int reserved;                               //页面保留的空间
    private int headerSize;
                                                        //系统分配的rowid

    public Page(){
        this.size = SpaceAllocation.PAGE_SIZE;
        this.data = new byte[this.size];
        this.sectorSize = SpaceAllocation.SECTOR_SIZE;
        this.reserved = SpaceAllocation.PAGE_RESERVED;
        this.headerSize = SpaceAllocation.PAGE_HEADER_SIZE;

        setCells(null);
        setPageType((byte)0);
        setpChild(0);
        setPgno(0);
        setOffset(this.size);
    }
    public void updateData(){
        toString();
        Utils.fillInt(this.pgno, this.data, Position.PGNO_IN_PAGE);
        this.data[Position.PGTYPE_IN_PAGE] = this.pageType;
        Utils.fillInt(this.offset,this.data,Position.OFFSET_IN_PAGE);
        Utils.fillInt(this.overflowPgno,this.data,Position.OVERFLOWPGNO_IN_PAGE);
        Utils.fillInt(this.pChild,this.data,Position.CHILDPAGE_IN_PAGE);
        this.data[Position.CELLNUM_IN_PAGE] = this.nCell;
        setCells(this.cells);

        this.data[Position.CELLNUM_IN_PAGE] = this.nCell;
    }

    @Override
    public String toString() {
        return "Page{" +
                " maxRowID=" + maxRowID +
                ", pgno=" + pgno +
                ", pageType=" + pageType +
                ", offset=" + offset +
                ", pChild=" + pChild +
                ", overflowPgno=" + overflowPgno +
                ", nCell=" + nCell +
                ", cells=" + cells +
                ", size=" + size +
                ", sectorSize=" + sectorSize +
                ", reserved=" + reserved +
                ", headerSize=" + headerSize +
                '}';
    }

    public byte getPageType() {
        return this.pageType;
    }


    public void setPageType(byte pageType) {
        this.data[Position.PGTYPE_IN_PAGE] = pageType;
        this.pageType =  this.data[Position.PGTYPE_IN_PAGE];
    }

    public int getpChild() {
        return this.pChild;
    }

    public void setpChild(int pChild) {
        if(pChild < 2)
            return ;

        if(this.pgno == 1){
            /**
             * 有待补充
             */
        }
        if(this.pgno > 1){
            this.pChild = pChild;
            Utils.fillInt(this.pChild,this.data,Position.CHILDPAGE_IN_PAGE);
        }
    }

    public byte getOrder() {
        return order;
    }

    /**
     * 尚待修改
     * @param order
     */
    public void setOrder(byte order) {
        this.order = order;
    }

    public byte getnCell() {
        return this.nCell;
    }

    public void setCells(List<Integer> cells) {
        if(cells == null)
           this.cells = new ArrayList<>();
        else
            this.cells = cells;
        this.nCell = (byte)this.cells.size();
        for(Integer rowid : this.cells){
            Utils.fillInt(rowid, this.data, Position.CELL_IN_PAGE);
        }
        this.data[Position.CELLNUM_IN_PAGE] = this.nCell;
    }

    /**
     * 添加cell
     * @param rowid
     */
    public void addCell(int rowid) {
        if(this.cells == null)
            this.cells = new ArrayList<>();
        this.cells.add(rowid);
        this.nCell = (byte)cells.size();

        this.data[ Position.CELLNUM_IN_PAGE] = this.nCell;
        Utils.fillInt(rowid, this.data, Position.CELL_IN_PAGE + this.nCell*4);
    }

    public int getOffset() {
        return offset;
    }

    public int getOverflowPgno() {
        return overflowPgno;
    }

    public void setOverflowPgno(int overflowPgno) {
        if(overflowPgno < 2 && this.overflowPgno == 0)
        {
            overflowPgno = 0;
            return ;
        }
        this.overflowPgno = overflowPgno;
        Utils.fillInt(this.overflowPgno,this.data,Position.OVERFLOWPGNO_IN_PAGE);
    }

    public void setOffset(int offset) {
        this.offset = offset;
        Utils.fillInt(this.offset,this.data,Position.OFFSET_IN_PAGE);

    }
    public void copyData(byte[] data)
    {
        if(data.length != this.data.length)
            return ;
        for(int i =0 ;i<data.length;i++){
            this.data[i] = data[i];
        }
    }
    /**
     *  设置指定位置数据
     * @param bytes    要添加的数据
     */
    public byte[] fillData( byte[] bytes){
        if(bytes == null || bytes.length == 0)
            return null;

        int usable = getUsable() - bytes.length;
        if(usable < 0){
            /**
             * 添加溢出页面
             */
        }else{
            if(this.pgno == 1){
                System.out.println("offset "+this.offset);
                System.out.println("bytes len:"+bytes.length);
            }
            int start = this.offset - bytes.length;
            this.data = Utils.fillBytes(bytes, this.data, start);
            setOffset((short)start);
        }
//        updateData();
        return this.data;
    }
    /**
     * @return 当前页面可用空间
     */
    public int getUsable(){
        return this.offset - this.headerSize ;
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
        if(pgno < 1)
            return ;

        this.pgno = pgno;
        Utils.fillInt(this.pgno, this.data, Position.PGNO_IN_PAGE);
    }

}
