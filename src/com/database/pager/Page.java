package com.database.pager;

/**
 * Created by zoe on 2016/12/2.
 */

import com.database.global.PageType;
import com.database.global.SpaceAllocation;
import com.database.global.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * 页面对象
 */
public class Page {
    //page 1 头
    private  int tableCount;

    //普通页面头
    private int pgno;                                   //页号
    private byte pageType;                              //页面类型，值由PageType类定义
    private int offset;                                 //指向有数据的地方
    private int pParent;                                //父节点 pgno
    private int pPrev;                                  //前一个节点 pgno
    private int pNext;                                  //后一个节点 pgno
    private int overflowPgno;                           //溢出页号
    private byte nCell;                                 //当前页面中cell的数量
    private List<Integer> cells;                        //Cell中：rowid
    //B+树根页头
    private int head ;                                  //
    private byte order;                                 //
    private int maxRowID;                               //

    //其他
    private int size;
    private byte[] data;
    private boolean dirty;
    private short nRef;

    //数据域。内部结点：存储页号；叶子结点：存储记录
    private int sectorSize;
    private int reserved;                               //页面保留的空间
    private int headerSize;

    public Page(){
        init();
    }
    private void init(){
        this.size = SpaceAllocation.PAGE_SIZE;
        this.data = new byte[this.size];
        this.sectorSize = SpaceAllocation.SECTOR_SIZE;
        this.reserved = SpaceAllocation.PAGE_RESERVED;
        this.headerSize = SpaceAllocation.PAGE_HEADER_SIZE;
        this.pageType = PageType.TABLE_LEAF;
        setCells(null);
        setPageType((byte)0);
        setPgno(0);
        setOffset(this.size);
    }

    //————————————————属性的getter setter————————————————
    public void setnCell(byte nCell) {
        this.nCell = nCell;
        this.data[Position.CELLNUM_IN_PAGE] = this.nCell;
    }

    public int getMaxRowID() {
        return maxRowID;
    }

    public void setMaxRowID(int maxRowID) {
        if(this.pgno ==1 )
            return;

        this.maxRowID = maxRowID;
        Utils.fillInt(this.maxRowID,this.data,Position.MAX_ROWID_IN_BPLIS_ROOT);
    }

    public int getpParent() {
        return pParent;
    }

    public void setpParent(int pParent) {
//        if(pParent < 2)
//            return ;

        if(this.pParent == 1){
            /**
             * 有待补充
             */
        }
        if(this.pParent > 1){
            this.pParent = pParent;
            Utils.fillInt(this.pParent,this.data,Position.PARENT_PAGE_IN_PAGE);
        }
    }

    public int getTableCount() {
        return tableCount;
    }

    public void setTableCount(int tableCount) {
        if(this.pgno != 1)
            return ;

        this.tableCount = tableCount;
        Utils.fillInt(this.tableCount,this.data,Position.TABLE_COUNT_IN_FIRST_PAGE);
    }

    public int getHead() {
        return head;
    }

    public void setHead(int head) {
        if(this.pgno == 1)
            return ;

        if(head < 2)
            return ;

        if(this.head == 1){
            /**
             * 有待补充
             */
        }
        if(this.head > 1){
            this.head = head;
            Utils.fillInt(this.head,this.data,Position.HEAD_IN_BPLUS_ROOT);
        }
    }

    public int getpPrev() {
        return pPrev;
    }

    /**
     * 重置页面内容，全部填充为0，再重新初始化内容
     */
    public void reset(){
        Arrays.fill(this.data, (byte)0);
        init();
    }
    public void setpPrev(int pPrev) {
        if(this.pgno == 1){
            /**
             * 有待补充
             */
        }
        if(this.pgno > 1){
            this.pPrev = pPrev;
            Utils.fillInt(this.pPrev,this.data,Position.PREV_PAGE_IN_PAGE);
        }
    }

    public int getpNext() {
        return pNext;
    }

    public void setpNext(int pNext) {
        if(this.pgno == 1){
            /**
             * 有待补充
             */
        }
        if(this.pgno > 1){
            this.pNext = pNext;
            Utils.fillInt(this.pNext,this.data,Position.NEXT_PAGE_IN_PAGE);
        }
    }

    @Override
    public String toString() {
        return "Page{" +
                "order=" + order +
                ", pgno=" + pgno +
                ", pageType=" + pageType +
                ", offset=" + offset +
                ", pParent=" + pParent +
                ", pPrev=" + pPrev +
                ", pNext=" + pNext +
                ", overflowPgno=" + overflowPgno +
                ", nCell=" + nCell +
                ", cells=" + cells +
                ", size=" + size +
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


    public byte getOrder() {
        return order;
    }

    /**
     * 尚待修改
     * @param order
     */
    public void setOrder(byte order) {
        this.data[Position.ORDER_IN_BPLUS_ROOT] = order;
        this.order =  this.data[Position.ORDER_IN_BPLUS_ROOT];
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

    /**
     * 将参数内容拷贝到当前page对象的data中，当参数长度>页面大小，不做任何改变
     * @param data
     */
    public void copyData(byte[] data)
    {
        if(data.length > this.size)
            return ;

        if(data.length == this.size)
        {
            for(int i =0 ; i < this.data.length;i++){
                this.data[i] = data[i];
            }
        }else{
            for(int i =0 ; i < data.length;i++){
                this.data[i] = data[i];
            }
            for(int i = data.length ; i < this.data.length;i++){
                this.data[i] = (byte) 0;
            }
        }

    }

    /**
     *
     * @param entryList
     */
    public void fillData(List<Map.Entry<Integer, byte[]>> entryList){
        if(entryList == null || entryList.size() == 0)
            return ;
        Map.Entry<Integer, byte[]> entry = null;
        int usable = getUsable() ;
        List<Integer> rowidList = new ArrayList<Integer>(entryList.size());
        int offset = this.size;
        for(int i = 0;i < entryList.size(); i++){
            entry = entryList.get(i);
            rowidList.add(entry.getKey());
            byte[] data = entry.getValue();
            Utils.fillBytes(data, this.data, offset - entry.getValue().length);
            usable -= entry.getValue().length;
            offset -= entry.getValue().length;
            if(usable < 0){
                /**
                 * 添加溢出页面
                 */
            }
        }
        setCells(rowidList);
        setOffset((short)offset);
    }

    /**
     *  设置指定位置追加数据（有Bug，追加数据后rowid为追加）
     * @param entry 要添加的数据
     */
    public byte[] appendData( Map.Entry<Integer, byte[]> entry){
        int rowid = entry.getKey();
        byte[] bytes = entry.getValue();

        if(bytes == null || bytes.length == 0)
            return null;
        List<Integer> rowidList = new ArrayList();
        rowidList.add(rowid);
        int usable = getUsable() - bytes.length;
        if(usable < 0){
            /**
             * 添加溢出页面
             */
        }else{
            int start = this.offset - bytes.length;
            this.data = Utils.fillBytes(bytes, this.data, start);
        }
        setCells(rowidList);
        setOffset((short)this.offset - bytes.length);
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
