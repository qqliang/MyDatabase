package com.database.pager;

import com.database.global.SpaceAllocation;

import java.util.*;

public class PCache{
    private Page[] cacheSpace;
    private List<Page> dirtyPgs;
    private List<Page> lruList;
    private List<Page> freePgs;
    private List<List<Page>> apHash;        //维护使用页面的哈希表
    private int nHash;
    private int cacheSize ;                 //分配空间的大小
    private int nMaxPage;                   //最大页面
    private int nMinPage;                   //最小页面
    private int maxPinned;                  //最大pinned page
    private int n90pct;                     //缓存数量的百分之九十
    private int maxPgno;                    //cached page 最大页号
    private int nCachedPage;
    private int nRecyclable;
    private boolean underPressure;

    public PCache() {
        initSpace();

        this.maxPinned = 10;
        this.nMinPage = 2;
        this.n90pct = this.cacheSize*9/10;
        this.freePgs = new ArrayList<Page>();
        this.freePgs.addAll(Arrays.asList(this.cacheSpace));
        this.dirtyPgs = new ArrayList<Page>();
        this.lruList = new ArrayList<Page>();
        this.nMaxPage = this.cacheSize;
        this.underPressure = this.freePgs.size() < this.nMinPage;
        this.nRecyclable = this.lruList.size();
    }

    /**
     * 空间初始化
     */
    public void initSpace(){
        this.cacheSize = 100;
        this.cacheSpace = new Page[cacheSize];
        for(int i =0 ;i <this.cacheSpace.length; i++){
            Page page = new Page();
            page.setData(new byte[SpaceAllocation.PAGE_SIZE]);
            page.setPgno(i);
            page.setOverflowPgno(0);
            page.setSectorSize(SpaceAllocation.SECTOR_SIZE);
            page.setSize(SpaceAllocation.PAGE_SIZE);
            page.setOffset(SpaceAllocation.PAGE_SIZE);
            cacheSpace[i] = page;
        }
        this.nHash = 10;
        this.apHash = new ArrayList<List<Page>>(this.nHash);
        for(int i = 0; i< this.nHash; i++){
            this.apHash.add(new ArrayList<Page>());
        }
    }

    /**
     * 释放一个页面
     */
    public void free(Page page){
        if(freePgs.contains(page)){
            return;
        }
        if(this.nCachedPage > this.nMaxPage){
            removeFromHash(page);
            freePage(page);
        }else{
            this.lruList.add(page);
            this.nRecyclable++;
        }
    }
    /**
     * 释放一个页面
     */
    public void free(int pgno){
        /**
         * 日后添加是否创建标记
         */
        Page page = fetch(pgno);

        free(page);
    }



    /**
     * 试图获取指定页号的页面,如果没有获取到，为其分配一个新页面（页号为0）
     * @param pgno 要获取的页号
     * @return 获取到的页面，或者重新利用/分配的页面（pgno为0）
     */
    public Page fetch(int pgno){        //日后添加是否创建标记
        Page page = null;
        //step 1 查找页面
        int key = getHashKey(pgno);
        ArrayList<Page> listInHash = ((ArrayList<Page>)this.apHash.get(key));

        for(int i = 0; i < listInHash.size(); page = listInHash.get(i)){
            Page temp = listInHash.get(i);
            if(temp != null && page.getPgno() == pgno){
                page = temp;
                break;
            }
        }
        if(page != null){
            pin(page);
            return page;
        }

        //step 2 get from freeList
        if(this.freePgs != null && this.freePgs.size() > 0){
            page = this.freePgs.remove(0);
            page.reset();
            return page;
        }

        //step 3 recycle a page
        if((this.nCachedPage+1) >= this.nMaxPage && lruList != null && lruList.size() > 0){
            page = lruList.remove(lruList.size()-1);
            removeFromHash(page);
            pin(page);
            page.setPgno(0);
            return page;
        }

        //step 3 allocate a new one， pgno is 0
        page = allocPage();

        if(page != null ){
            this.nCachedPage++;
            ((ArrayList)this.apHash.get(getHashKey(pgno))).add(page);
        }
        return page;
    }
    public boolean isDirtyPage(Page page){
        if(this.dirtyPgs == null || this.dirtyPgs.size()==0)
            return false;
        if(this.dirtyPgs.contains(page))
            return true;
        else
            return false;
    }
    public void printStatus(){
        System.out.println("freeList: " + this.freePgs
                + ", apHash: " + this.apHash
                + ", lruList: " + this.lruList
                + ", dirtyList: " + this.dirtyPgs
        );
    }
    /**
     * 为当前对象分配一个新的页面
     * @return 新分配的页面
     */
    public Page allocPage(){
        return new Page();
    }

    /**
     * 日后添加
     * @param nMaxPage
     */
    public void setCacheSize(int nMaxPage){
    }
    /**
     * @return cache中页面总数
     */
    public int getPageCount(){
        return this.nCachedPage;
    }

    /**
     *
     * @param page 对page 进行pin操作
     */
    public void  pin(Page page){
        if(page == null)
            return;
        this.lruList.remove(page);
        this.nRecyclable--;
    }

    /**
     *  对应于pcache1Rekey
     */
    public Page rekey(Page page,int newPgno){
        int oldPgno = page.getPgno();

        if(oldPgno == newPgno)
            return page;

        List<Page> cachedPageList = this.apHash.get(getHashKey(oldPgno));
        cachedPageList.remove(page);

        page.setPgno(newPgno);
        this.apHash.get(getHashKey(newPgno)).add(page);

        return page;
    }
    public void unpin(Page page){

    }
    public List<Page> getDirtyPgs() {
        return dirtyPgs;
    }

    /**
     * 如果存放页面的空间不够，对其进行扩容
     */
    private void resizePages()
    {
//        int oldPageNum = this.pageNum;
//        Page[] newPages = new Page[oldPageNum * 2];
//        for(int i = 0; i< oldPageNum; i++){
//            newPages[i] = this.pages[i];
//        }
//        for(int i = oldPageNum; i < newPages.length; i++){
//            Page page = new Page();
//            page.setData(new byte[SpaceAllocation.PAGE_SIZE]);
//            page.setPgno(i);
//            page.setSectorSize(SpaceAllocation.SECTOR_SIZE);
//            page.setSize(SpaceAllocation.PAGE_SIZE);
//            page.setOffset(SpaceAllocation.PAGE_SIZE);
//            newPages[i] = page;
//        }
//
//        this.pages = newPages;
//        this.pageNum = newPages.length;
    }
    /**
     * 日后添加
     */
    public void shrink(){

    }
    /**
     * 日后添加
     */
    private void enforceMaxPage(){

    }
    /**
     * 日后添加
     */
    private void truncate(int limit){

    }

    public void makeDirty(Page page){
        addDirtyPg(page);
    }
    private void addDirtyPg(Page dirtyPage){
        if(this.dirtyPgs!=null && !this.dirtyPgs.contains(dirtyPage)){
            this.dirtyPgs.add(dirtyPage);
        }
    }
    /**
     * 日后添加
     */
    public void releaseMemory(int nByte){

    }
    public List<Page> getFreePgs() {
        return freePgs;
    }
    private void removeFromHash(Page page){
        int key = getHashKey(page.getPgno());
        List<Page> cached = this.apHash.get(key);
        cached.remove(page);
        this.nCachedPage -- ;
    }

    public void setFreePgs(List<Page> freePgs) {
        this.freePgs = freePgs;
    }
    /**
     * 日后添加
     */
    public void makeClean(Page page){

    }
    private int getHashKey(int pgno){
        return pgno % this.apHash.size();
    }

    /**
     * 释放页面缓存
     * @param page
     */
    private void freePage(Page page){
        if(allSpaceContains(page)){
            this.freePgs.add(page);
            this.underPressure = this.freePgs.size() < this.nMinPage;
            this.nCachedPage --;
        }else{
            page = null;
        }
    }

    /**
     * 判断页面是否是最初分配
     * @param page
     * @return
     */
    public boolean allSpaceContains(Page page){
        boolean contains = false;
        for(int i =0; i< this.cacheSize; i++){
            if(this.cacheSpace[i] == page)
                return true;
        }
        return contains;
    }

}