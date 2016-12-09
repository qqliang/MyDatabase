package com.database.pager;

import java.util.List;

public class PCache{
    private int pageRefNum;
    private int szExtra;
    private Page[] pages;
    private List<Page> dirtyPgs;
    private List<Page> freePgs;
    private List<Page>[] apHash;        //保存

    // 从数据库中读入的页面
    private int nHash;
    private boolean purgeable;

    public PCache() {
    }

    public Page[] getPages() {
        return pages;
    }

    public void setPages(Page[] pages) {
        this.pages = pages;
    }

    public List<Page> getDirtyPgs() {
        return dirtyPgs;
    }

    public void setDirtyPgs(List<Page> dirtyPgs) {
        this.dirtyPgs = dirtyPgs;
    }

    public List<Page> getFreePgs() {
        return freePgs;
    }

    public void setFreePgs(List<Page> freePgs) {
        this.freePgs = freePgs;
    }

    public boolean isPurgeable() {
        return purgeable;
    }

    public void setPurgeable(boolean purgeable) {
        this.purgeable = purgeable;
    }

    public int getPageRefNum() {
        return pageRefNum;
    }

    public void setPageRefNum(int pageRefNum) {
        this.pageRefNum = pageRefNum;
    }

    public int getSzExtra() {
        return szExtra;
    }

    public void setSzExtra(int szExtra) {
        this.szExtra = szExtra;
    }

    public void makeClean(Page page){

    }

}