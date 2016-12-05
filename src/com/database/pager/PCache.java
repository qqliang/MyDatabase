package com.database.pager;

public class PCache{
    private int pageRefNum;
    private int szExtra;
    private Page[] pages;

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