package com.database.myBplusTree;

/**
 * Created by Qing_L on 2016/11/23.
 */
/**
 * B+树的定义：
 *
 * 1.任意非叶子结点最多有M个子节点；且M>2；M为B+树的阶数
 * 2.除根结点以外的非叶子结点至少有 (M+1)/2个子节点；
 * 3.根结点至少有2个子节点；
 * 4.除根节点外每个结点存放至少（M-1）/2和至多M-1个关键字；（至少1个关键字）
 * 5.非叶子结点的子树指针比关键字多1个；
 * 6.非叶子节点的所有key按升序存放，假设节点的关键字分别为K[0], K[1] … K[M-2],
 *  指向子女的指针分别为P[0], P[1]…P[M-1]。则有：
 *  P[0] < K[0] <= P[1] < K[1] …..< K[M-2] <= P[M-1]
 * 7.所有叶子结点位于同一层；
 * 8.为所有叶子结点增加一个链指针；
 * 9.所有关键字都在叶子结点出现
 */
import com.database.global.Database;
import com.database.global.PageType;
import com.database.pager.TableSchema;

import java.util.ArrayList;
import java.util.List;

public class BplusTree{

    Database db ;               //数据库对象
    TableSchema schema;         //表结构

    protected BplusNode root;   //根节点
    protected int order;        //树阶，M值
    protected BplusNode head;   //头指针
    protected int height = 0;   //树高

    private int maxRowid = 0;      //最大行号

    /**
     *构造函数
     * 1、判断树阶是否大于等于3
     * 2、创建根节点
     * 3、树的head指针指向根节点
     */
    public BplusTree(int order,Database db, TableSchema schema) {
        /* 创建一个新的B+树 */
        if (order < 3) {
            System.out.print("树阶必须大于2！");
            System.exit(0);
        }
        this.order = order;
        this.schema = schema;
        root = new BplusNode(db.getPager(), PageType.TABLE_LEAF, schema);
        root.page.setOrder((byte)order);
        root.page.setMaxRowID(0);			//设置目前最大rowid为0
        this.maxRowid = 0;
        head = root;
        root.page.setHead(head.page.getPgno());
        this.db = db;
    }
    public BplusTree(int order,Database db,BplusNode root,BplusNode head, TableSchema schema) {
        /* 从根节点中读取B+树，并构建 */
        if (order < 3) {
            System.out.print("树阶必须大于2！");
            System.exit(0);
        }
        this.order = order;
        this.root = root;
        this.head = head;
        this.schema = schema;
        this.maxRowid = root.page.getMaxRowID();
        this.db = db;
    }

    /** 获取/设置头指针 */
    public BplusNode getHead() {
        return head;
    }
    public void setHead(BplusNode head) {
        this.head = head;
    }

    /** 获取/设置B+树根节点 */
    public BplusNode getRoot() {
        return root;
    }
    public void setRoot(BplusNode root) {
        this.root = root;
    }

    /** 获取/设置B+树的树阶 */
    public int getOrder() {
        return order;
    }
    public void setOrder(int order) {
        this.order = order;
    }

    /** 获取/设置B+树的树高 */
    public void setHeight(int height) {
        this.height = height;
    }
    public int getHeight() {
        return height;
    }

    /* 获取rowid */
    public int getRowid(){
        maxRowid++;
        this.root.page.setMaxRowID(maxRowid);
        return maxRowid;
    }

    public String get(Integer key) {
        return root.get(key,this);
    }

    public String remove(Integer key) {
        return root.remove(key, this);
    }

    public void insertOrUpdate(Integer key, String value) {
        root.insertOrUpdate(key, value, this);
        root.flushPage( root.entries, root );
    }



    public String Remove(Integer key) {
        System.out.print("删除数据!");
        String result = remove(key);
        if (result == null) {
            return "未找到该数据";
        } else {
            return "删除数据为："+result;
        }
    }

    /**
     * 根据rowid查询
     */
    public String SelectByKey(Integer key) {
        System.out.println("开始查询！");
        String result = get(key);

        return result;
    }

    /**
     * 查询所有数据
     * 1、从头指针开始遍历
     */
    public List<String> SelectAll() {
        System.out.println("开始查询！");

        List<String> results = new ArrayList<>();

        BplusNode tempNode;
        if(head.page.getPgno() != 0){

            tempNode = new BplusNode(db.getPager(),db.getPager().aquirePage(head.page.getPgno()), schema);

            while(tempNode != null){
                for(int i=0;i < tempNode.entries.size();i++){
                    String tempStr = tempNode.entries.get(i).getValue();
                    results.add(tempStr);
                }// end for
                if(tempNode.page.getpNext() != 0){
                    tempNode = new BplusNode(db.getPager(),db.getPager().aquirePage(tempNode.page.getpNext()), schema);
                }else{
                    tempNode = null;
                }
            }//end while
        }

        return results;
    }

    /**
     * 查询其他字段
     */
    public List<String> SelectByOther(String param, String value){
        System.out.println("开始查询！");

        List<String> results = new ArrayList<>();
        BplusNode tempNode = new BplusNode(db.getPager(),db.getPager().aquirePage(head.page.getpNext()), schema);
        while(tempNode != null){
            for(int i=0;i < tempNode.entries.size();i++){
                String tempStr = tempNode.entries.get(i).getValue();
                String tempValue[] = tempStr.split(",");
                if(tempValue[1].equals(value)){
                    results.add(tempStr);
                }
            }// end for
            tempNode = new BplusNode(db.getPager(),db.getPager().aquirePage(head.page.getpNext()), schema);
        }// end while

        return results;
    }

    /**
     * 插入数据
     * 1、获取rowid，执行插入
     * 2、rowid自增
     * @param value
     */
    public void Insert(String value) {
        System.out.println("开始插入!");
        insertOrUpdate(getRowid(), value);
    }

}
