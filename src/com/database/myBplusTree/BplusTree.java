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
import com.database.global.Databases;
import com.database.pager.Pager;

import java.util.*;

public class BplusTree{

    Databases db ;

    /** 根节点 */
    protected BplusNode root;

    /** 阶数，M值 */
    protected int order;

    /** 叶子节点的链表头 */
    protected BplusNode head;

    /** 树高*/
    protected int height = 0;

    public BplusNode getHead() {
        return head;
    }

    public void setHead(BplusNode head) {
        this.head = head;
    }

    public BplusNode getRoot() {
        return root;
    }

    public void setRoot(BplusNode root) {
        this.root = root;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public void setHeight(int height) {
        this.height = height;
    }

    public int getHeight() {
        return height;
    }

    public String get(Integer key) {
        return root.get(key);
    }

    public String remove(Integer key) {
        return root.remove(key, this);
    }

    public void insertOrUpdate(Integer key, String value) {
        root.insertOrUpdate(key, value, this);

    }

    public BplusTree(int order,Databases db) {
        if (order < 3) {
            System.out.print("order must be greater than 2");
            System.exit(0);
        }
        this.order = order;
        root = new BplusNode(true, true);
        head = root;
        this.db = db;
    }

    public String Remove(Integer key) {
        System.out.print("开始删除!");
        String result = remove(key);
        if (result == null) {
            return "未找到对象";
        } else {
            return "删除对象："+result;
        }


    }

    public String Search(Integer key) {
        System.out.println("开始查询");
        String result = get(key);
        if (result == null) {
            return "未找到对象";
        }else{
            return result;
        }
    }

    public void Insert(Integer key,String value) {
        System.out.println("开始插入!");

        insertOrUpdate(key, value);

        Pager pager = new Pager();
        BplusNode node = getHead();
        while (node!=null){
            pager.writeTable(db.getDBFile(), "test",node.entries.toString());
            node = node.next;
        }
    }
}
