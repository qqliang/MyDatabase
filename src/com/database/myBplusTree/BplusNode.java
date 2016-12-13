package com.database.myBplusTree;

/**
 * Created by Qing_L on 2016/11/23.
 */
import com.database.global.ColumnConstraint;
import com.database.global.DataType;
import com.database.global.PageType;
import com.database.pager.Column;
import com.database.pager.Page;
import com.database.pager.Pager;
import com.database.pager.TableSchema;

import java.util.*;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

public class BplusNode {

    Pager pager ;
    public Page page;
    TableSchema schema;

    private BplusNode parent;         //父节点
    private BplusNode previous;       //叶子节点的前一个节点
    private BplusNode next;           //叶子节点的下一个节点
    private List<BplusNode> children; //孩子节点

    /*记录信息：rowid->data。若为内部节点，data为孩子节点的页号，若为叶子节点，data为一条记录*/
    public List<Entry<Integer, String>> entries;

    /**
     * 构造函数
     */
    public BplusNode(Pager pager, byte type) {
        /* 构造一个新的节点，向pager请求分配一个新的页面，并设置页面类型 */
        this.pager = pager;
        this.page = pager.newPage();
        this.page.setPageType(type);
        this.children = new ArrayList<>();

        entries = new ArrayList<Entry<Integer, String>>();
    }

    public BplusNode(Pager pager, Page page) {
        /* 从page中读取内容，构造一个节点 */
        this.pager = pager;
        this.page = page;
        this.entries = pager.readRecord(page.getPgno());
        Collections.reverse(this.entries);
        this.children = new ArrayList<>();
    }

    /**
     * 根据关键字，查找数据（采用二分查找）
     */
    public String get(Integer key) {
        //如果是叶子节点
        if (page.getPageType() == PageType.TABLE_LEAF) {
            int low = 0, high = entries.size() - 1, mid;
            int comp ;
            while (low <= high) {
                mid = (low + high) / 2;
                comp = entries.get(mid).getKey().compareTo(key);
                if (comp == 0) {
                    return entries.get(mid).getValue();
                } else if (comp < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            //未找到所要查询的对象
            return null;
        }
        /**
         * 如果不是叶子节点
         * 1、清空子节点；并从页面中加载子节点
         * 2、如果key小于节点最左边的key，沿第一个子节点继续搜索
         * 3、如果key大于等于节点最右边的key，沿最后一个子节点继续搜索
         * 4、否则沿比key大的前一个子节点继续搜索
         */
        loadChildren(this);
        if (key.compareTo(entries.get(0).getKey()) < 0) {
            return children.get(0).get(key);
        }else if (key.compareTo(entries.get(entries.size()-1).getKey()) >= 0) {
            return children.get(children.size()-1).get(key);
        }else {
            int low = 0, high = entries.size() - 1, mid= 0;
            int comp ;
            while (low <= high) {
                mid = (low + high) / 2;
                comp = entries.get(mid).getKey().compareTo(key);
                if (comp == 0) {
                    return children.get(mid+1).get(key);
                } else if (comp < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            return children.get(low).get(key);
        }
    }

    /* 加载子节点 */
    public void loadChildren(BplusNode node){
        node.children.clear();
        for(int i=0;i<node.entries.size();i++){
            Integer pgno = Integer.parseInt(node.entries.get(i).getValue());
            BplusNode temp = new BplusNode(pager,pager.aquirePage(pgno));
            node.children.add(temp);
        }
    }

    public void insertOrUpdate(Integer key, String value, BplusTree tree){
        /* 如果是叶子节点 */
        if (page.getPageType() == PageType.TABLE_LEAF){
            /**
             * 不需要分裂的情况：
             * 1、节点中包含该关键字，则直接修改数据域
             * 2、节点中不包含该关键字，而该节点未满，则插入数据
             */
            if (contains(key) != -1 || entries.size() < tree.getOrder()){
                insertOrUpdate(key, value);
                if(tree.getHeight() == 0){
                    tree.setHeight(1);
                }
                return ;
            }
            /**
             * 需要分裂的情况：节点不包含该关键字，且该节点已满，则分裂节点
             */
            /**
             * 设置链接
             * 1、创建两个新的节点：left、right；
             * 2、若前一个节点不为空：
             *      1)把前一个节点的next指针指向left
             *      2)把left的previous指针指向前一个节点
             * 3、若后一个节点不为空：
             *      1)把后一个节点的previous指针指向right
             *      2)把right的next指针指向后一个节点
             * 4、left的next指针指向right，right的previous指针指向left
             * 5、把当前节点的previous、next指针赋值为NULL
             */
            BplusNode left = new BplusNode(pager,PageType.TABLE_LEAF);
            BplusNode right = new BplusNode(pager,PageType.TABLE_LEAF);

            if (page.getpPrev() != 0){
                if(previous == null)
                    previous = new BplusNode(pager,pager.aquirePage(page.getpPrev()));

                previous.next = left;
                previous.page.setpNext(left.page.getPgno());

                left.previous = previous ;
                left.page.setpPrev(previous.page.getPgno());
            }else{
                tree.setHead(left);
            }
            if (page.getpNext() != 0) {
                next = new BplusNode(pager, pager.aquirePage(page.getpNext()));

                next.previous = right;
                next.page.setpPrev(right.page.getPgno());

                right.next = next;
                right.page.setpNext(next.page.getPgno());
            }
            left.next = right;
            left.page.setpNext(right.page.getPgno());

            right.previous = left;
            right.page.setpPrev(left.page.getPgno());

            previous = null;
            page.setpPrev(0);

            next = null;
            page.setpNext(0);

            /**
             * 将当前节点的entry分给left和right节点
             */
            copy2Nodes(key, value, left, right, tree);

            /**
             * 调整当前节点的父子关系指针
             * 若当前节点不是根节点
             *      1)将父节点指向当前节点的children指针移除
             *      2)left的parent指针指向父节点
             *      3)right的parent指针指向父节点
             *      4)父节点的children指针添加left和right
             *      5)父节点的entries添加right的第0条记录。（left的第0条记录，父节点中已有）
             *      6)删除当前节点的entries记录和children指针
             *      7)更新父节点的关键字
             *      8)删除当前节点的parent指针
             * 若当前节点是根节点
             *      1)新建一个parent节点作为根节点
             *      2)树的根节点设置为parent节点
             *      3)left和right节点的parent指针指向parent节点
             *      4)parent节点的子节点指针添加left和right节点
             *      5)parent节点的entries记录赋值为当前节点的entries
             *      6)清空当前节点的指针和记录
             */
            if (page.getpParent() != 0) {
                //调整父子节点关系
                if(parent == null){
                    parent = new BplusNode(pager,pager.aquirePage(page.getpParent()));
                    loadChildren(parent);
                }
                int index = parent.children.indexOf(this);
                parent.children.remove(this);

                left.parent = parent;
                left.page.setpParent(page.getpParent());

                right.parent = parent;
                right.page.setpParent(page.getpParent());

                parent.children.add(index,left);
                parent.children.add(index + 1, right);

                parent.entries.set(index,new SimpleEntry<Integer, String>(
                        left.entries.get(0).getKey(),String.valueOf(left.page.getPgno())));
                parent.entries.add(index+1,new SimpleEntry<Integer, String>(
                        right.entries.get(0).getKey(),String.valueOf(right.page.getPgno())));

                //父节点插入或更新关键字
                parent.updateInsert(tree);

                page.setPageType((byte)0);
                pager.freePage(page.getPgno());
                //如果是根节点
            }else {
                page.setPageType(PageType.TABLE_ROOT);

                left.parent = this;
                left.page.setpParent(page.getPgno());
                right.parent = this;
                right.page.setpParent(page.getPgno());

                children.clear();
                this.children.add(left);
                this.children.add(right);

                entries.clear();
                entries.add(new SimpleEntry<Integer, String>(
                        left.entries.get(0).getKey(),String.valueOf(left.page.getPgno())));
                entries.add(new SimpleEntry<Integer, String>(
                        right.entries.get(0).getKey(),String.valueOf(right.page.getPgno())));
                flushPage(entries,this);
            }
            return ;
        }
        /**
         * 如果不是叶子节点
         * 1、如果key小于等于节点最左边的key，沿第一个子节点继续搜索
         * 2、如果key大于节点最右边的key，沿最后一个子节点继续搜索
         * 3、否则沿比key大的前一个子节点继续搜索
         */
        if (key.compareTo(entries.get(0).getKey()) < 0) {
            children.get(0).insertOrUpdate(key, value, tree);
        }else if (key.compareTo(entries.get(entries.size()-1).getKey()) >= 0) {
            children.get(children.size()-1).insertOrUpdate(key, value, tree);
        }else {
            int low = 0, high = entries.size() - 1, mid= 0;
            int comp ;
            while (low <= high) {
                mid = (low + high) / 2;
                comp = entries.get(mid).getKey().compareTo(key);
                if (comp == 0) {
                    children.get(mid+1).insertOrUpdate(key, value, tree);
                    break;
                } else if (comp < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            if(low>high){
                children.get(low).insertOrUpdate(key, value, tree);
            }
        }
    }

    /** 将当前节点的entry记录分给left和right节点 */
    private void copy2Nodes(Integer key, String value, BplusNode left, BplusNode right,BplusTree tree) {
        //左右两个节点关键字长度
        int leftSize = (tree.getOrder() + 1) / 2 + (tree.getOrder() + 1) % 2;
        boolean b = false;//用于记录新元素是否已经被插入
        for (int i = 0; i < entries.size(); i++) {
            if(leftSize !=0){
                leftSize --;
                if(!b&&entries.get(i).getKey().compareTo(key) > 0){
                    left.entries.add(new SimpleEntry<Integer, String>(key, value));
                    b = true;
                    i--;
                }else {
                    left.entries.add(entries.get(i));
                }
            }else {
                if(!b&&entries.get(i).getKey().compareTo(key) > 0){
                    right.entries.add(new SimpleEntry<Integer, String>(key, value));
                    b = true;
                    i--;
                }else {
                    right.entries.add(entries.get(i));
                }
            }
        }
        if(!b){
            right.entries.add(new SimpleEntry<Integer, String>(key, value));
        }

        flushPage(left.entries,left);       /* 刷新左节点页面数据域 */
        flushPage(right.entries,right);     /* 刷新右节点页面数据域 */
    }

    /** 插入节点后中间节点的更新 */
    protected void updateInsert(BplusTree tree){

        /**
         * 如果子节点数超出阶数，则需要分裂该节点
         *      1)分裂成左右两个节点：left、right
         *      2)计算left和right节点的子节点的长度
         *      3)将当前节点的子节点赋值给left和right，并把子节点的parent指针指向left和right
         *      4)把当前节点的entries记录赋值给left和right的entries记录
         *如果不是根节点
         *      1)将当前节点从父节点的children指针中移除
         *      2)left和right节点的parent指针指向父节点
         *      3)父节点的children指针添加left和right节点
         *      4)父节点的entries记录添加right节点的第0条entries记录
         *      6)更新父节点的记录和指针
         *      7)清空当前节点的指针和记录
         *如果是根节点
         *      1)新建一个parent节点作为根节点
         *      2)设置树的根节点为parent节点，并将树高+1
         *      3)left和right节点的parent指针指向parent节点
         *      4)parent节点的子节点指针添加left和right节点
         *      5)parent节点的entries记录赋值为当前节点的entries
         *      6)清空当前节点的指针和记录
         */
        if (entries.size() > tree.getOrder()) {
            //分裂成左右两个节点
            BplusNode left = new BplusNode(pager,PageType.TABLE_INTERNAL);
            BplusNode right = new BplusNode(pager,PageType.TABLE_INTERNAL);

            /* 计算left和right节点子节点的长度 */
            int leftSize = (tree.getOrder() + 1) / 2 + (tree.getOrder() + 1) % 2;
            int rightSize = (tree.getOrder() + 1) / 2;

            /* 复制记录信息和孩子指针给left和right节点 */
            for (int i = 0; i < leftSize; i++) {
                left.entries.add(entries.get(i));
                left.children.add(children.get(i));
            }
            for (int i = 0; i < rightSize; i++) {
                right.entries.add(entries.get(leftSize+i));
                right.children.add(children.get(leftSize+i));
            }

            //如果不是根节点
            if (page.getpParent() != 0) {
                //调整父子节点关系
                Page p = pager.aquirePage(page.getpParent());
                BplusNode parent = new BplusNode(pager, p);
                left.page.setpParent(page.getpParent());
                right.page.setpParent(page.getpParent());
                parent.entries.add(entries.get(leftSize - 1));
                entries = null;

                //父节点更新关键字
                parent.updateInsert(tree);
                parent = null;

            }else {
                /* 如果是根节点 */
                tree.setHeight(tree.getHeight() + 1);
                page.setPageType(PageType.TABLE_ROOT);

                /* 调整left和right节点的parent指针 */
                left.parent = this;
                left.page.setpParent(page.getPgno());
                right.parent = this;
                right.page.setpParent(page.getPgno());

                /* 将left和right节点添加至父节点的孩子指针 */
                this.children.clear();
                this.children.add(left);
                this.children.add(right);

                this.entries.clear();
                this.entries.add(new SimpleEntry<Integer, String>(
                        left.entries.get(0).getKey(),String.valueOf(left.page.getPgno())));
                this.entries.add(new SimpleEntry<Integer, String>(
                        right.entries.get(0).getKey(),String.valueOf(right.page.getPgno())));

                /* 更新数据域 */
                flushPage(this.entries,this);
                flushPage(left.entries,left);
                flushPage(right.entries,right);
            }
        }
    }

    /** 删除节点后中间节点的更新*/
    protected void updateRemove(BplusTree tree) {

        // 如果子节点数小于M / 2或者小于2，则需要合并节点
        if (entries.size() < tree.getOrder() / 2 || entries.size() < 2) {
            if (page.getPageType() == PageType.TABLE_ROOT) {
                // 如果是根节点并且子节点数大于等于2，OK
                if (entries.size() >= 2) return;
                // 否则与子节点合并
                Page p = pager.aquirePage(Integer.parseInt(entries.get(0).getValue()));
                BplusNode root = new BplusNode(pager, p);
                tree.setRoot(root);
                tree.setHeight(tree.getHeight() - 1);
                root.page.setpParent(0);
                root.page.setPageType(PageType.TABLE_ROOT);
                entries = null;
                return ;
            }

            // 如果前节点子节点数大于M / 2并且大于2，则从其处借补
            BplusNode previous = new BplusNode(pager, pager.aquirePage(page.getpPrev()));
            if (page.getpPrev() != 0
                    && previous.entries.size() > tree.getOrder() / 2
                    && previous.entries.size() > 2) {
                //前叶子节点末尾节点添加到首位
                int idx = previous.entries.size() - 1;
                Integer pgno = Integer.parseInt(previous.entries.get(idx).getValue());
                BplusNode borrow = new BplusNode(pager, pager.aquirePage(pgno));
                borrow.page.setpParent(page.getPgno());

                BplusNode parent = new BplusNode(pager, pager.aquirePage(page.getpParent()));
                int preIndex = parent.entries.indexOf(previous);

                entries.add(0,parent.entries.get(preIndex));
                parent.entries.set(preIndex, previous.entries.remove(idx - 1));
                return ;
            }

            // 如果后节点子节点数大于M / 2并且大于2，则从其处借补
            BplusNode next = new BplusNode(pager, pager.aquirePage(page.getpNext()));
            if (page.getpNext() != 0
                    && next.entries.size() > tree.getOrder() / 2
                    && next.entries.size() > 2) {
                //后叶子节点首位添加到末尾
                Integer pgno = Integer.parseInt(next.entries.get(0).getValue());
                BplusNode borrow = new BplusNode(pager, pager.aquirePage(pgno));
                next.entries.remove(0);
                borrow.page.setpParent(page.getPgno());

                BplusNode parent = new BplusNode(pager,pager.aquirePage(page.getpParent()));

                int preIndex = parent.entries.indexOf(this);
                entries.add(parent.entries.get(preIndex));
                parent.entries.set(preIndex, next.entries.remove(0));
                return ;
            }

            // 同前面节点合并
            BplusNode parent = new BplusNode(pager,pager.aquirePage(page.getpParent()));
            if (page.getpPrev() != 0
                    && (previous.entries.size() <= tree.getOrder() / 2
                    || previous.entries.size() <= 2)) {
                for (int i = 0; i < entries.size(); i++) {
                    previous.entries.add(entries.get(i));
                }
                for(int i = 0; i < previous.entries.size();i++){
//                    previous.children.get(i).parent = this;
                }
                int indexPre = parent.entries.indexOf(previous);
                previous.entries.add(parent.entries.get(indexPre));
                for (int i = 0; i < entries.size(); i++) {
                    previous.entries.add(entries.get(i));
                }
                entries = previous.entries;

                //更新父节点的关键字列表
                parent.entries.remove(previous);
                previous.page.setpParent(0);
                previous.entries = null;
                parent.entries.remove(parent.entries.indexOf(this));
                if((!(parent.page.getPageType()==0) && (parent.entries.size() >= tree.getOrder() / 2
                        && parent.entries.size() >= 2))
                        ||(parent.page.getPageType()==0) && parent.entries.size() >= 2){
                    return ;
                }
                parent.updateRemove(tree);
                return ;
            }

            // 同后面节点合并
            if (page.getpNext() != 0
                    && (next.entries.size() <= tree.getOrder() / 2
                    || next.entries.size() <= 2)) {
                for (int i = 0; i < next.entries.size(); i++) {
                    Integer pgno = Integer.parseInt(next.entries.get(i).getValue());
                    BplusNode child = new BplusNode(pager, pager.aquirePage(pgno));
//                    entries.add(child.entries.get(0).getValue(), child.page.getPgno());
                    child.page.setpParent(page.getPgno());
                }
                int index = parent.entries.indexOf(entries.get(0).getKey());
                entries.add(parent.entries.get(index));
                for (int i = 0; i < next.entries.size(); i++) {
                    entries.add(next.entries.get(i));
                }
                parent.entries.remove(next.entries.get(0).getKey());
                next.page.setpParent(0);
                next.entries = null;
                parent.entries.remove(parent.entries.indexOf(entries.get(0).getKey()));
                if((!(parent.page.getPageType() == PageType.TABLE_ROOT ) && (parent.entries.size() >= tree.getOrder() / 2
                        && parent.entries.size() >= 2))
                        ||(parent.page.getPageType() == PageType.TABLE_ROOT ) && parent.entries.size() >= 2){
                    return ;
                }
                parent.updateRemove(tree);
                return ;
            }
        }
    }

    public String remove(Integer key, BplusTree tree){
        //如果是叶子节点
        if (page.getPageType() == PageType.TABLE_LEAF){
            //如果不包含该关键字，则直接返回
            if (contains(key) == -1){
                return null;
            }
            //如果既是叶子节点又是根节点，直接删除
            if (page.getPageType() == PageType.TABLE_ROOT) {
                if(entries.size() == 1){
                    tree.setHeight(0);
                }
                return remove(key);
            }
            //如果关键字数大于M / 2，直接删除
            if (entries.size() > tree.getOrder() / 2 && entries.size() > 2) {
                return remove(key);
            }
            //如果自身关键字数小于M / 2，并且前节点关键字数大于M / 2，则从其处借补
            BplusNode previous = new BplusNode(pager, pager.aquirePage(page.getpPrev()));
            BplusNode parent = new BplusNode(pager, pager.aquirePage(page.getpParent()));
            if (page.getpPrev() != 0 &&
                    previous.page.getpParent() == page.getpParent()
                    && previous.entries.size() > tree.getOrder() / 2
                    && previous.entries.size() > 2 ) {
                //添加到首位
                int size = previous.entries.size();
                entries.add(0, previous.entries.remove(size - 1));
                int index = parent.entries.indexOf(previous.entries.get(0).getKey());
                parent.entries.set(index, entries.get(0));
                return remove(key);
            }
            //如果自身关键字数小于M / 2，并且后节点关键字数大于M / 2，则从其处借补
            BplusNode next = new BplusNode(pager, pager.aquirePage(page.getpNext()));
            if (page.getpNext() != 0
                    && next.page.getpParent() == page.getpParent()
                    && next.entries.size() > tree.getOrder() / 2
                    && next.entries.size() > 2) {
                entries.add(next.entries.remove(0));
                int index = parent.entries.indexOf(entries.get(0).getKey());
                parent.entries.set(index, next.entries.get(0));
                return remove(key);
            }

            //同前面节点合并
            if (previous != null
                    && previous.page.getpParent() == page.getpParent()
                    && (previous.entries.size() <= tree.getOrder() / 2
                    || previous.entries.size() <= 2)) {
                String returnValue =  remove(key);
                for (int i = 0; i < entries.size(); i++) {
                    //将当前节点的关键字添加到前节点的末尾
                    previous.entries.add(entries.get(i));
                }
                entries = previous.entries;
                parent.entries.remove(previous.entries.get(0).getKey());
                previous.page.setpParent(0);
                previous.entries = null;
                //更新链表
                if (previous.page.getpPrev() != 0) {
                    BplusNode temp = previous;
                    new BplusNode(pager, pager.aquirePage(temp.page.getpPrev())).page.setpNext(page.getPgno());
                    page.setpPrev(temp.page.getpPrev());
                    temp.page.setpPrev(0);
                    temp.page.setpNext(0);
                }else {
                    tree.setHead(this);
                    previous.page.setpNext(0);
                    page.setpPrev(0);
                }
                parent.entries.remove(parent.entries.indexOf(entries.get(0).getKey()));
                if((!(parent.page.getPageType() == PageType.TABLE_ROOT) && (parent.entries.size() >= tree.getOrder() / 2
                        && parent.entries.size() >= 2))
                        ||(parent.page.getPageType() == PageType.TABLE_ROOT) && parent.entries.size() >= 2){
                    return returnValue;
                }
                parent.updateRemove(tree);
                return returnValue;
            }
            //同后面节点合并
            if(next != null
                    && next.page.getpParent() == page.getpParent()
                    && (next.entries.size() <= tree.getOrder() / 2
                    || next.entries.size() <= 2)) {
                String returnValue = remove(key);
                for (int i = 0; i < next.entries.size(); i++) {
                    //从首位开始添加到末尾
                    entries.add(next.entries.get(i));
                }
                next.page.setpParent(0);
                next.entries = null;
                parent.entries.remove(next);
                //更新链表
                if (next.page.getpNext() != 0) {
                    BplusNode temp = next;
                    new BplusNode(pager, pager.aquirePage(temp.page.getpNext())).page.setpPrev(page.getPgno());
                    page.setpNext(temp.page.getpNext());
                    temp.page.setpPrev(0);
                    temp.page.setpNext(0);
                }else {
                    next.page.setpPrev(0);
                    next = null;
                }
                //更新父节点的关键字列表
                parent.entries.remove(parent.entries.indexOf(entries.get(0).getKey()));
                if((!(parent.page.getPageType() == PageType.TABLE_ROOT) && (parent.entries.size() >= tree.getOrder() / 2
                        && parent.entries.size() >= 2))
                        ||(parent.page.getPageType() == PageType.TABLE_ROOT) && parent.entries.size() >= 2){
                    return returnValue;
                }
                parent.updateRemove(tree);
                return returnValue;
            }
        }
        /*如果不是叶子节点*/

        //如果key小于等于节点最左边的key，沿第一个子节点继续搜索
        Integer pgno;
        BplusNode children;
        if (key.compareTo(entries.get(0).getKey()) < 0) {
            pgno = Integer.parseInt(entries.get(0).getValue());
            children = new BplusNode(pager, pager.aquirePage(pgno));
            return children.remove(key, tree);
            //如果key大于节点最右边的key，沿最后一个子节点继续搜索
        }else if (key.compareTo(entries.get(entries.size()-1).getKey()) >= 0) {
            pgno = Integer.parseInt(entries.get(entries.size()-1).getValue());
            children = new BplusNode(pager, pager.aquirePage(pgno));
            return children.remove(key, tree);
            //否则沿比key大的前一个子节点继续搜索
        }else {
            int low = 0, high = entries.size() - 1, mid= 0;
            int comp ;
            while (low <= high) {
                mid = (low + high) / 2;
                comp = entries.get(mid).getKey().compareTo(key);
                if (comp == 0) {
                    pgno = Integer.parseInt(entries.get(mid + 1).getValue());
                    children = new BplusNode(pager, pager.aquirePage(pgno));
                    return children.remove(key, tree);
                } else if (comp < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            pgno = Integer.parseInt(entries.get(low).getValue());
            children = new BplusNode(pager, pager.aquirePage(pgno));
            return children.remove(key, tree);
        }
    }

    /**
     * 判断当前节点是否包含该关键字
     * 若包含该关键字，则返回-1，若包含该关键字，则返回entries中的位置
     */
    protected int contains(Integer key) {
        int low = 0, high = entries.size() - 1, mid;
        int comp ;
        while (low <= high) {
            mid = (low + high) / 2;
            comp = entries.get(mid).getKey().compareTo(key);
            if (comp == 0) {
                return mid;
            } else if (comp < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return -1;
    }

    /** 插入到当前节点的关键字中*/
    protected void insertOrUpdate(Integer key, String value){
        //二叉查找，插入
        int low = 0, high = entries.size() - 1, mid;
        int comp ;
        while (low <= high) {
            mid = (low + high) / 2;
            comp = entries.get(mid).getKey().compareTo(key);
            if (comp == 0) {
                entries.get(mid).setValue(value);
                break;
            } else if (comp < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        if(low>high){
            entries.add(low, new AbstractMap.SimpleEntry<Integer, String>(key, value));
        }
        flushPage(entries,this);
    }
    /* 刷新页面数据域 */
    protected void flushPage(List<Entry<Integer,String>> entries, BplusNode node){
        if(node.page.getPageType() == PageType.TABLE_LEAF){
            node.schema = node.schema.getTableSchema();
        }else{
            node.schema = node.schema.getInternalSchema();
        }

        List<Entry<Integer,byte[]>> dataList = new ArrayList<>();
        for(int i=0; i<entries.size();i++)
        {
            dataList.add(new AbstractMap.SimpleEntry<Integer,byte[]>(entries.get(i).getKey(),
                    node.schema.getBytes(entries.get(i).getKey(),entries.get(i).getValue())));
        }
        pager.writeData(node.page.getPgno(),dataList);
    }

    /** 删除节点*/
    protected String remove(Integer key){
        int low = 0,high = entries.size() -1,mid;
        int comp;
        while(low<= high){
            mid  = (low+high)/2;
            comp = entries.get(mid).getKey().compareTo(key);
            if(comp == 0){
                return entries.remove(mid).getValue();
            }else if(comp < 0){
                low = mid + 1;
            }else {
                high = mid - 1;
            }
        }
        return null;
    }
    public String toString(){
        return page.toString();
    }
}
