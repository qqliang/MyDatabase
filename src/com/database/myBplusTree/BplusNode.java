package com.database.myBplusTree;

/**
 * Created by Qing_L on 2016/11/23.
 */
import com.database.global.ColumnConstraint;
import com.database.global.DataType;
import com.database.pager.Column;
import com.database.pager.Page;
import com.database.pager.Pager;
import com.database.pager.Record;

import java.util.*;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;

public class BplusNode {

    Pager pager ;
    Page page;

    private int flags;                //节点类型：0：根节点；1：内部节点；2：叶子节点；
    private BplusNode parent;         //父节点
    private BplusNode previous;       //叶子节点的前一个节点
    private BplusNode next;           //叶子节点的下一个节点
    private List<BplusNode> children; //孩子节点

    //记录信息：rowid->data。若为内部节点，data为孩子节点的页号，若为叶子节点，data为一条记录
    private List<Entry<Integer, String>> entries;

    public int getFlags() {
        return flags;
    }
    public void setFlags(int flags) {
        this.flags = flags;
    }

    public BplusNode getParent() {
        return parent;
    }
    public void setParent(BplusNode parent) {
        this.parent = parent;
    }

    public BplusNode getPrevious() {
        return previous;
    }
    public void setPrevious(BplusNode previous) {
        this.previous = previous;
    }

    public BplusNode getNext() {
        return next;
    }
    public void setNext(BplusNode next) {
        this.next = next;
    }

    public List<BplusNode> getChildren() {
        return children;
    }
    public void setChildren(List<BplusNode> children) {
        this.children = children;
    }

    public List<Entry<Integer, String>> getEntries() {
        return entries;
    }
    public void setEntries(List<Entry<Integer, String>> entries) {
        this.entries = entries;
    }

    /**
     * 构造函数
     */
    public BplusNode(int type) {
        this.flags = type;
        entries = new ArrayList<Entry<Integer, String>>();

        //如果不是叶子节点，则添加孩子指针
        if (type != 2) {
            children = new ArrayList<BplusNode>();
        }

        //新建页面
//        page = pager.getPage();
    }

    public BplusNode(Pager pager,int type) {
        this(type);
        this.pager = pager;
    }

    /**
     * 根据关键字，查找数据（采用二分查找）
     */
    public String get(Integer key) {
        //如果是叶子节点
        if (flags == 2) {
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
         * 1、如果key小于节点最左边的key，沿第一个子节点继续搜索
         * 2、如果key大于等于节点最右边的key，沿最后一个子节点继续搜索
         * 3、否则沿比key大的前一个子节点继续搜索
         */
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

    public void insertOrUpdate(Integer key, String value, BplusTree tree){
        /**
         * 如果是叶子节点
         */
        if (flags == 2){
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
            //分裂成左右两个节点
            BplusNode left = new BplusNode(pager,1);
            BplusNode right = new BplusNode(pager,1);
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
            if (previous != null){
                previous.next = left;
                left.previous = previous ;
            }
            if (next != null) {
                next.previous = right;
                right.next = next;
            }
            if (previous == null){
                tree.setHead(left);
            }
            left.next = right;
            right.previous = left;
            previous = null;
            next = null;

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
            if (parent != null) {
                //调整父子节点关系
                int index = parent.children.indexOf(this);
                parent.children.remove(this);
                left.parent = parent;
                right.parent = parent;
                parent.children.add(index,left);
                parent.children.add(index + 1, right);
                parent.entries.add(index,right.entries.get(0));
                entries = null; //删除当前节点的关键字信息
                children = null; //删除当前节点的孩子节点引用

                //父节点插入或更新关键字
                parent.updateInsert(tree);
                parent = null; //删除当前节点的父节点引用
                //如果是根节点
            }else {
                flags = 1;
                BplusNode parent = new BplusNode (pager, 0);
                tree.setRoot(parent);
                left.parent = parent;
                right.parent = parent;
                parent.children.add(left);
                parent.children.add(right);
                parent.entries.add(right.entries.get(0));
                entries = null;
                children = null;
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
        if (children.size() > tree.getOrder()) {
            //分裂成左右两个节点
            BplusNode left = new BplusNode(pager,1);
            BplusNode right = new BplusNode(pager,1);
            //左右两个节点子节点的长度
            int leftSize = (tree.getOrder() + 1) / 2 + (tree.getOrder() + 1) % 2;
            int rightSize = (tree.getOrder() + 1) / 2;
            //复制子节点到分裂出来的新节点，并更新关键字
            for (int i = 0; i < leftSize; i++){
                left.children.add(children.get(i));
                children.get(i).parent = left;
            }
            for (int i = 0; i < rightSize; i++){
                right.children.add(children.get(leftSize + i));
                children.get(leftSize + i).parent = right;
            }
            for (int i = 0; i < leftSize - 1; i++) {
                left.entries.add(entries.get(i));
            }
            for (int i = 0; i < rightSize - 1; i++) {
                right.entries.add(entries.get(leftSize + i));
            }

            //如果不是根节点
            if (parent != null) {
                //调整父子节点关系
                int index = parent.children.indexOf(this);
                parent.children.remove(this);
                left.parent = parent;
                right.parent = parent;
                parent.children.add(index,left);
                parent.children.add(index + 1, right);
                parent.entries.add(index,entries.get(leftSize - 1));
                entries = null;
                children = null;

                //父节点更新关键字
                parent.updateInsert(tree);
                parent = null;
                //如果是根节点
            }else {
                flags = 1;
                BplusNode parent = new BplusNode(pager, 0);
                tree.setRoot(parent);
                tree.setHeight(tree.getHeight() + 1);
                left.parent = parent;
                right.parent = parent;
                parent.children.add(left);
                parent.children.add(right);
                parent.entries.add(entries.get(leftSize - 1));
                entries = null;
                children = null;
            }
        }
    }

    /** 删除节点后中间节点的更新*/
    protected void updateRemove(BplusTree tree) {

        // 如果子节点数小于M / 2或者小于2，则需要合并节点
        if (children.size() < tree.getOrder() / 2 || children.size() < 2) {
            if (flags == 0) {
                // 如果是根节点并且子节点数大于等于2，OK
                if (children.size() >= 2) return;
                // 否则与子节点合并
                BplusNode root = children.get(0);
                tree.setRoot(root);
                tree.setHeight(tree.getHeight() - 1);
                root.parent = null;
                root.flags = 0;
                entries = null;
                children = null;
                return ;
            }
            //计算前后节点
            int currIdx = parent.children.indexOf(this);
            int prevIdx = currIdx - 1;
            int nextIdx = currIdx + 1;
            BplusNode previous = null, next = null;
            if (prevIdx >= 0) {
                previous = parent.children.get(prevIdx);
            }
            if (nextIdx < parent.children.size()) {
                next = parent.children.get(nextIdx);
            }

            // 如果前节点子节点数大于M / 2并且大于2，则从其处借补
            if (previous != null
                    && previous.children.size() > tree.getOrder() / 2
                    && previous.children.size() > 2) {
                //前叶子节点末尾节点添加到首位
                int idx = previous.children.size() - 1;
                BplusNode borrow = previous.children.get(idx);
                previous.children.remove(idx);
                borrow.parent = this;
                children.add(0, borrow);
                int preIndex = parent.children.indexOf(previous);

                entries.add(0,parent.entries.get(preIndex));
                parent.entries.set(preIndex, previous.entries.remove(idx - 1));
                return ;
            }

            // 如果后节点子节点数大于M / 2并且大于2，则从其处借补
            if (next != null
                    && next.children.size() > tree.getOrder() / 2
                    && next.children.size() > 2) {
                //后叶子节点首位添加到末尾
                BplusNode borrow = next.children.get(0);
                next.children.remove(0);
                borrow.parent = this;
                children.add(borrow);
                int preIndex = parent.children.indexOf(this);
                entries.add(parent.entries.get(preIndex));
                parent.entries.set(preIndex, next.entries.remove(0));
                return ;
            }

            // 同前面节点合并
            if (previous != null
                    && (previous.children.size() <= tree.getOrder() / 2
                    || previous.children.size() <= 2)) {
                for (int i = 0; i < children.size(); i++) {
                    previous.children.add(children.get(i));
                }
                for(int i = 0; i < previous.children.size();i++){
                    previous.children.get(i).parent = this;
                }
                int indexPre = parent.children.indexOf(previous);
                previous.entries.add(parent.entries.get(indexPre));
                for (int i = 0; i < entries.size(); i++) {
                    previous.entries.add(entries.get(i));
                }
                children = previous.children;
                entries = previous.entries;

                //更新父节点的关键字列表
                parent.children.remove(previous);
                previous.parent = null;
                previous.children = null;
                previous.entries = null;
                parent.entries.remove(parent.children.indexOf(this));
                if((!(parent.flags==0) && (parent.children.size() >= tree.getOrder() / 2
                        && parent.children.size() >= 2))
                        ||(parent.flags==0) && parent.children.size() >= 2){
                    return ;
                }
                parent.updateRemove(tree);
                return ;
            }

            // 同后面节点合并
            if (next != null
                    && (next.children.size() <= tree.getOrder() / 2
                    || next.children.size() <= 2)) {
                for (int i = 0; i < next.children.size(); i++) {
                    BplusNode child = next.children.get(i);
                    children.add(child);
                    child.parent = this;
                }
                int index = parent.children.indexOf(this);
                entries.add(parent.entries.get(index));
                for (int i = 0; i < next.entries.size(); i++) {
                    entries.add(next.entries.get(i));
                }
                parent.children.remove(next);
                next.parent = null;
                next.children = null;
                next.entries = null;
                parent.entries.remove(parent.children.indexOf(this));
                if((!(parent.flags==0) && (parent.children.size() >= tree.getOrder() / 2
                        && parent.children.size() >= 2))
                        ||(parent.flags==0) && parent.children.size() >= 2){
                    return ;
                }
                parent.updateRemove(tree);
                return ;
            }
        }
    }

    public String remove(Integer key, BplusTree tree){
        //如果是叶子节点
        if (flags == 2){
            //如果不包含该关键字，则直接返回
            if (contains(key) == -1){
                return null;
            }
            //如果既是叶子节点又是根节点，直接删除
            if (flags == 0) {
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
            if (previous != null &&
                    previous.parent == parent
                    && previous.entries.size() > tree.getOrder() / 2
                    && previous.entries.size() > 2 ) {
                //添加到首位
                int size = previous.entries.size();
                entries.add(0, previous.entries.remove(size - 1));
                int index = parent.children.indexOf(previous);
                parent.entries.set(index, entries.get(0));
                return remove(key);
            }
            //如果自身关键字数小于M / 2，并且后节点关键字数大于M / 2，则从其处借补
            if (next != null
                    && next.parent == parent
                    && next.entries.size() > tree.getOrder() / 2
                    && next.entries.size() > 2) {
                entries.add(next.entries.remove(0));
                int index = parent.children.indexOf(this);
                parent.entries.set(index, next.entries.get(0));
                return remove(key);
            }

            //同前面节点合并
            if (previous != null
                    && previous.parent == parent
                    && (previous.entries.size() <= tree.getOrder() / 2
                    || previous.entries.size() <= 2)) {
                String returnValue =  remove(key);
                for (int i = 0; i < entries.size(); i++) {
                    //将当前节点的关键字添加到前节点的末尾
                    previous.entries.add(entries.get(i));
                }
                entries = previous.entries;
                parent.children.remove(previous);
                previous.parent = null;
                previous.entries = null;
                //更新链表
                if (previous.previous != null) {
                    BplusNode temp = previous;
                    temp.previous.next = this;
                    previous = temp.previous;
                    temp.previous = null;
                    temp.next = null;
                }else {
                    tree.setHead(this);
                    previous.next = null;
                    previous = null;
                }
                parent.entries.remove(parent.children.indexOf(this));
                if((!(parent.flags==0) && (parent.children.size() >= tree.getOrder() / 2
                        && parent.children.size() >= 2))
                        ||(parent.flags==0) && parent.children.size() >= 2){
                    return returnValue;
                }
                parent.updateRemove(tree);
                return returnValue;
            }
            //同后面节点合并
            if(next != null
                    && next.parent == parent
                    && (next.entries.size() <= tree.getOrder() / 2
                    || next.entries.size() <= 2)) {
                String returnValue = remove(key);
                for (int i = 0; i < next.entries.size(); i++) {
                    //从首位开始添加到末尾
                    entries.add(next.entries.get(i));
                }
                next.parent = null;
                next.entries = null;
                parent.children.remove(next);
                //更新链表
                if (next.next != null) {
                    BplusNode temp = next;
                    temp.next.previous = this;
                    next = temp.next;
                    temp.previous = null;
                    temp.next = null;
                }else {
                    next.previous = null;
                    next = null;
                }
                //更新父节点的关键字列表
                parent.entries.remove(parent.children.indexOf(this));
                if((!(parent.flags==0) && (parent.children.size() >= tree.getOrder() / 2
                        && parent.children.size() >= 2))
                        ||(parent.flags==0) && parent.children.size() >= 2){
                    return returnValue;
                }
                parent.updateRemove(tree);
                return returnValue;
            }
        }
        /*如果不是叶子节点*/

        //如果key小于等于节点最左边的key，沿第一个子节点继续搜索
        if (key.compareTo(entries.get(0).getKey()) < 0) {
            return children.get(0).remove(key, tree);
            //如果key大于节点最右边的key，沿最后一个子节点继续搜索
        }else if (key.compareTo(entries.get(entries.size()-1).getKey()) >= 0) {
            return children.get(children.size()-1).remove(key, tree);
            //否则沿比key大的前一个子节点继续搜索
        }else {
            int low = 0, high = entries.size() - 1, mid= 0;
            int comp ;
            while (low <= high) {
                mid = (low + high) / 2;
                comp = entries.get(mid).getKey().compareTo(key);
                if (comp == 0) {
                    return children.get(mid + 1).remove(key, tree);
                } else if (comp < 0) {
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            return children.get(low).remove(key, tree);
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
        StringBuilder sb = new StringBuilder();
        sb.append("flags: ");
        sb.append(flags);
        sb.append(", ");
        sb.append("keys: ");
        for (Entry<Integer,String> entry : entries){
            sb.append(entry.getKey());
            sb.append(", ");
        }
        sb.append(", ");
        return sb.toString();

    }
    public Record getRecord() {
        Record record = new Record();
        List<Column> cols = new ArrayList<Column>();
        Column idCol =  new Column();
        idCol.setName("id");
        idCol.setType(DataType.INTEGER);
        idCol.setConstraint(ColumnConstraint.NONE);

        Column nameCol =  new Column();
        nameCol.setName("name");
        nameCol.setType(DataType.TEXT);
        nameCol.setConstraint(ColumnConstraint.NONE);

        Column ageCol =  new Column();
        ageCol.setName("age");
        ageCol.setType(DataType.TINY_INT);
        ageCol.setConstraint(ColumnConstraint.NONE);

        cols.add(idCol);
        cols.add(nameCol);
        cols.add(ageCol);
        record.setColumns(cols);
        return record;
    }
}
