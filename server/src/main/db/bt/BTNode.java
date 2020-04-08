package main.db.bt;

/**
 * @author fengcaiwen
 * @since 12/13/2019
 */
public class BTNode<T extends Comparable<T>> implements Comparable<BTNode<T>>{
    BTNode<T> parent = null;// 父节点
    Vector<T> key = new Vector<>();//关键码，有序
    Vector<BTNode<T>> child = new Vector<>();//孩子节点，总是比关键码多一个
    java.util.Vector<Object> value = new java.util.Vector<>(); // 当节点为外部节点，才有此值

    // 创建根节点使用，含有零个关键码和孩子
    BTNode() {
        //当关键码的个数为0个的时候，孩子的数量为1是无意义的。只有当关键码的数量大于等于1
    }

    public BTNode(T e, BTNode<T> lc, BTNode<T> rc) {
        parent = null;
        key.add(0, e);
        if ((lc == null && rc != null) || (lc != null && rc == null)) {
            throw new RuntimeException("this is impossible");//左右子树应该同时存在或者同时不存在
        }

        if (lc != null) {
            child.add(0, lc);
            child.add(1, rc);
        }

        if (lc != null) {
            lc.parent = this;
        }
        if (rc != null) {
            rc.parent = this;
        }
    }

    @Override
    public int compareTo(BTNode<T> o) {
        return 0;
    }
}
