package main.db.bt;

/**
 * @author fengcaiwen
 * @since 12/13/2019
 */
public class BTNode<T extends Comparable<T>> implements Comparable<BTNode<T>> {
    private BTNode<T> parent = null;// 父节点
    private Vector<T> key = new Vector<>();//关键码，有序
    private Vector<BTNode<T>> child = new Vector<>();//孩子节点，总是比关键码多一个
    private java.util.Vector<Object> value = new java.util.Vector<>(); // 当节点为外部节点，才有此值

    // 创建根节点使用，含有零个关键码和孩子
    BTNode() {
        parent = null;
        child.add(0, null);//孩子比关键码多一个
    }

    public BTNode(T e, BTNode<T> lc, BTNode<T> rc) {
        parent = null;
        key.add(0, e);
        if (lc != null) {
            if (rc != null) {
                child.add(0, lc);
                child.add(1, rc);
            } else {
                child.add(0, lc);
            }
        } else {
            if (rc != null) {
                child.add(0, rc);
            }
        }

        if (lc != null) {
            lc.parent = this;
        }
        if (rc != null) {
            rc.parent = this;
        }
    }

    BTNode<T> getParent() {
        return parent;
    }

    void setParent(BTNode<T> parent) {
        this.parent = parent;
    }

    Vector<T> getKey() {
        return key;
    }

    public void setKey(Vector<T> key) {
        this.key = key;
    }

    Vector<BTNode<T>> getChild() {
        return child;
    }

    java.util.Vector<Object> getValue() {
        return value;
    }

    public void setValue(java.util.Vector<Object> value) {
        this.value = value;
    }

    public void setChild(Vector<BTNode<T>> child) {
        this.child = child;
    }

    @Override
    public int compareTo(BTNode<T> o) {
        return 0;
    }
}
