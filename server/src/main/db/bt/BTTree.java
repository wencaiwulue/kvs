package main.db.bt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * B tree base on inner static class Bt-node
 *
 * @author fengcaiwen
 * @since 7/5/2019
 */
public class BTTree<T extends Comparable<T>> {
    private int size; // 关键码总数
    private int order;// B-树阶次 256~1024
    private BTNode<T> root;
    private AtomicReference<BTNode<T>> hot = new AtomicReference<>(null);//search方法最后访问的节点

    private BTTree(int order) {
        this.order = order;
        size = 0;
        root = new BTNode<>();
    }

    private BTNode<T> search(T e) {//查找关键码e
        BTNode<T> v = root;// 从根出发
        hot.set(null);
        while (v != null) {// 逐层查找
            int r = v.key.search(e);//在当前节点中，找到不大于e的最大关键码
            if (r >= 0 && e == v.key.get(r)) return v;// 说明是叶子节点，成功

            hot.set(v);// 否则转向对应子树
            v = v.child.get(r + 1);
        }
        return null;
    }

    private boolean insert(T e, Object value) {
        BTNode<T> v = search(e);
        if (v != null) return false; // 说明已存在此关键码

        int r = hot.get().key.search(e);// 找到不大于e的最大值。
        hot.get().key.add(r + 1, e);//然后在这个位置添加，将原来的关键码向右移动一格。
//        hot.get().child.add(r + 2, null);
//        hot.get().getValue().add(r, value);//对应的value需要添加对应值。
        size++;
        overflow(hot.get());
        return true;
    }

    // 上溢
    private void overflow(BTNode<T> v) {
        if (v.key.size() <= order) return;// 不满足上浮条件，递归基出口

        int s = v.key.size() / 2;
        BTNode<T> u = new BTNode<>();
        for (int i = 0; i < v.key.size() - s - 1; i++) {// v右侧order-s-1个孩子及关键码分裂为右侧节点u
            u.key.add(i, v.key.remove(s + 1));
        }
        if (v.child.size() != 0) {//这里需要这样处理，因为新节点的孩子为空
            for (int i = 0; i < v.key.size() - s; i++) {
                u.child.add(i, v.child.remove(s + 1));//
            }
        }

        if (v.child.size() != 0) {//若u的孩子们非空
            for (int i = 0; i < v.key.size() - s; i++) {
                u.child.get(i).parent = u;//令其父节点统一指向u
            }
        }

        BTNode<T> p = v.parent;//v当前父节点
        if (p == null) {
            root = p = new BTNode<>();
            v.parent = p;
            p.key.add(0, v.key.remove(s));
            p.child.add(0, v);
            p.child.add(1, u);
        } else {
            int r = 1 + p.key.search(v.key.get(0));
            p.key.add(r, v.key.remove(s));
            p.child.add(r + 1, u);
        }
        u.parent = p;
        overflow(p);
    }

    public boolean remove(T e) {
        BTNode<T> v = search(e);
        if (v == null) return false;

        int r = v.key.search(e);
        if (v.child.get(0) != null) {
            BTNode<T> u = v.child.get(r + 1);
            while (u.child.get(0) != null) {
                u = u.child.get(0);
            }
            v.key.add(r, u.key.get(0));
            v = u;
            r = 0;
        }
        v.key.remove(r);
        v.child.remove(r + 1);
        size--;
        underFlow(v);
        return true;
    }

    private void underFlow(BTNode<T> v) {
        if ((order + 1) / 2 <= v.child.size()) return;

        BTNode<T> p = v.parent;
        if (p == null) {
            if (v.key.size() == 0 && v.child.get(0) != null) {
                root = v.child.get(0);
                root.parent = null;
                v.child.add(0, null);
            }
            return;
        }

        int r = 0;
        while (p.child.get(r) != v) {
            r++;
        }

        //1, 向左兄弟借
        if (0 < r) {
            BTNode<T> ls = p.child.get(r - 1);
            if ((order + 1) / 2 < ls.child.size()) {
                System.out.println("case 1");
                v.key.add(0, p.key.get(r - 1));
                p.key.add(r - 1, ls.key.remove(ls.key.size() - 1));
                v.child.add(0, ls.child.remove(ls.child.size() - 1));
                if (v.child.get(0) != null) {
                    v.child.get(0).parent = v;
                }
                return;
            }
        }

        // 2, 向有兄弟借
        if (p.child.size() - 1 > r) {
            BTNode<T> rs = p.child.get(r + 1);
            if ((order + 1) / 2 < rs.child.size()) {
                System.out.println("case 2");
                v.key.add(v.key.size(), p.key.get(r));
                p.key.add(r, rs.key.remove(0));
                v.child.add(v.child.size(), rs.child.remove(0));
            }
            if (v.child.get(v.child.size() - 1) != null) {
                v.child.get(v.child.size() - 1).parent = v;
            }
            return;
        }

        if (0 < r) {
            System.out.println("case 3");
            BTNode<T> ls = p.child.get(r - 1);
            ls.key.add(ls.key.size(), p.key.remove(r - 1));
            p.child.remove(r);
            ls.child.add(ls.child.size(), v.child.remove(0));
            if (ls.child.get(ls.child.size() - 1) != null) {
                ls.child.get(ls.child.size() - 1).parent = ls;
            }
            while (!v.key.isEmpty()) {
                ls.key.add(ls.key.size(), v.key.remove(0));
                ls.child.add(ls.child.size(), v.child.remove(0));
                if (ls.child.get(ls.child.size() - 1) != null) {
                    ls.child.get(ls.child.size() - 1).parent = ls;
                }
            }
        } else {
            System.out.println("case 4");
            BTNode<T> rs = p.child.get(r + 1);
            rs.key.add(0, v.key.remove(v.child.size() - 1));
            p.child.remove(r);
            rs.child.add(0, v.child.remove(v.child.size() - 1));
            if (rs.child.get(0) != null) {
                rs.child.get(0).parent = rs;
            }
            while (!v.key.isEmpty()) {
                rs.key.add(0, v.key.remove(v.key.size() - 1));
                rs.child.add(0, v.child.remove(v.child.size() - 1));
                if (rs.child.get(0) != null) {
                    rs.child.get(0).parent = rs;
                }
            }
        }
        underFlow(p);
        return;
    }

    public static void main(String[] args) {
        BTTree<Integer> tree = new BTTree<>(3);
        int n = 1000;
        for (int i = 0; i < n; i++) {
            tree.insert(i, i);
        }

        List<Integer> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            BTNode<Integer> search = tree.search(n);
            if (search == null) {
                list.add(i);
            }
        }
        System.out.println(Arrays.toString(list.toArray()));
        System.out.println(list.size());
    }
}
