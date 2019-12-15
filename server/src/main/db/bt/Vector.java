package main.db.bt;

/**
 * @author fengcaiwen
 * @since 12/13/2019
 */
public class Vector<E extends Comparable<E>> extends java.util.Vector<E> {

    public int realSize = 0;

    Vector() {
        super(10);
//        for (int i = 0; i < 10; i++) {
//            super.add(i, null);
//        }
    }

    public Vector(int initialCapacity) {
        super(initialCapacity);
//        for (int i = 0; i < initialCapacity; i++) {
//            super.add(i, null);
//        }
    }

    @Override
    public synchronized E get(int index) {
        try {
            return super.get(index);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public synchronized E elementAt(int index) {
        try {
            return super.elementAt(index);
        } catch (Exception e) {
            return null;
        }
    }

    // 这里的关键码都是有序的，是否可以考虑采用二分。
    // 在当前节点中，找到不大于e的最大关键码
    int search(E e) {
        int p = -1;
        for (int i = 0; i < super.size(); i++) {
            if (this.get(i) != null && this.get(i).compareTo(e) <= 0) {
                p = i;
            }
        }
        return p;
    }


    @Override
    public void add(int index, E element) {
        realSize++;
        super.add(index, element);
    }
}
