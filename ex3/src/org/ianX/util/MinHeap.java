package org.ianX.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;

public class MinHeap<E> {
	private ArrayList<E> data = new ArrayList<E>();
	private HashSet<E> set = new HashSet<E>();
	private Comparator<? super E> comparator = null;

	public MinHeap(Comparator<? super E> comp) {
		// TODO Auto-generated constructor stub
		this.comparator = comp;
	}

	public void ensureCapacity(int n) {
		data.ensureCapacity(n);
	}

	private void exchange(int p1, int p2) {
		E tmp = data.get(p1);
		data.set(p1, data.get(p2));
		data.set(p2, tmp);
	}

	public void push(E e) {
		if (set.contains(e))
			return;
		set.add(e);
		int pos = data.size();
		int father;
		data.add(pos, e);
		while (pos > 0) {
			father = pos / 2;
			if (comparator.compare(data.get(pos), data.get(father)) < 0) {
				exchange(pos, father);
				pos = father;
			}
			break;
		}
	}

	public E pop() {
		if (data.isEmpty())
			return null;

		int lastpos = data.size() - 1;
		E last = data.get(lastpos);
		data.remove(lastpos);
		set.remove(last);

		if (data.size() == 0)
			return last;

		return change(last);
	}

	public E change(E e) {
		if (data.isEmpty()) {
			return null;
		}

		E ret = data.get(0);

		if (set.contains(e) || comparator.compare(ret, e) < 0)
			return e;

		set.add(e);
		set.remove(ret);
		data.set(0, e);
		int pos = 0;
		int child = 1;
		while (child < data.size()) {
			if (child + 1 < data.size()
					&& comparator.compare(data.get(child), data.get(child + 1)) > 0)
				child++;

			if (comparator.compare(data.get(pos), data.get(child)) > 0) {
				exchange(pos, child);
			} else {
				break;
			}
			pos = child;
			child = pos * 2 + 1;
		}
		return ret;
	}

	public int size() {
		return data.size();
	}
}
