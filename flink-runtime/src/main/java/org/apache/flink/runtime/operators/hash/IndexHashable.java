package org.apache.flink.runtime.operators.hash;

/**
 * Created by rafiullah on 6/23/15.
 */
public interface IndexHashable {
    /**
     * Compare items at the given addresses consistent with the semantics of
     * {@link java.util.Comparator#compare(Object, Object)}.
     */

    //parameters data type will be considered after implementing hash function
    int compare(int i, int j);

    /**
     * Gets the number of elements in the sortable.
     *
     * @return The number of elements.
     */
    int size();
}
