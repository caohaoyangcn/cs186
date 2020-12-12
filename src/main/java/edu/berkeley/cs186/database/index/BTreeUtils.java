package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.DataBox;

import java.util.List;

public class BTreeUtils {
    /**
     * returns true if xs contains key
     */
    public static boolean contains(List<DataBox> xs, DataBox key) {
        int idx = insertIdxRight(xs, key);
        if (idx == 0) {
            return false;
        }
        return xs.get(idx-1).compareTo(key) == 0;
    }

    /**
     * Insert item key in list xs, and keep it sorted assuming xs is sorted.
     * If x is already in a, insert it to the left of the leftmost x.
     */
    public static int insertIdxRight(List<DataBox> xs, DataBox key) {
        int hi = xs.size();
        int lo = 0;
        int mid;
        while (lo < hi) {
            mid = (lo + hi) / 2;
            int compareResult = xs.get(mid).compareTo(key);
            if (compareResult > 0) {
                hi = mid;
            }
            else {
                lo = mid + 1;
            }
        }
        return lo;
    }

}
