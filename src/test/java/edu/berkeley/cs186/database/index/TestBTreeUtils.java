package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;


import static edu.berkeley.cs186.database.index.BTreeUtils.*;

public class TestBTreeUtils {


    @Test
    public void testContains() {
        List<DataBox> xs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            xs.add(new IntDataBox(i));
        }
        for (int i = 0; i < 10; i++) {
            assertTrue(contains(xs, new IntDataBox(i)));
        }
        for (int i = -10; i < 0; i++) {
            assertFalse(contains(xs, new IntDataBox(i)));
        }
        for (int i = 10; i < 20; i++) {
            assertFalse(contains(xs, new IntDataBox(i)));
        }
    }

    @Test
    public void testInsertIdxLeft() {
        List<DataBox> xs = new ArrayList<>();
        for (int i = 0; i < 10; i+=2) {
            xs.add(new IntDataBox(i));
        }
        assertEquals(0, insertIdxRight(xs, new IntDataBox(-1)));
        assertEquals(1, insertIdxRight(xs, new IntDataBox(1)));
        assertEquals(2, insertIdxRight(xs, new IntDataBox(3)));
        assertEquals(3, insertIdxRight(xs, new IntDataBox(5)));
        assertEquals(5, insertIdxRight(xs, new IntDataBox(10)));
    }
}
