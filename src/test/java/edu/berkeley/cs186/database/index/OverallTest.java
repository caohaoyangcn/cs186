package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.MemoryDiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.BufferManagerImpl;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.recovery.DummyRecoveryManager;
import edu.berkeley.cs186.database.table.RecordId;
import org.junit.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;


public class OverallTest {
    private BufferManager bufferManager;
    private BPlusTreeMetadata metadata;
    private LockContext treeContext;

    @Before
    public void setup()  {
        DiskSpaceManager diskSpaceManager = new MemoryDiskSpaceManager();
        diskSpaceManager.allocPart(0);
        this.bufferManager = new BufferManagerImpl(diskSpaceManager, new DummyRecoveryManager(), 1024,
                new ClockEvictionPolicy());
        this.treeContext = new DummyLockContext();
        this.metadata = null;
    }

    private void setBPlusTreeMetadata(Type keySchema, int order) {
        this.metadata = new BPlusTreeMetadata("test", "col", keySchema, order,
                0, DiskSpaceManager.INVALID_PAGE_NUM, -1);
    }

    private BPlusTree getBPlusTree(Type keySchema, int order) {
        setBPlusTreeMetadata(keySchema, order);
        return new BPlusTree(bufferManager, metadata, treeContext);
    }


    @Test
    public void testIterator() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        List<Pair<DataBox, RecordId>> xs = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            xs.add(new Pair<>(new IntDataBox(i), new RecordId((long) i, (short) i)));
        }
        Iterator<Pair<DataBox, RecordId>> iter = xs.iterator();
        tree.bulkLoad(iter, 0.75f);
        for (int i = 0; i < 20; i++) {
            tree.remove(new IntDataBox(i));
        }
        Iterator<RecordId> idIterator = tree.scanAll();
        assertFalse(idIterator.hasNext());
    }
}
