package org.apache.flink.runtime.operators.hash;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.memorymanager.AbstractPagedInputView;
import org.apache.flink.runtime.memorymanager.AbstractPagedOutputView;
import org.apache.flink.runtime.operators.sort.LargeRecordHandler;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rafiullah on 6/23/15.
 */
public class FixedLengthRecordHasher<T> implements  InMemoryHasher<T> {

    private static final int MIN_REQUIRED_BUFFERS = 3;

    // ------------------------------------------------------------------------
    //                               Members
    // ------------------------------------------------------------------------


    private final TypeSerializer<T> serializer;

    private final TypeComparator<T> comparator;

    private final SingleSegmentOutputView outView;

    private final SingleSegmentInputView inView;


    private MemorySegment currentHashBufferSegment;
    //private MemorySegment currentSortBufferSegment;

    private int currentHashBufferOffset;


    private final ArrayList<MemorySegment> freeMemory;

    private final ArrayList<MemorySegment> hashBuffer;

    private long hashBufferBytes;
    //private long sortBufferBytes;

    private int numRecords;

    private final int numKeyBytes;

    private final int recordSize;

    private final int recordsPerSegment;

    private final int lastEntryOffset;

    private final int segmentSize;

    private final int totalNumBuffers;

    private final boolean useNormKeyUninverted;

    private final T recordInstance;


    // -------------------------------------------------------------------------
    // Constructors / Destructors
    // -------------------------------------------------------------------------

    public FixedLengthRecordHasher(TypeSerializer<T> serializer, TypeComparator<T> comparator,
                                   List<MemorySegment> memory)
    {
        if (serializer == null || comparator == null || memory == null) {
            throw new NullPointerException();
        }

        this.serializer = serializer;
        this.comparator = comparator;
        this.useNormKeyUninverted = !comparator.invertNormalizedKey();

        // check the size of the first buffer and record it. all further buffers must have the same size.
        // the size must also be a power of 2
        this.totalNumBuffers = memory.size();
        if (this.totalNumBuffers < MIN_REQUIRED_BUFFERS) {
            throw new IllegalArgumentException("Normalized-Key hasher requires at least " + MIN_REQUIRED_BUFFERS + " memory buffers.");
        }
        this.segmentSize = memory.get(0).size();
        this.recordSize = serializer.getLength();
        this.numKeyBytes = this.comparator.getNormalizeKeyLen();

        // check that the serializer and comparator allow our operations
        if (this.recordSize <= 0) {
            throw new IllegalArgumentException("This hasher works only for fixed-length data types.");
        } else if (this.recordSize > this.segmentSize) {
            throw new IllegalArgumentException("This hasher works only for record lengths below the memory segment size.");
        } else if (!comparator.supportsSerializationWithKeyNormalization()) {
            throw new IllegalArgumentException("This hasher requires a comparator that supports serialization with key normalization.");
        }

        // compute the entry size and limits
        this.recordsPerSegment = segmentSize / this.recordSize;
        this.lastEntryOffset = (this.recordsPerSegment - 1) * this.recordSize;


        if (memory instanceof ArrayList<?>) {
            this.freeMemory = (ArrayList<MemorySegment>) memory;
        }
        else {
            this.freeMemory = new ArrayList<MemorySegment>(memory.size());
            this.freeMemory.addAll(memory);
        }

        // create the buffer collections
        this.hashBuffer = new ArrayList<MemorySegment>(16);
        this.outView = new SingleSegmentOutputView(this.segmentSize);
        this.inView = new SingleSegmentInputView(this.lastEntryOffset + this.recordSize);
        this.currentHashBufferSegment = nextMemorySegment();
        this.hashBuffer.add(this.currentHashBufferSegment);
        this.outView.set(this.currentHashBufferSegment);

        this.recordInstance = this.serializer.createInstance();
    }

    // -------------------------------------------------------------------------
    // Memory Segment
    // -------------------------------------------------------------------------

    /**
     * Resets the sort buffer back to the state where it is empty. All contained data is discarded.
     */
    @Override
    public void reset() {
        // reset all offsets
        this.numRecords = 0;
        this.currentHashBufferOffset = 0;
        this.hashBufferBytes = 0;

        // return all memory
        this.freeMemory.addAll(this.hashBuffer);
        this.hashBuffer.clear();

        // grab first buffers
        this.currentHashBufferSegment = nextMemorySegment();
        this.hashBuffer.add(this.currentHashBufferSegment);
        this.outView.set(this.currentHashBufferSegment);
    }

    /**
     * Checks whether the buffer is empty.
     *
     * @return True, if no record is contained, false otherwise.
     */
    @Override
    public boolean isEmpty() {
        return this.numRecords == 0;
    }

    /**
     * Collects all memory segments from this hasher.
     *
     * @return All memory segments from this hasher.
     */
    @Override
    public List<MemorySegment> dispose() {
        this.freeMemory.addAll(this.hashBuffer);
        this.hashBuffer.clear();
        return this.freeMemory;
    }

    @Override
    public long getCapacity() {
        return ((long) this.totalNumBuffers) * this.segmentSize;
    }

    @Override
    public long getOccupancy() {
        return this.hashBufferBytes;
    }

    @Override
    public long getNumRecordBytes() {
        return this.hashBufferBytes;
    }

    // -------------------------------------------------------------------------
    // Retrieving and Writing
    // -------------------------------------------------------------------------

    /**
     * Gets the record at the given logical position.
     *
     * @param reuse The reuse object to deserialize the record into.
     * @param logicalPosition The logical position of the record.
     * @throws IOException Thrown, if an exception occurred during deserialization.
     */
    @Override
    public T getRecord(T reuse, int logicalPosition) throws IOException {
        final int buffer = logicalPosition / this.recordsPerSegment;
        final int inBuffer = (logicalPosition % this.recordsPerSegment) * this.recordSize;
        this.inView.set(this.hashBuffer.get(buffer), inBuffer);
        return this.comparator.readWithKeyDenormalization(reuse, this.inView);
    }

    /**
     * Writes a given record to this hash buffer. The written record will be appended and take
     * the last logical position.
     *
     * @param record The record to be written.
     * @return True, if the record was successfully written, false, if the sort buffer was full.
     * @throws IOException Thrown, if an error occurred while serializing the record into the buffers.
     */
    @Override
    public boolean write(T record) throws IOException {
        // check whether we need a new memory segment for the hash index
        if (this.currentHashBufferOffset > this.lastEntryOffset) {
            if (memoryAvailable()) {
                this.currentHashBufferSegment = nextMemorySegment();
                this.hashBuffer.add(this.currentHashBufferSegment);
                this.outView.set(this.currentHashBufferSegment);
                this.currentHashBufferOffset = 0;
                this.hashBufferBytes += this.segmentSize;
            }
            else {
                return false;
            }
        }

        // serialize the record into the data buffers
        try {
            this.comparator.writeWithKeyNormalization(record, this.outView);
            this.numRecords++;
            this.currentHashBufferOffset += this.recordSize;
            return true;
        } catch (EOFException eofex) {
            throw new IOException("Error: Serialization consumes more bytes than announced by the serializer.");
        }
    }

    // ------------------------------------------------------------------------
    //                           Access Utilities
    // ------------------------------------------------------------------------

    private final boolean memoryAvailable() {
        return !this.freeMemory.isEmpty();
    }

    private final MemorySegment nextMemorySegment() {
        return this.freeMemory.remove(this.freeMemory.size() - 1);
    }

    // -------------------------------------------------------------------------
    // Hashing
    // -------------------------------------------------------------------------

    @Override
    public int compare(int i, int j) {
        final int bufferNumI = i / this.recordsPerSegment;
        final int segmentOffsetI = (i % this.recordsPerSegment) * this.recordSize;

        final int bufferNumJ = j / this.recordsPerSegment;
        final int segmentOffsetJ = (j % this.recordsPerSegment) * this.recordSize;

        final MemorySegment segI = this.hashBuffer.get(bufferNumI);
        final MemorySegment segJ = this.hashBuffer.get(bufferNumJ);

        int val = MemorySegment.compare(segI, segJ, segmentOffsetI, segmentOffsetJ, this.numKeyBytes);
        return this.useNormKeyUninverted ? val : -val;
    }

    /*@Override
    public void swap(int i, int j) {
        final int bufferNumI = i / this.recordsPerSegment;
        final int segmentOffsetI = (i % this.recordsPerSegment) * this.recordSize;

        final int bufferNumJ = j / this.recordsPerSegment;
        final int segmentOffsetJ = (j % this.recordsPerSegment) * this.recordSize;

        final MemorySegment segI = this.sortBuffer.get(bufferNumI);
        final MemorySegment segJ = this.sortBuffer.get(bufferNumJ);

        MemorySegment.swapBytes(segI, segJ, this.swapBuffer, segmentOffsetI, segmentOffsetJ, this.recordSize);
    }*/

    @Override
    public int size() {
        return this.numRecords;
    }

    // -------------------------------------------------------------------------

    /**
     * Gets an iterator over all records in this buffer in their logical order.
     *
     * @return An iterator returning the records in their logical order.
     */
    @Override
    public final MutableObjectIterator<T> getIterator() {
        final SingleSegmentInputView startIn = new SingleSegmentInputView(this.recordsPerSegment * this.recordSize);
        startIn.set(this.hashBuffer.get(0), 0);

        return new MutableObjectIterator<T>() {

            private final SingleSegmentInputView in = startIn;
            private final TypeComparator<T> comp = comparator;

            private final int numTotal = size();
            private final int numPerSegment = recordsPerSegment;

            private int currentTotal = 0;
            private int currentInSegment = 0;
            private int currentSegmentIndex = 0;

            @Override
            public T next(T reuse) {
                if (this.currentTotal < this.numTotal) {

                    if (this.currentInSegment >= this.numPerSegment) {
                        this.currentInSegment = 0;
                        this.currentSegmentIndex++;
                        this.in.set(hashBuffer.get(this.currentSegmentIndex), 0);
                    }

                    this.currentTotal++;
                    this.currentInSegment++;

                    try {
                        return this.comp.readWithKeyDenormalization(reuse, this.in);
                    }
                    catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
                else {
                    return null;
                }
            }

            @Override
            public T next() {
                if (this.currentTotal < this.numTotal) {

                    if (this.currentInSegment >= this.numPerSegment) {
                        this.currentInSegment = 0;
                        this.currentSegmentIndex++;
                        this.in.set(hashBuffer.get(this.currentSegmentIndex), 0);
                    }

                    this.currentTotal++;
                    this.currentInSegment++;

                    try {
                        return this.comp.readWithKeyDenormalization(serializer.createInstance(), this.in);
                    }
                    catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
                else {
                    return null;
                }
            }
        };
    }

    // ------------------------------------------------------------------------
    //                Writing to a DataOutputView
    // ------------------------------------------------------------------------

    /**
     * Writes the records in this buffer in their logical order to the given output.
     *
     * @param output The output view to write the records to.
     * @throws IOException Thrown, if an I/O exception occurred writing to the output view.
     */
    @Override
    public void writeToOutput(final ChannelWriterOutputView output) throws IOException {
        final TypeComparator<T> comparator = this.comparator;
        final TypeSerializer<T> serializer = this.serializer;
        T record = this.recordInstance;

        final SingleSegmentInputView inView = this.inView;

        final int recordsPerSegment = this.recordsPerSegment;
        int recordsLeft = this.numRecords;
        int currentMemSeg = 0;

        while (recordsLeft > 0) {
            final MemorySegment currentIndexSegment = this.hashBuffer.get(currentMemSeg++);
            inView.set(currentIndexSegment, 0);

            // check whether we have a full or partially full segment
            if (recordsLeft >= recordsPerSegment) {
                // full segment
                for (int numInMemSeg = 0; numInMemSeg < recordsPerSegment; numInMemSeg++) {
                    record = comparator.readWithKeyDenormalization(record, inView);
                    serializer.serialize(record, output);
                }
                recordsLeft -= recordsPerSegment;
            } else {
                // partially filled segment
                for (; recordsLeft > 0; recordsLeft--) {
                    record = comparator.readWithKeyDenormalization(record, inView);
                    serializer.serialize(record, output);
                }
            }
        }
    }

    @Override
    public void writeToOutput(ChannelWriterOutputView output, LargeRecordHandler<T> largeRecordsOutput)
            throws IOException
    {
        writeToOutput(output);
    }

    /**
     * Writes a subset of the records in this buffer in their logical order to the given output.
     *
     * @param output The output view to write the records to.
     * @param start The logical start position of the subset.
     * @param num The number of elements to write.
     * @throws IOException Thrown, if an I/O exception occurred writing to the output view.
     */
    @Override
    public void writeToOutput(final ChannelWriterOutputView output, final int start, int num) throws IOException {
        final TypeComparator<T> comparator = this.comparator;
        final TypeSerializer<T> serializer = this.serializer;
        T record = this.recordInstance;

        final SingleSegmentInputView inView = this.inView;

        final int recordsPerSegment = this.recordsPerSegment;
        int currentMemSeg = start / recordsPerSegment;
        int offset = (start % recordsPerSegment) * this.recordSize;

        while (num > 0) {
            final MemorySegment currentIndexSegment = this.hashBuffer.get(currentMemSeg++);
            inView.set(currentIndexSegment, offset);

            // check whether we have a full or partially full segment
            if (num >= recordsPerSegment && offset == 0) {
                // full segment
                for (int numInMemSeg = 0; numInMemSeg < recordsPerSegment; numInMemSeg++) {
                    record = comparator.readWithKeyDenormalization(record, inView);
                    serializer.serialize(record, output);
                }
                num -= recordsPerSegment;
            } else {
                // partially filled segment
                for (; num > 0; num--) {
                    record = comparator.readWithKeyDenormalization(record, inView);
                    serializer.serialize(record, output);
                }
            }
        }
    }

    private static final class SingleSegmentOutputView extends AbstractPagedOutputView {

        SingleSegmentOutputView(int segmentSize) {
            super(segmentSize, 0);
        }

        void set(MemorySegment segment) {
            seekOutput(segment, 0);
        }

        @Override
        protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException {
            throw new EOFException();
        }
    }

    private static final class SingleSegmentInputView extends AbstractPagedInputView {

        private final int limit;

        SingleSegmentInputView(int limit) {
            super(0);
            this.limit = limit;
        }

        protected void set(MemorySegment segment, int offset) {
            seekInput(segment, offset, this.limit);
        }

        @Override
        protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
            throw new EOFException();
        }

        @Override
        protected int getLimitForSegment(MemorySegment segment) {
            return this.limit;
        }
    }
}
