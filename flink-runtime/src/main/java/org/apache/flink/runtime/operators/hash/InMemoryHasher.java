package org.apache.flink.runtime.operators.hash;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.operators.sort.LargeRecordHandler;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.List;

/**
 * Created by rafiullah on 6/23/15.
 */
public interface InMemoryHasher<T> extends IndexHashable {

    /**
     * Resets the hash buffer back to the state where it is empty. All contained data is discarded.
     */
    void reset();

    /**
     * Checks whether the buffer is empty.
     *
     * @return True, if no record is contained, false otherwise.
     */
    boolean isEmpty();

    /**
     * Collects all memory segments from this hasher.
     *
     * @return All memory segments from this hasher.
     */
    List<MemorySegment> dispose();

    /**
     * Gets the total capacity of this hasher, in bytes.
     *
     * @return The hasher's total capacity.
     */
    long getCapacity();

    /**
     * Gets the number of bytes currently occupied in this hasher, records and hash index.
     *
     * @return The number of bytes occupied.
     */
    long getOccupancy();

    /**
     * Gets the number of bytes occupied by records only.
     *
     * @return The number of bytes occupied by records.
     */
    long getNumRecordBytes();

    /**
     * Gets the record at the given logical position.
     *
     * @param reuse The reuse object to deserialize the record into.
     * @param logicalPosition The logical position of the record.
     * @throws IOException Thrown, if an exception occurred during deserialization.
     */
    T getRecord(T reuse, int logicalPosition) throws IOException;

    /**
     * Writes a given record to this hash buffer. The written record will be appended and take
     * the last logical position.
     *
     * @param record The record to be written.
     * @return True, if the record was successfully written, false, if the sort buffer was full.
     * @throws IOException Thrown, if an error occurred while serializing the record into the buffers.
     */
    boolean write(T record) throws IOException;

    /**
     * Gets an iterator over all records in this buffer in their logical order.
     *
     * @return An iterator returning the records in their logical order.
     */
    MutableObjectIterator<T> getIterator();

    /**
     * Writes the records in this buffer in their logical order to the given output.
     *
     * @param output The output view to write the records to.
     * @throws IOException Thrown, if an I/O exception occurred writing to the output view.
     */
    public void writeToOutput(ChannelWriterOutputView output) throws IOException;

    public void writeToOutput(ChannelWriterOutputView output, LargeRecordHandler<T> largeRecordsOutput) throws IOException;

    /**
     * Writes a subset of the records in this buffer in their logical order to the given output.
     *
     * @param output The output view to write the records to.
     * @param start The logical start position of the subset.
     * @param num The number of elements to write.
     * @throws IOException Thrown, if an I/O exception occurred writing to the output view.
     */
    public void writeToOutput(final ChannelWriterOutputView output, final int start, int num) throws IOException;
}
