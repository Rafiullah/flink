package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.operators.hash.FixedLengthRecordHasher;
import org.apache.flink.runtime.operators.hash.InMemoryPartition;
import org.apache.flink.runtime.operators.sort.FixedLengthRecordSorter;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.runtime.util.NonReusingKeyGroupedIterator;
import org.apache.flink.runtime.util.ReusingKeyGroupedIterator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by rafiullah on 6/20/15.
 */
public class HashGroupReduceCombineDriver<IN, OUT> implements PactDriver<GroupCombineFunction<IN, OUT>, OUT> {


    private static final Logger LOG = LoggerFactory.getLogger(HashGroupReduceCombineDriver.class);

    /** Fix length records with a length below this threshold will be in-place sorted, if possible. */
    private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;

    private PactTaskContext<GroupCombineFunction<IN, OUT>, OUT> taskContext;

    private InMemoryPartition<IN> hasher;

    private GroupCombineFunction<IN, OUT> combiner;

    private TypeSerializer<IN> serializer;

    private TypeComparator<IN> hashComparator;

    private TypeComparator<IN> groupingComparator;

    private HashFunction hash = new hashFunction();

    private MemoryManager memManager;

    private Collector<OUT> output;

    private volatile boolean running = true;

    private boolean objectReuseEnabled = false;

    @Override
    public void setup(PactTaskContext<GroupCombineFunction<IN, OUT>, OUT> context) {
        this.taskContext = context;
        this.running = true;
    }

    @Override
    public int getNumberOfInputs() {
        return 1;
    }

    @Override
    public int getNumberOfDriverComparators() {
        return 2;
    }

    @Override
    public Class<GroupCombineFunction<IN, OUT>> getStubType() {
        final Class<GroupCombineFunction<IN, OUT>> cls = (Class<GroupCombineFunction<IN, OUT>>) (Class<?>) GroupCombineFunction.class;
        return cls;
    }

    @Override
    public void prepare() throws Exception {
        final DriverStrategy driverStrategy = this.taskContext.getTaskConfig().getDriverStrategy();
        if(driverStrategy != DriverStrategy.SORTED_GROUP_COMBINE){
            throw new Exception("Invalid strategy " + driverStrategy + " for " +
                    "group reduce combinder.");
        }

        this.memManager = this.taskContext.getMemoryManager();
        final int numMemoryPages = memManager.computeNumberOfPages(this.taskContext.getTaskConfig().getRelativeMemoryDriver());

        final TypeSerializerFactory<IN> serializerFactory = this.taskContext.getInputSerializer(0);
        this.serializer = serializerFactory.getSerializer();
        this.hashComparator = this.taskContext.getDriverComparator(0);
        this.groupingComparator = this.taskContext.getDriverComparator(1);
        this.combiner = this.taskContext.getStub();
        this.output = this.taskContext.getOutputCollector();

        final List<MemorySegment> memory = this.memManager.allocatePages(this.taskContext.getOwningNepheleTask(),
                numMemoryPages);

        // instantiate a fix-length in-place hasher, if possible, otherwise the out-of-place hasher
        if (this.hashComparator.supportsSerializationWithKeyNormalization() &&
                this.serializer.getLength() > 0 && this.serializer.getLength() <= THRESHOLD_FOR_IN_PLACE_SORTING)
        {
            //implement inMemoryHasher
        } else {
            //implement outOfMemHasher
        }

        ExecutionConfig executionConfig = taskContext.getExecutionConfig();
        this.objectReuseEnabled = executionConfig.isObjectReuseEnabled();

        if (LOG.isDebugEnabled()) {
            LOG.debug("GroupReduceCombineDriver object reuse: " + (this.objectReuseEnabled ? "ENABLED" : "DISABLED") + ".");
        }
    }

    @Override
    public void run() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Combiner starting.");
        }
        //Read memSegment and apply combine function on buckets
    }

    @Override
    public void cleanup() throws Exception {
        
    }

    @Override
    public void cancel() {
        this.running = false;

    }
}
