package org.hive.propolis;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.*;

import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.*;


@Description(name = "Avg columns element-wise",
        value = "_FUNC_[T <: Numeric](arg: T*): Array[Double] - Returns a new array with each element averaged."
)

public class MultipleAvgUDAF extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(MultipleAvgUDAF.class.getName());
    static final String COUNT_FIELD = "count";
    static final String SUM_FILED = "sum";

    public MultipleAvgUDAF() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        Set<PrimitiveObjectInspector.PrimitiveCategory> column_types = new HashSet<>();

        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i-1,
                        "Only primitive types are accepted but array of "
                                + parameters[i].getTypeName() + " was passed as parameter 1.");
            }
            column_types.add(((PrimitiveTypeInfo) parameters[i]).getPrimitiveCategory());
        }

        Set<PrimitiveObjectInspector.PrimitiveCategory> work_types = new HashSet<>(
                Arrays.asList(BYTE, SHORT, INT, LONG, TIMESTAMP, FLOAT, DOUBLE, VOID)
        );

        if (work_types.containsAll(column_types)) {
            return new GenericUDAFAvgMultipleColumns();
        } else {
            throw new UDFArgumentTypeException(0, "All aggregation columns must be Numeric");
        }
    }


    public static class GenericUDAFAvgMultipleColumns extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE
        private PrimitiveObjectInspector elementOI;
        private List<ObjectInspectorConverters.Converter> converters = new ArrayList<>();
        // For PARTIAL2 and FINAL
        private transient StructObjectInspector soi;
        private transient StructField countField;
        private transient StructField sumField;
        private transient ListObjectInspector counterFieldOI;
        private LongObjectInspector countOI;
        private transient ListObjectInspector containerFieldOI;
        private DoubleObjectInspector sumOI;
        // For PARTIAL1 and PARTIAL2
        protected transient Object[] partialResult;


        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);
            elementOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

            // init input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                for (int i = 0; i < parameters.length; i++) {
                    converters.add(
                            ObjectInspectorConverters.getConverter(parameters[i],
                                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector));
                }
            } else {
                // Object Inspector for aggregation buffer:
                soi = (StructObjectInspector) parameters[0];

                sumField = soi.getStructFieldRef(SUM_FILED);
                containerFieldOI = (ListObjectInspector) sumField.getFieldObjectInspector();
                sumOI = (DoubleObjectInspector) containerFieldOI.getListElementObjectInspector();

                countField = soi.getStructFieldRef(COUNT_FIELD);
                counterFieldOI = (ListObjectInspector) countField.getFieldObjectInspector();
                countOI = (LongObjectInspector) counterFieldOI.getListElementObjectInspector();
            }

            // init output
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                // The output of a partial aggregation is a struct containing
                // a list of "long" counts and a list "long" sums.
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableLongObjectInspector));
                foi.add(ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));

                ArrayList<String> fname = new ArrayList<String>();
                fname.add(COUNT_FIELD);
                fname.add(SUM_FILED);
                // index 1 set by child
                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            } else {
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector
                );
            }
        }

        class ArrayAggregationBuffer implements AggregationBuffer {
            List<LongWritable> counter;
            List<DoubleWritable> container;

            protected ArrayAggregationBuffer() {
                counter = new ArrayList<>();
                container = new ArrayList<>();
            }
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ArrayAggregationBuffer) agg).counter.clear();
            ((ArrayAggregationBuffer) agg).container.clear();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ArrayAggregationBuffer ret = new ArrayAggregationBuffer();
            return ret;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            List<Double> args = new ArrayList<>();

            for (int i = 0; i < parameters.length; i++) {
                Double col = null;
                if (parameters[i] != null) {
                    col = (Double) converters.get(i).convert(parameters[i]);
                }
                args.add(col);
            }

            ArrayAggregationBuffer myagg = (ArrayAggregationBuffer) agg;
            addToBuffer(args, myagg);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            ArrayAggregationBuffer myagg = (ArrayAggregationBuffer) agg;
            final int bufferSize = myagg.container.size();
            assert bufferSize == myagg.counter.size();

            List<Object> ret_sum = new ArrayList<Object>(bufferSize);
            ret_sum.addAll(myagg.container);
            List<Object> ret_cnt = new ArrayList<Object>(bufferSize);
            ret_cnt.addAll(myagg.counter);

            Object[] res = new Object[2];
            res[0] = ret_cnt;
            res[1] = ret_sum;

            return res;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {

            List<Object> sums = (List<Object>) containerFieldOI.getList(soi.getStructFieldData(partial, sumField));
            List<Object> counts = (List<Object>) counterFieldOI.getList(soi.getStructFieldData(partial, countField));

            if (sums != null && counts != null) {
                ArrayAggregationBuffer myagg = (ArrayAggregationBuffer) agg;
                doMerge(sums, counts, myagg);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ArrayAggregationBuffer myagg = (ArrayAggregationBuffer) agg;
            ArrayList<DoubleWritable> ret = new ArrayList<>();
            assert myagg.container.size() == myagg.counter.size();
            for (int i = 0; i < myagg.container.size(); i++) {
                DoubleWritable lw_vals = myagg.container.get(i);
                LongWritable lw_cnt = myagg.counter.get(i);
                final Double vals = lw_vals.get();
                final Double cnt = new Double(lw_cnt.get());
                if (cnt != 0) {
                    ret.add(new DoubleWritable(vals/cnt));
                } else {
                    ret.add(null);
                }
            }
            return ret;
        }

        private void addToBuffer(List<Double> p, ArrayAggregationBuffer myagg) {

            final int currentSize = myagg.container.size();
            assert currentSize == myagg.counter.size();

            for (int i = 0; i < p.size(); i++) {
                Double v = p.get(i);
                if (v != null) {
                    if (i >= currentSize) {
                        myagg.container.add(new DoubleWritable(v));
                        myagg.counter.add(new LongWritable(1));
                    } else {
                        DoubleWritable current = myagg.container.get(i);
                        current.set(current.get() + v);
                        LongWritable cnt = myagg.counter.get(i);
                        cnt.set(cnt.get() + 1);
                    }
                } else {
                    if (i >= currentSize) {
                        myagg.container.add(new DoubleWritable(0));
                        myagg.counter.add(new LongWritable(0));
                    }
                }
            }
        }

        private void doMerge(List<Object> sums, List<Object> counts, ArrayAggregationBuffer myagg) {

            final int currentSize = myagg.container.size();
            assert currentSize == myagg.counter.size();
            assert sums.size() == counts.size();

            for (int i = 0; i < sums.size(); i++) {
                final DoubleWritable sum_value = (DoubleWritable) elementOI.getPrimitiveJavaObject(sums.get(i));
                final LongWritable cnt_value = (LongWritable) elementOI.getPrimitiveJavaObject(counts.get(i));
                if (i >= currentSize) {
                    myagg.container.add(sum_value);
                    myagg.counter.add(cnt_value);
                } else {
                    DoubleWritable current = myagg.container.get(i);
                    current.set(current.get() + sum_value.get());
                    LongWritable cnt = myagg.counter.get(i);
                    cnt.set(cnt.get() + cnt_value.get());
                }
            }
        }
    }
}
