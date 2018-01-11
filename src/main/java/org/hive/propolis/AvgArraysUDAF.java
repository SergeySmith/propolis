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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.ArrayList;
import java.util.List;

@Description(name = "Avg Arrays element-wise",
        value = "_FUNC_[T <: Numeric](arg: Array[T]): Array[Double] - Returns a new array with each element averaged."
)

public class AvgArraysUDAF extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(AvgArraysUDAF.class.getName());
    static final String COUNT_FIELD = "count";
    static final String SUM_FILED = "sum";

    public AvgArraysUDAF() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {

        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Only one arguments are expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(0,
                    "Only list argument are accepted but "
                            + parameters[0].getTypeName() + " was passed as parameter 1.");
        }

        TypeInfo elementTypeInfo = ((ListTypeInfo) parameters[0]).getListElementTypeInfo();

        if (elementTypeInfo.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive array are accepted but array of "
                            + elementTypeInfo.getTypeName() + " was passed as parameter 1.");
        }


        switch (((PrimitiveTypeInfo) elementTypeInfo).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case TIMESTAMP:
                return new GenericUDAFAvgLongArray();
            case FLOAT:
            case DOUBLE:
                return new GenericUDAFAvgDoubleArray();
            case STRING:
            case DECIMAL:
            case BOOLEAN:
                throw new UDFArgumentTypeException(0, "Unsupported yet");
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric type arrays are accepted but array of "
                                + elementTypeInfo.getTypeName() + " is passed.");
        }
    }

    public static class GenericUDAFAvgLongArray extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE
        private transient ListObjectInspector input_listOI;
        private PrimitiveObjectInspector elementOI;
        transient private ObjectInspectorConverters.Converter inputConverter;
        // For PARTIAL2 and FINAL
        private transient StructObjectInspector soi;
        private transient StructField countField;
        private transient StructField sumField;
        private transient ListObjectInspector counterFieldOI;
        private LongObjectInspector countOI;
        private transient ListObjectInspector containerFieldOI;
        protected LongObjectInspector sumOI;
        // For PARTIAL1 and PARTIAL2
        protected transient Object[] partialResult;


        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);

            elementOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

            // init input
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                input_listOI = (ListObjectInspector) parameters[0];
                inputConverter = ObjectInspectorConverters.getConverter(
                        parameters[0],
                        ObjectInspectorFactory.getStandardListObjectInspector(
                                PrimitiveObjectInspectorFactory.javaLongObjectInspector
                        )
                );
            } else {
                // Object Inspector for aggregation buffer:
                soi = (StructObjectInspector) parameters[0];

                countField = soi.getStructFieldRef(COUNT_FIELD);
                counterFieldOI = (ListObjectInspector) countField.getFieldObjectInspector();
                countOI = (LongObjectInspector) counterFieldOI.getListElementObjectInspector();

                sumField = soi.getStructFieldRef(SUM_FILED);
                containerFieldOI = (ListObjectInspector) sumField.getFieldObjectInspector();
                sumOI = (LongObjectInspector) containerFieldOI.getListElementObjectInspector();
            }

            // init output
            if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableLongObjectInspector));
                foi.add(ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableLongObjectInspector));

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
            List<LongWritable> container;

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
            assert (parameters.length == 1);
            List<Object> p = (List<Object>) inputConverter.convert(parameters[0]);
            if (p != null) {
                ArrayAggregationBuffer myagg = (ArrayAggregationBuffer) agg;
                addToBuffer(p, myagg);
            }
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

            List<Object> counts = (List<Object>) counterFieldOI.getList(soi.getStructFieldData(partial, countField));
            List<Object> sums = (List<Object>) containerFieldOI.getList(soi.getStructFieldData(partial, sumField));

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
                LongWritable lw_vals = myagg.container.get(i);
                LongWritable lw_cnt = myagg.counter.get(i);
                final Double vals = new Double(lw_vals.get());
                final Double cnt = new Double(lw_cnt.get());
                if (cnt != 0) {
                    ret.add(new DoubleWritable(vals/cnt));
                } else {
                    ret.add(null);
                }
            }
            return ret;
        }

        private void addToBuffer(List<Object> p, ArrayAggregationBuffer myagg) {

            final int currentSize = myagg.container.size();
            assert currentSize == myagg.counter.size();

            for (int i = 0; i < p.size(); i++) {
                Object list_elem = p.get(i);
                if (list_elem != null) {
                    final long v = PrimitiveObjectInspectorUtils.getLong(p.get(i), elementOI);
                    if (i >= currentSize) {
                        myagg.container.add(new LongWritable(v));
                        myagg.counter.add(new LongWritable(1));
                    } else {
                        LongWritable current = myagg.container.get(i);
                        current.set(current.get() + v);
                        LongWritable cnt = myagg.counter.get(i);
                        cnt.set(cnt.get() + 1);
                    }
                } else {
                    if (i >= currentSize) {
                        myagg.container.add(new LongWritable(0));
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
                final LongWritable sum_value = (LongWritable) elementOI.getPrimitiveJavaObject(sums.get(i));
                final LongWritable cnt_value = (LongWritable) elementOI.getPrimitiveJavaObject(counts.get(i));
                if (i >= currentSize) {
                    myagg.container.add(sum_value);
                    myagg.counter.add(cnt_value);
                } else {
                    LongWritable current = myagg.container.get(i);
                    current.set(current.get() + sum_value.get());
                    LongWritable cnt = myagg.counter.get(i);
                    cnt.set(cnt.get() + cnt_value.get());
                }
            }
        }
    }



    public static class GenericUDAFAvgDoubleArray extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE
        private transient ListObjectInspector input_listOI;
        private PrimitiveObjectInspector elementOI;
        transient private ObjectInspectorConverters.Converter inputConverter;
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
                input_listOI = (ListObjectInspector) parameters[0];
                inputConverter = ObjectInspectorConverters.getConverter(
                        parameters[0],
                        ObjectInspectorFactory.getStandardListObjectInspector(
                                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
                        )
                );
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
            assert (parameters.length == 1);
            List<Object> p = (List<Object>) inputConverter.convert(parameters[0]);
            if (p != null) {
                ArrayAggregationBuffer myagg = (ArrayAggregationBuffer) agg;
                addToBuffer(p, myagg);
            }
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

        private void addToBuffer(List<Object> p, ArrayAggregationBuffer myagg) {

            final int currentSize = myagg.container.size();
            assert currentSize == myagg.counter.size();

            for (int i = 0; i < p.size(); i++) {
                Object list_elem = p.get(i);
                if (list_elem != null) {
                    final double v = PrimitiveObjectInspectorUtils.getDouble(p.get(i), elementOI);
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
