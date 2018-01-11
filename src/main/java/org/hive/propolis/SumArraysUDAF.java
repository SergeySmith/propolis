package org.hive.propolis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.ArrayList;
import java.util.List;

@Description(name = "Sum Arrays elements",
        value = "_FUNC_[T <: Numeric](arg: Array[T]): Array[T] - Returns a new array with each element summed up."
)

public class SumArraysUDAF extends AbstractGenericUDAFResolver {
    
    static final Log LOG = LogFactory.getLog(SumArraysUDAF.class.getName());

    public SumArraysUDAF() {
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
                return new GenericUDAFSumLongArray();
            case FLOAT:
            case DOUBLE:
                return new GenericUDAFSumDoubleArray();
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

    public static class GenericUDAFSumLongArray extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector elementOI;
        transient private ObjectInspectorConverters.Converter inputConverter;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            elementOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;

            inputConverter = ObjectInspectorConverters.getConverter(
                    parameters[0],
                    ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.javaLongObjectInspector
                    )
            );

            return ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.writableLongObjectInspector
            );
        }

        static class ArrayAggregationBuffer implements AggregationBuffer {
            List<LongWritable> container;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ArrayAggregationBuffer) agg).container = new ArrayList<LongWritable>();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ArrayAggregationBuffer ret = new ArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            merge(agg, parameters[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {

            List<Object> p = (List<Object>) inputConverter.convert(partial);
            if (p != null) {
                ArrayAggregationBuffer myagg = (ArrayAggregationBuffer) agg;
                sumArray(p, myagg);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ArrayAggregationBuffer myagg = (ArrayAggregationBuffer) agg;
            ArrayList<LongWritable> ret = new ArrayList<LongWritable>();
            ret.addAll(myagg.container);
            return ret;
        }

        private void sumArray(List<Object> p, ArrayAggregationBuffer myagg) {

            int currentSize = myagg.container.size();

            for (int i = 0; i < p.size(); i++) {
                Object list_elem = p.get(i);
                if (list_elem != null) {
                    long v = PrimitiveObjectInspectorUtils.getLong(p.get(i), elementOI);
                    if (i >= currentSize) {
                        myagg.container.add(new LongWritable(v));
                    } else {
                        LongWritable current = myagg.container.get(i);
                        current.set(current.get() + v);
                    }
                } else {
                    if (i >= currentSize) {
                        myagg.container.add(new LongWritable(0));
                    }
                }
            }
        }
    }

    public static class GenericUDAFSumDoubleArray extends GenericUDAFEvaluator {

        private PrimitiveObjectInspector elementOI;
        transient private ObjectInspectorConverters.Converter inputConverter;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            elementOI = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

            inputConverter = ObjectInspectorConverters.getConverter(
                    parameters[0],
                    ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
                    )
            );

            return ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.writableDoubleObjectInspector
            );
        }

        static class ArrayAggregationBuffer implements AggregationBuffer {
            List<DoubleWritable> container;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ArrayAggregationBuffer) agg).container = new ArrayList<DoubleWritable>();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ArrayAggregationBuffer ret = new ArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            merge(agg, parameters[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {

            List<Object> p = (List<Object>) inputConverter.convert(partial);
            if (p != null) {
                ArrayAggregationBuffer myagg = (ArrayAggregationBuffer) agg;
                sumArray(p, myagg);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ArrayAggregationBuffer myagg = (ArrayAggregationBuffer) agg;
            ArrayList<DoubleWritable> ret = new ArrayList<DoubleWritable>();
            ret.addAll(myagg.container);
            return ret;
        }

        private void sumArray(List<Object> p, ArrayAggregationBuffer myagg) {

            int currentSize = myagg.container.size();

            for (int i = 0; i < p.size(); i++) {
                Object list_elem = p.get(i);
                if (list_elem != null) {
                    double v = PrimitiveObjectInspectorUtils.getDouble(p.get(i), elementOI);
                    if (i >= currentSize) {
                        myagg.container.add(new DoubleWritable(v));
                    } else {
                        DoubleWritable current = myagg.container.get(i);
                        current.set(current.get() + v);
                    }
                } else {
                    if (i >= currentSize) {
                        myagg.container.add(new DoubleWritable(0.0));
                    }
                }
            }
        }
    }
}
