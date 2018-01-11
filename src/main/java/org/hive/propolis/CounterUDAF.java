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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

import java.util.HashMap;
import java.util.Map;


@Description(name = "Counter UDAF",
        value = "_FUNC_(x1[, x2, ...]) - Count occurrences of each input value and " +
                "return a Map<value, count> (like Python Counter class).")

public class CounterUDAF extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(CounterUDAF.class.getName());

    public CounterUDAF() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {


        for (int i = 0; i < parameters.length; i++) {
            if (parameters[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                        "Only Primitive type arguments are accepted but "
                                + parameters[i].getTypeName() + " was passed");
            }
        }

        return new GenericUDAFCounterEvaluator();
    }

    public static class GenericUDAFCounterEvaluator extends GenericUDAFEvaluator {

        // input
        private PrimitiveObjectInspector inputOI;

        // final output
        private StandardMapObjectInspector counterOI;
        // partial output
        private StandardMapObjectInspector bufferOI;


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);

            if (!(parameters[0] instanceof StandardMapObjectInspector)) {
                inputOI = (PrimitiveObjectInspector) parameters[0];

                counterOI = ObjectInspectorFactory.getStandardMapObjectInspector(
                        (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI),
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector);
                return counterOI;

            } else {
                bufferOI = (StandardMapObjectInspector) parameters[0];
                inputOI = (PrimitiveObjectInspector) bufferOI.getMapKeyObjectInspector();

                counterOI = ObjectInspectorFactory.getStandardMapObjectInspector(
                        (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI),
                        PrimitiveObjectInspectorFactory.writableIntObjectInspector);

                return counterOI;
            }
        }

        static class MapAggregationBuffer implements AggregationBuffer {
            Map<Object, IntWritable> container;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MapAggregationBuffer) agg).container = new HashMap<Object, IntWritable>();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MapAggregationBuffer ret = new MapAggregationBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {

            for (Object p : parameters) {
                if (p != null) {
                    MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
                    addToBuffer(inputOI.getPrimitiveWritableObject(p), 1, myagg);
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            MapAggregationBuffer myagg = (MapAggregationBuffer) agg;

            HashMap<Object, IntWritable> partialResult = (HashMap<Object, IntWritable>) bufferOI.getMap(partial);
            for (Map.Entry<Object, IntWritable> i : partialResult.entrySet()) {
                addToBuffer(i.getKey(), i.getValue().get(), myagg);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
            HashMap<Object, IntWritable> ret = new HashMap<Object, IntWritable>();
            ret.putAll(myagg.container);
            return ret;
        }

        private void addToBuffer(Object v, int count, MapAggregationBuffer myagg) {
            Object vCopy = ObjectInspectorUtils.copyToStandardObject(v, this.counterOI.getMapKeyObjectInspector());

            if (myagg.container.containsKey(vCopy)) {
                IntWritable c = myagg.container.get(vCopy);
                c.set(c.get() + count);
            } else {
                myagg.container.put(vCopy, new IntWritable(count));
            }
        }
    }
}
