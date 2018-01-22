package org.hive.propolis;


import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.Map;

@Description(
        name = "Aggregate Maps in a group",
        value = "_FUNC_(input_map Map<U, T extends Numeric>): Map<U, Long|Double>\n"
                + "-- aggregates (merges) Maps for each group, run describe extended for more details",
        extended = "\nFor a group of IDs, sums up maps values for common keys adding new ones if not exists."
                + "\nUsage:\n"
                + "> select group_column, _FUNC_(map_column)\n"
                + ">   from your_table\n"
                + ">  group by group_column\n"
)

public class MergeMapsUDAF extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(SumMapsUDAF.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] tis) throws SemanticException {

        if (tis.length != 1) {
            throw new UDFArgumentTypeException(0, "Exactly one argument is expected.");
        }
        if (tis[0].getCategory() != ObjectInspector.Category.MAP) {
            throw new UDFArgumentTypeException(0, "The first argument must by Map type");
        }

        MapTypeInfo map_type = (MapTypeInfo) tis[0];
        TypeInfo value_type = map_type.getMapValueTypeInfo();

        if (value_type.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Map value must by Primitive");
        }

        switch (((PrimitiveTypeInfo) value_type).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return new MergeLongMapEvaluator();
            case TIMESTAMP:
            case FLOAT:
            case DOUBLE:
                return new MergeDoubleMapEvaluator();
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric Map value type arguments are accepted but "
                                + value_type.getTypeName() + " is passed.");
        }
    }


    // Long type:
    public static class MergeLongMapEvaluator extends GenericUDAFSumEvaluator<Long> {

        @Override
        protected PrimitiveObjectInspector getReturnValueType() {
            return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        }

        @Override
        protected Long getMapValue(Object map_value, PrimitiveObjectInspector poi) {
            return PrimitiveObjectInspectorUtils.getLong(map_value, poi);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggBuffer<Long> map_buffer = new AggBuffer<Long>();
            reset(map_buffer);
            return map_buffer;
        }
    }


    // Double type:
    public static class MergeDoubleMapEvaluator extends GenericUDAFSumEvaluator<Double> {

        @Override
        protected PrimitiveObjectInspector getReturnValueType() {
            return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        }

        @Override
        protected Double getMapValue(Object map_value, PrimitiveObjectInspector poi) {
            return PrimitiveObjectInspectorUtils.getDouble(map_value, poi);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggBuffer<Double> map_buffer = new AggBuffer<Double>();
            reset(map_buffer);
            return map_buffer;
        }
    }

    // Abstract aggregation buffer class:
    private static class AggBuffer<T extends Number> extends GenericUDAFEvaluator.AbstractAggregationBuffer {
        Map<Object, T> buffer;

        protected AggBuffer() {
            buffer = new HashMap<>();
        }
    }

    // Abstract Evaluator base class:
    public static abstract class GenericUDAFSumEvaluator<T extends Number> extends GenericUDAFEvaluator {

        protected transient StandardMapObjectInspector mapOI;
        protected transient PrimitiveObjectInspector keyOI;
        protected transient PrimitiveObjectInspector valueOI;
        protected transient AbstractPrimitiveWritableObjectInspector keyOutputTypeOI;

        protected abstract PrimitiveObjectInspector getReturnValueType();
        protected abstract T getMapValue(Object map_value, PrimitiveObjectInspector poi);

        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);

            mapOI = (StandardMapObjectInspector) parameters[0];
            keyOI = (PrimitiveObjectInspector) mapOI.getMapKeyObjectInspector();
            valueOI = (PrimitiveObjectInspector) mapOI.getMapValueObjectInspector();
            keyOutputTypeOI = (AbstractPrimitiveWritableObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(keyOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);

            return ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI,
                    getReturnValueType());
        }

        static class MapBuffer extends AbstractAggregationBuffer {
            Map<Object, Double> buffer = new HashMap<>();
        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((AggBuffer<T>) ab).buffer.clear();
        }

        @Override
        public void iterate(AggregationBuffer ab, Object[] parameters)  throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                AggBuffer<T> agg = (AggBuffer<T>) ab;
                Map<Object, Object> o = (Map<Object, Object>) mapOI.getMap(p);
                if (!o.isEmpty()) {
                    mapAppend(agg.buffer, o);
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer ab) throws HiveException {
            AggBuffer<T> agg = (AggBuffer<T>) ab;
            return Maps.newHashMap(agg.buffer);
        }

        @Override
        public void merge(AggregationBuffer ab, Object p) throws HiveException {
            AggBuffer<T> agg = (AggBuffer<T>) ab;
            @SuppressWarnings("unchecked")
            Map<Object, Object> obj = (Map<Object, Object>) mapOI.getMap(p);
            mapAppend(agg.buffer, obj);
        }

        @Override
        public Object terminate(AggregationBuffer ab)  throws HiveException {
            AggBuffer<T> agg = (AggBuffer<T>) ab;
            return Maps.newHashMap(agg.buffer);
        }

        protected T add(T x, T y) {

            if (x == null || y == null) {
                return null;
            }

            if (x instanceof Double) {
                return (T) new Double(x.doubleValue() + y.doubleValue());
            } else if (x instanceof Long) {
                return (T) new Long(x.longValue() + y.longValue());
            } else {
                throw new IllegalArgumentException("Type " + x.getClass() + " is not supported by this method");
            }
        }

        protected void mapAppend(Map<Object, T> m, Map<Object, Object> from)  {
            if (from == null) {
                return;
            }
            for(Map.Entry<Object, Object> entry : from.entrySet()) {
                Object entry_key = entry.getKey();
                Object entry_value = entry.getValue();

                if (entry_key == null || entry_value == null) {
                    continue;
                }

                Object key = keyOutputTypeOI.copyObject(keyOI.getPrimitiveWritableObject(entry.getKey()));
                T value = getMapValue(entry_value, valueOI);

                if (m.containsKey(key)) {
                    value = add(m.get(key), value);
                }
                m.put(key, value);
            }
        }
    }
}
