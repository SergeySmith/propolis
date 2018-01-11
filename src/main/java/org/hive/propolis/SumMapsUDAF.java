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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


@Description(
        name = "Aggregate Maps in a group",
        value = "_FUNC_(input_map Map<U, Int>[, theta Int, mode String (STRICT|NOSTRICT)]): Map<U, Int>\n"
                + "-- aggregates (merges) Maps for each group, run describe extended for more details",
        extended = "\nFor a group of IDs, sums up maps values for common keys adding new ones if not exists."
                + "\nNeeds either one or three arguments (the last two are optional)."
                + "\nIf threshold theta and mode are given then the function keeps keys"
                + "\nwith values >= theta in NOSTRICT mode and > theta in STRICT mode\n"
                + "\nUsage:\n"
                + "> select group_column, _FUNC_(map_column)\n"
                + ">   from your_table\n"
                + ">  group by group_column\n"
                + "\n or\n"
                + "> select group_column, _FUNC_(map_column, theta, 'NOSTRICT')\n"
                + ">   from your_table\n"
                + ">  group by group_column\n"
)

public class SumMapsUDAF extends AbstractGenericUDAFResolver {

    private static int theta = Integer.MIN_VALUE;
    private static String sign = "NOSTRICT";

    static final Log LOG = LogFactory.getLog(SumMapsUDAF.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] tis) throws SemanticException {

        if (tis[0].getCategory() != ObjectInspector.Category.MAP) {
            throw new UDFArgumentTypeException(0, "The first argument must by Map type");
        }

        MapTypeInfo map_type = (MapTypeInfo) tis[0];
        TypeInfo value_type = map_type.getMapValueTypeInfo();
        if (value_type.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Map value must by Primitive");
        }

        if (tis.length == 1) {
            switch (((PrimitiveTypeInfo) value_type).getPrimitiveCategory()) {
                case INT:
                    return new MapGroupSumEvaluator();
                case LONG:
                    return new MapGroupLongSumEvaluator();
                case FLOAT:
                    return new MapGroupFloatSumEvaluator();
                case DOUBLE:
                    return new MapGroupDoubleSumEvaluator();
                default:
                    throw new UDFArgumentTypeException(0,
                            "Only numeric Map value type arguments are accepted but "
                                    + value_type.getTypeName() + " is passed.");
            }
        }
        else if (tis.length == 3) {
            return new MapGroupConstrainedSumEvaluator();
        }
        else {
            throw new UDFArgumentTypeException(tis.length - 1, "Exactly one or three arguments is expected.");
        }

    }

    // Int type
    public static class MapGroupSumEvaluator extends GenericUDAFEvaluator {
        private MapObjectInspector originalDataOI;
        private IntObjectInspector valueOI;
        private PrimitiveObjectInspector keyOI;
        private AbstractPrimitiveWritableObjectInspector keyOutputTypeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            originalDataOI = (MapObjectInspector) parameters[0];
            keyOI = (PrimitiveObjectInspector) originalDataOI.getMapKeyObjectInspector();
            valueOI = (IntObjectInspector) originalDataOI.getMapValueObjectInspector();
            keyOutputTypeOI = (AbstractPrimitiveWritableObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(keyOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
            return ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI,
                    PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        }

        static class MapBuffer implements AggregationBuffer {
            Map<Object, Integer> map = new HashMap<>();
        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((MapBuffer) ab).map.clear();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MapBuffer();
        }

        private void mapAppend(Map<Object, Integer> m, Map<Object, Object> from)  {
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
                Integer value = valueOI.get(entry_value);

                if (m.containsKey(key)) {
                    value += m.get(key);
                }
                m.put(key, value);
            }
        }

        @Override
        public void iterate(AggregationBuffer ab, Object[] parameters)  throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                MapBuffer agg = (MapBuffer) ab;
                Map<Object, Object> o = (Map<Object, Object>) this.originalDataOI.getMap(p);
                if (!o.isEmpty()) {
                    mapAppend(agg.map, o);
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer ab) throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
        }

        @Override
        public void merge(AggregationBuffer ab, Object p) throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            @SuppressWarnings("unchecked")
            Map<Object, Object> obj = (Map<Object, Object>) this.originalDataOI.getMap(p);
            mapAppend(agg.map, obj);
        }

        @Override
        public Object terminate(AggregationBuffer ab)  throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
        }
    }

    // Long type:
    public static class MapGroupLongSumEvaluator extends GenericUDAFEvaluator {
        private MapObjectInspector originalDataOI;
        private LongObjectInspector valueOI;
        private PrimitiveObjectInspector keyOI;
        private AbstractPrimitiveWritableObjectInspector keyOutputTypeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            originalDataOI = (MapObjectInspector) parameters[0];
            keyOI = (PrimitiveObjectInspector) originalDataOI.getMapKeyObjectInspector();
            valueOI = (LongObjectInspector) originalDataOI.getMapValueObjectInspector();
            keyOutputTypeOI = (AbstractPrimitiveWritableObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(keyOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
            return ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI,
                    PrimitiveObjectInspectorFactory.javaLongObjectInspector);
        }

        static class MapBuffer implements AggregationBuffer {
            Map<Object, Long> map = new HashMap<>();
        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((MapBuffer) ab).map.clear();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MapBuffer();
        }

        protected void mapAppend(Map<Object, Long> m, Map<Object, Object> from)  {
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
                Long value = valueOI.get(entry_value);

                if (m.containsKey(key)) {
                    value += m.get(key);
                }
                m.put(key, value);
            }
        }

        @Override
        public void iterate(AggregationBuffer ab, Object[] parameters)  throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                MapBuffer agg = (MapBuffer) ab;
                Map<Object, Object> o = (Map<Object, Object>) this.originalDataOI.getMap(p);
                if (!o.isEmpty()) {
                    mapAppend(agg.map, o);
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer ab) throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
        }

        @Override
        public void merge(AggregationBuffer ab, Object p) throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            @SuppressWarnings("unchecked")
            Map<Object, Object> obj = (Map<Object, Object>) this.originalDataOI.getMap(p);
            mapAppend(agg.map, obj);
        }

        @Override
        public Object terminate(AggregationBuffer ab)  throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
        }
    }

    // Float type:
    public static class MapGroupFloatSumEvaluator extends GenericUDAFEvaluator {
        private MapObjectInspector originalDataOI;
        private FloatObjectInspector valueOI;
        private PrimitiveObjectInspector keyOI;
        private AbstractPrimitiveWritableObjectInspector keyOutputTypeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            originalDataOI = (MapObjectInspector) parameters[0];
            keyOI = (PrimitiveObjectInspector) originalDataOI.getMapKeyObjectInspector();
            valueOI = (FloatObjectInspector) originalDataOI.getMapValueObjectInspector();
            keyOutputTypeOI = (AbstractPrimitiveWritableObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(keyOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
            return ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI,
                    PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
        }

        static class MapBuffer implements AggregationBuffer {
            Map<Object, Float> map = new HashMap<>();
        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((MapBuffer) ab).map.clear();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MapBuffer();
        }

        protected void mapAppend(Map<Object, Float> m, Map<Object, Object> from)  {
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
                Float value = valueOI.get(entry_value);

                if (m.containsKey(key)) {
                    value += m.get(key);
                }
                m.put(key, value);
            }
        }

        @Override
        public void iterate(AggregationBuffer ab, Object[] parameters)  throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                MapBuffer agg = (MapBuffer) ab;
                Map<Object, Object> o = (Map<Object, Object>) this.originalDataOI.getMap(p);
                if (!o.isEmpty()) {
                    mapAppend(agg.map, o);
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer ab) throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
        }

        @Override
        public void merge(AggregationBuffer ab, Object p) throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            @SuppressWarnings("unchecked")
            Map<Object, Object> obj = (Map<Object, Object>) this.originalDataOI.getMap(p);
            mapAppend(agg.map, obj);
        }

        @Override
        public Object terminate(AggregationBuffer ab)  throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
        }
    }

    // Double type:
    public static class MapGroupDoubleSumEvaluator extends GenericUDAFEvaluator {
        private MapObjectInspector originalDataOI;
        private DoubleObjectInspector valueOI;
        private PrimitiveObjectInspector keyOI;
        private AbstractPrimitiveWritableObjectInspector keyOutputTypeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            originalDataOI = (MapObjectInspector) parameters[0];
            keyOI = (PrimitiveObjectInspector) originalDataOI.getMapKeyObjectInspector();
            valueOI = (DoubleObjectInspector) originalDataOI.getMapValueObjectInspector();
            keyOutputTypeOI = (AbstractPrimitiveWritableObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(keyOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
            return ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI,
                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
        }

        static class MapBuffer implements AggregationBuffer {
            Map<Object, Double> map = new HashMap<>();
        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((MapBuffer) ab).map.clear();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MapBuffer();
        }

        protected void mapAppend(Map<Object, Double> m, Map<Object, Object> from)  {
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
                Double value = valueOI.get(entry_value);

                if (m.containsKey(key)) {
                    value += m.get(key);
                }
                m.put(key, value);
            }
        }

        @Override
        public void iterate(AggregationBuffer ab, Object[] parameters)  throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                MapBuffer agg = (MapBuffer) ab;
                Map<Object, Object> o = (Map<Object, Object>) this.originalDataOI.getMap(p);
                if (!o.isEmpty()) {
                    mapAppend(agg.map, o);
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer ab) throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
        }

        @Override
        public void merge(AggregationBuffer ab, Object p) throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            @SuppressWarnings("unchecked")
            Map<Object, Object> obj = (Map<Object, Object>) this.originalDataOI.getMap(p);
            mapAppend(agg.map, obj);
        }

        @Override
        public Object terminate(AggregationBuffer ab)  throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
        }
    }


    // ////////////////////////// With threshold on Map values ////////////////////////// //

    public static class MapGroupConstrainedSumEvaluator extends GenericUDAFEvaluator {
        private MapObjectInspector originalDataOI;
        private IntObjectInspector valueOI;
        private PrimitiveObjectInspector keyOI;
        private AbstractPrimitiveWritableObjectInspector keyOutputTypeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (parameters.length == 3) {
                WritableConstantIntObjectInspector thetaOI = (WritableConstantIntObjectInspector) parameters[1];
                theta = thetaOI.getWritableConstantValue().get();
                WritableConstantStringObjectInspector signOI = (WritableConstantStringObjectInspector) parameters[2];
                sign = signOI.getWritableConstantValue().toString();

                if (!sign.equals("STRICT") && !sign.equals("NOSTRICT")) {
                    throw new HiveException("The third argument must be equal [STRICT|NOSTRICT]");
                }
            }

            originalDataOI = (MapObjectInspector) parameters[0];
            keyOI = (PrimitiveObjectInspector) originalDataOI.getMapKeyObjectInspector();
            valueOI = (IntObjectInspector) originalDataOI.getMapValueObjectInspector();
            keyOutputTypeOI = (AbstractPrimitiveWritableObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(keyOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.WRITABLE);
            return ObjectInspectorFactory.getStandardMapObjectInspector(keyOutputTypeOI,
                    PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        }

        static class MapBuffer implements AggregationBuffer {
            Map<Object, Integer> map = new HashMap<Object, Integer>();
        }

        @Override
        public void reset(AggregationBuffer ab) throws HiveException {
            ((MapBuffer) ab).map.clear();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MapBuffer();
        }

        protected void mapAppend(Map<Object, Integer> m, Map<Object, Object> from)  {
            if (from == null) {
                return;
            }

            for(Map.Entry<Object, Object> entry : from.entrySet()) {
                final Object entry_key = entry.getKey();
                final Object entry_value = entry.getValue();

                if (entry_key == null || entry_value == null) {
                    continue;
                }

                final Object key = keyOutputTypeOI.copyObject(keyOI.getPrimitiveWritableObject(entry_key));
                Integer value = valueOI.get(entry_value);

                if (m.containsKey(key)) {
                    value += m.get(key);
                }
                m.put(key, value);
            }
        }

        protected void clearBuffer(Map<Object, Integer> buf_map) {
            Iterator it = buf_map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Object, Integer> entry = (Map.Entry<Object, Integer>) it.next();
                Integer value = entry.getValue();
                if (sign.equals("STRICT")) {
                    if (value <= theta) it.remove();
                }
                else {
                    if (value < theta) it.remove();
                }
            }
        }

        @Override
        public void iterate(AggregationBuffer ab, Object[] parameters)  throws HiveException {
            assert (parameters.length == 3);
            Object p = parameters[0];
            if (p != null) {
                MapBuffer agg = (MapBuffer) ab;
                Map<Object, Object> o = (Map<Object, Object>) this.originalDataOI.getMap(p);

                if (!o.isEmpty()) {
                    mapAppend(agg.map, o);
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer ab) throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            return Maps.newHashMap(agg.map);
        }

        @Override
        public void merge(AggregationBuffer ab, Object p) throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            @SuppressWarnings("unchecked")
            Map<Object, Object> obj = (Map<Object, Object>) this.originalDataOI.getMap(p);
            mapAppend(agg.map, obj);
        }

        @Override
        public Object terminate(AggregationBuffer ab)  throws HiveException {
            MapBuffer agg = (MapBuffer) ab;
            clearBuffer(agg.map);

            HashMap<Object, Integer> output = Maps.newHashMap(agg.map);

            if (output.isEmpty()) {
                return null;
            } else {
                return output;
            }
        }
    }
}
