package org.hive.propolis.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;


@Description(name = "hbase_get_family",
        value = "_FUNC_(key_col, hbase_tbl, family[, map_value_type]): Map<String, T> - Simple Hbase value getter")


public class HBaseGetFamilyUDF extends GenericUDF {

    private static int NUM_ARGS;

    private transient PrimitiveObjectInspector keyOI;
    private transient StandardMapObjectInspector outputOI;

    private Configuration config;
    private Connection connection;

    private static String table_name;
    private static byte[] Hfamily;
    private static String map_value_type = null;

    private transient PrimitiveObjectInspectorConverter.TextConverter converter;

    private static final Logger LOG = LoggerFactory.getLogger(HBaseGetFamilyUDF.class.getName());

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        NUM_ARGS = arguments.length;
        if (NUM_ARGS < 3 || NUM_ARGS > 4) {
            throw new UDFArgumentLengthException(
                    "HBaseGetFamily() accepts exactly 3 or 4 arguments.");
        }

        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "The first argument must be Primitive, but "
                            + arguments[0].getCategory().name()
                            + " was passed.");
        }

        keyOI = (PrimitiveObjectInspector) arguments[0];

        for (int i = 1; i <= NUM_ARGS-2; ++i) {
            if (((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory() !=
                    PrimitiveObjectInspector.PrimitiveCategory.STRING ||
                    !ObjectInspectorUtils.isConstantObjectInspector(arguments[i])) {
                throw new UDFArgumentTypeException(i+1, "The " + i + "th " +
                        "argument of HBaseGetFamily() must be a constant string but " +
                        arguments[i].toString() + " was given.");
            }
        }

        table_name = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue().toString();
        Hfamily = ((ConstantObjectInspector) arguments[2]).getWritableConstantValue().toString().getBytes();

        if (NUM_ARGS == 4) {

            if (((PrimitiveObjectInspector) arguments[3]).getPrimitiveCategory() !=
                    PrimitiveObjectInspector.PrimitiveCategory.STRING ||
                    !ObjectInspectorUtils.isConstantObjectInspector(arguments[3])) {
                throw new UDFArgumentTypeException(4, "The " + 4 + "th " +
                        "argument of HBaseGetFamily() must be a constant string but " +
                        arguments[3].toString() + " was given.");
            }
            map_value_type = ((ConstantObjectInspector) arguments[3]).getWritableConstantValue().toString().toLowerCase();
            switch (map_value_type) {
                case "int":
                    outputOI = ObjectInspectorFactory.getStandardMapObjectInspector(
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                            PrimitiveObjectInspectorFactory.javaIntObjectInspector);
                    break;
                case "long":
                case "bigint":
                    outputOI = ObjectInspectorFactory.getStandardMapObjectInspector(
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                            PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                    break;
                case "double":
                case "float":
                    outputOI = ObjectInspectorFactory.getStandardMapObjectInspector(
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
                    break;
                case "string":
                    outputOI = ObjectInspectorFactory.getStandardMapObjectInspector(
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                    break;
                default:
                    throw new UDFArgumentException("Unrecognized map value type " + map_value_type);
            }
        } else {
            map_value_type = "string";
            outputOI = ObjectInspectorFactory.getStandardMapObjectInspector(
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        return outputOI;
    }

    @Override
    public void configure(MapredContext context) {
        super.configure(context);
        this.config = HBaseConfiguration.create(context.getJobConf());
        try {
            LOG.warn("[configure] Creating new connection");
            this.connection = ConnectionFactory.createConnection(this.config);
        } catch (IOException ioException) {
            throw new RuntimeException("Failed to open a connection to HBase", ioException);
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        Object keyObject = arguments[0].get();
        if (keyObject == null) {
            return null;
        }

        String key = keyObject.toString();
        if (key == null) {
            return null;
        }

        if (this.config == null) {
            LOG.warn("A Configuration object wasn't passed to us. Building our own HBaseConfiguration object, "
                    + "but we may have mis-configurations if there are specific overrides passed into the query session");
            this.config = HBaseConfiguration.create();
        }

        Get getter = new Get(Bytes.toBytes(key));
        getter.addFamily(Hfamily);

        if (this.connection == null) {
            try {
                LOG.warn(">>> Creating new connection");
                this.connection = ConnectionFactory.createConnection(this.config);
            } catch (IOException ioException) {
                throw new HiveException("Failed to open a connection to HBase", ioException);
            }
        }

        try {
            Table table = this.connection.getTable(TableName.valueOf(table_name));
            Result result = table.get(getter);
            NavigableMap<byte[], byte[]> family_map = result.getFamilyMap(Hfamily);
            if (family_map == null) {
                return null;
            }

            switch (map_value_type) {
                case "int":
                    Map<String, Integer> int_output = new HashMap<>();
                    for (Map.Entry<byte[], byte[]> mapEntry: family_map.entrySet()) {
                        int_output.put(Bytes.toString(mapEntry.getKey()),
                                Bytes.toInt(mapEntry.getValue()));
                    }
                    return int_output;
                case "bigint":
                case "long":
                    Map<String, Long> long_output = new HashMap<>();
                    for (Map.Entry<byte[], byte[]> mapEntry: family_map.entrySet()) {
                        long_output.put(Bytes.toString(mapEntry.getKey()),
                                Bytes.toLong(mapEntry.getValue()));
                    }
                    return long_output;
                case "float":
                case "double":
                    Map<String, Double> double_output = new HashMap<>();
                    for (Map.Entry<byte[], byte[]> mapEntry: family_map.entrySet()) {
                        double_output.put(Bytes.toString(mapEntry.getKey()),
                                Bytes.toDouble(mapEntry.getValue()));
                    }
                    return double_output;
                default:
                    Map<String, String> string_output = new HashMap<>();
                    for (Map.Entry<byte[], byte[]> mapEntry: family_map.entrySet()) {
                        string_output.put(Bytes.toString(mapEntry.getKey()),
                                Bytes.toString(mapEntry.getValue()));
                    }
                    return string_output;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == NUM_ARGS);
        return getStandardDisplayString("hbase_get_family", children);
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (this.connection != null) {
            this.connection.close();
        }
    }
}
