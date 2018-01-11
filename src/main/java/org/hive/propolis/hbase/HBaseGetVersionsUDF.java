package org.hive.propolis.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
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
import java.util.ArrayList;
import java.util.List;


@Description(name = "hbase_get_versions",
        value = "_FUNC_(key_col, hbase_tbl, family, column[, value_type T]): Array<T> - Simple Hbase value getter")


public class HBaseGetVersionsUDF extends GenericUDF {

    private static int NUM_ARGS;

    private transient PrimitiveObjectInspector keyOI;
    private transient StandardListObjectInspector outputOI;

    private Configuration config;
    private Connection connection;

    private static String table_name;
    private static byte[] Hfamily;
    private static byte[] Hcolumn;
    private static String list_value_type;

    private transient PrimitiveObjectInspectorConverter.TextConverter converter;

    private static final Logger LOG = LoggerFactory.getLogger(HBaseGetVersionsUDF.class.getName());

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        NUM_ARGS = arguments.length;
        if (NUM_ARGS < 4 || NUM_ARGS > 5) {
            throw new UDFArgumentLengthException(
                    "HBaseGetVersions() accepts exactly 4 or 5 arguments.");
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
                        "argument of HBaseGetValue() must be a constant string but " +
                        arguments[i].toString() + " was given.");
            }
        }

        table_name = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue().toString();
        Hfamily = ((ConstantObjectInspector) arguments[2]).getWritableConstantValue().toString().getBytes();
        Hcolumn = ((ConstantObjectInspector) arguments[3]).getWritableConstantValue().toString().getBytes();

        if (NUM_ARGS == 5) {

            if (((PrimitiveObjectInspector) arguments[4]).getPrimitiveCategory() !=
                    PrimitiveObjectInspector.PrimitiveCategory.STRING ||
                    !ObjectInspectorUtils.isConstantObjectInspector(arguments[4])) {
                throw new UDFArgumentTypeException(5, "The " + 5 + "th " +
                        "argument of HBaseGetFamily() must be a constant string but " +
                        arguments[4].toString() + " was given.");
            }
            list_value_type = ((ConstantObjectInspector) arguments[4]).getWritableConstantValue().toString().toLowerCase();
            switch (list_value_type) {
                case "int":
                    outputOI = ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.javaIntObjectInspector);
                    break;
                case "long":
                case "bigint":
                    outputOI = ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                    break;
                case "double":
                case "float":
                    outputOI = ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
                    break;
                case "string":
                    outputOI = ObjectInspectorFactory.getStandardListObjectInspector(
                            PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                    break;
                default:
                    throw new UDFArgumentException("Unrecognized map value type " + list_value_type);
            }
        } else {
            list_value_type = "string";
            outputOI = ObjectInspectorFactory.getStandardListObjectInspector(
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
        getter.addColumn(Hfamily, Hcolumn);
        getter.setMaxVersions();

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
            CellScanner scanner = result.cellScanner();

            switch (list_value_type) {
                case "int":
                    List<Integer> int_output = new ArrayList<>();
                    while (scanner.advance()) {
                        Cell res = scanner.current();
                        int_output.add(Bytes.toInt(CellUtil.cloneValue(res)));
                    }
                    return int_output;
                case "bigint":
                case "long":
                    List<Long> long_output = new ArrayList<>();
                    while (scanner.advance()) {
                        Cell res = scanner.current();
                        long_output.add(Bytes.toLong(CellUtil.cloneValue(res)));
                    }
                    return long_output;
                case "float":
                case "double":
                    List<Double> double_output = new ArrayList<>();
                    while (scanner.advance()) {
                        Cell res = scanner.current();
                        double_output.add(Bytes.toDouble(CellUtil.cloneValue(res)));
                    }
                    return double_output;
                default:
                    List<String> output = new ArrayList<>();
                    while (scanner.advance()) {
                        Cell res = scanner.current();
                        output.add(Bytes.toString(CellUtil.cloneValue(res)));
                    }
                    return output;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == NUM_ARGS);
        return getStandardDisplayString("hbase_get_versions", children);
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (this.connection != null) {
            this.connection.close();
        }
    }
}
