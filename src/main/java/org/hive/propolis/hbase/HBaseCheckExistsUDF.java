package org.hive.propolis.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


@Description(name = "hbase_check_value",
        value = "_FUNC_(key_col, hbase_tbl, family, column): Boolean - Simple Hbase value checker")

public class HBaseCheckExistsUDF extends GenericUDF {

    private static final int NUM_ARGS = 4;

    private transient PrimitiveObjectInspector keyOI;

    private Configuration config;
    private Connection connection;

    private static String table_name;
    private static byte[] Hfamily;
    private static byte[] Hcolumn;

    private transient PrimitiveObjectInspectorConverter.TextConverter converter;

    private static final Logger LOG = LoggerFactory.getLogger(HBaseCheckExistsUDF.class.getName());

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != NUM_ARGS) {
            throw new UDFArgumentLengthException(
                    "HBaseGetValue() accepts exactly 4 arguments.");
        }

        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "The first argument must be Primitive, but "
                            + arguments[0].getCategory().name()
                            + " was passed.");
        }

        keyOI = (PrimitiveObjectInspector) arguments[0];

        for (int i = 1; i <= NUM_ARGS-1; ++i) {
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

        return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
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
        getter.setCheckExistenceOnly(true);

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

            return result.getExists();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == NUM_ARGS);
        return getStandardDisplayString("hbase_check_value", children);
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (this.connection != null) {
            this.connection.close();
        }
    }
}
