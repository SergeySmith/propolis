package org.hive.propolis;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

@Description(name = "shift_timestamp",
        value = "_FUNC_(timestamp, string to_timezone, string from_timezone) - "
                + "converts a given timestamp in from_timezone to local time in to_timezone")

public class ShiftTimestampUDF extends GenericUDF {

    static final Logger LOG = LoggerFactory.getLogger(ShiftTimestampUDF.class);
    private static final int NUM_ARGS = 3;

    private transient TimestampConverter timestampConverter;
    private transient TextConverter toTzConverter;
    private transient TextConverter fromTzConverter;
    private transient SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != NUM_ARGS) {
            throw new UDFArgumentLengthException("UDF requires " + NUM_ARGS
                    + " arguments, but got " + arguments.length);
        }

        for (int i = 0; i < NUM_ARGS; ++i) {
            if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                        "The argument number " + (i+1) + " must be Primitive, but "
                                + arguments[i].getCategory().name() + " was passed.");
            }
        }

        timestampConverter = new TimestampConverter((PrimitiveObjectInspector) arguments[0],
                PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);

        toTzConverter = new TextConverter((PrimitiveObjectInspector) arguments[1]);
        fromTzConverter = new TextConverter((PrimitiveObjectInspector) arguments[2]);

        return PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
    }

    /**
     * Forked from Apache Hive project.
     * Parse the timestamp string using the input TimeZone.
     * This does not parse fractional seconds.
     * @param tsString
     * @param tz
     * @return
     */
    protected Timestamp timestampFromString(String tsString, TimeZone tz) {
        dateFormat.setTimeZone(tz);
        try {
            java.util.Date date = dateFormat.parse(tsString);
            if (date == null) {
                return null;
            }
            return new Timestamp(date.getTime());
        } catch (ParseException err) {
            return null;
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object tsObject = arguments[0].get();
        Object toTzObject = arguments[1].get();
        Object fromTzObject = arguments[2].get();

        if (tsObject == null || toTzObject == null || fromTzObject == null) {
            return null;
        }

        Object tsConverted = timestampConverter.convert(tsObject);
        if (tsConverted== null) {
            return null;
        }
        Timestamp inputTs = ((TimestampWritable) tsConverted).getTimestamp();

        TimeZone toTz = TimeZone.getTimeZone(toTzConverter.convert(toTzObject).toString());
        TimeZone fromTz = TimeZone.getTimeZone(fromTzConverter.convert(fromTzObject).toString());

        Timestamp fromTs = timestampFromString(inputTs.toString(), fromTz);
        if (fromTs == null) {
            return null;
        }

        dateFormat.setTimeZone(toTz);
        Timestamp result = Timestamp.valueOf(dateFormat.format(fromTs));

        if (inputTs.getNanos() != 0) {
            result.setNanos(inputTs.getNanos());
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == NUM_ARGS);
        return getStandardDisplayString("shift_timestamp", children);
    }

    public String getName() {
        return "shift_timestamp";
    }
}