package org.hive.propolis;

import net.iakovlev.timeshape.TimeZoneEngine;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.Optional;

@Description(
        name = "Get timezone from geo coordinates (lat, lon)",
        value = "_FUNC_(lat Double, lon Double): String\n"
                + "-- finds timezone from a given (latitude, longitude) pair"
)

public class GeoToTimeZoneUDF extends GenericUDF {

    private static final int NUM_ARGS = 2;
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(GeoToTimeZoneUDF.class.getName());
    private TimeZoneEngine engine;

    private transient DoubleObjectInspector latOI;
    private transient DoubleObjectInspector lonOI;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != NUM_ARGS) {
            throw new UDFArgumentLengthException(
                    "GeoToTimeZoneUDF accepts exactly " + NUM_ARGS + " arguments.");
        }

        for (int i = 0; i < NUM_ARGS; ++i) {
            if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentTypeException(i,
                        "The " + (i+1) + "th argument must be Primitive, but "
                                + arguments[i].getCategory().name()
                                + " was passed.");
            }
        }

        latOI = (DoubleObjectInspector) arguments[0];
        lonOI = (DoubleObjectInspector) arguments[1];

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public void configure(MapredContext context) {
        super.configure(context);
        LOG.warn("[configure] Initializing TimeZoneEngine");
        engine = TimeZoneEngine.initialize();
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object latObject = arguments[0].get();
        Object lonObject = arguments[1].get();

        if (latObject == null || lonObject == null) {
            return null;
        }

        Double lat = (Double) latOI.getPrimitiveJavaObject(latObject);
        Double lon = (Double) lonOI.getPrimitiveJavaObject(lonObject);

        if (engine == null) {
            LOG.warn("[eval] Initializing TimeZoneEngine");
            engine = TimeZoneEngine.initialize();
        }

        Optional<ZoneId> zoneId = engine.query(lat, lon);
        if (zoneId.isPresent()) {
            return zoneId.get().getId();
        } else {
            return null;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == NUM_ARGS);
        return getStandardDisplayString("timezone", children);
    }
}
