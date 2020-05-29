package org.hive.propolis;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.*;


public class ArrayStructDistinctUDF extends GenericUDF {
    // the return variable. Java Object[] become hive struct<>. Java ArrayList<> become hive array<>.
    // The return variable only holds the values that are in the struct<>. The field names
    // are defined in the ObjectInspector that is returned by the initialize() method.
    private static ArrayList ret;
    private static List<String> struct_fields;

    // Global variables that inspect the input.
    // These are set up during the initialize() call, and are then used during the
    // calls to evaluate()
    //
    // ObjectInspector for the list (input array<>)
    // ObjectInspector for the struct<>
    // ObjectInspectors for the elements of the struct<>, target, quantity and price
    private static ListObjectInspector loi;
    private static StructObjectInspector soi;
    private static ObjectInspector toi;
    private static StandardConstantListObjectInspector snamesOI;
    private static StringObjectInspector fieldOI;

    static final Log LOG = LogFactory.getLog(ArrayStructDistinctUDF.class.getName());

    @Override
    // This is what we do in the initialize() method:
    // Verify that the input is of the type expected
    // Set up the ObjectInspectors for the input in global variables
    // Initialize the output ArrayList
    // Return the ObjectInspector for the output
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        // Verify the input is of the required type.
        // Set the global variables (the various ObjectInspectors) while we're doing this

        // Exactly one input argument
        if( arguments.length != 2 )
            throw new UDFArgumentLengthException("ArrayStructDistinctUDF() accepts exactly two argument.");
//        LOG.warn(">>> [initialize] length check");

        if( arguments[1].getCategory() != ObjectInspector.Category.LIST )
            throw new UDFArgumentTypeException(1,"The second argument to ArrayStructDistinctUDF should be "
                    + "Array<String>"
                    + " but " + arguments[1].getTypeName() + " is found");
        snamesOI = (StandardConstantListObjectInspector) arguments[1];
        ObjectInspector elemOI = snamesOI.getListElementObjectInspector();

        if (elemOI.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1,
                    "The array element must be PRIMITIVE, but "
                            + elemOI.getCategory().name()
                            + " was passed.");
        }

        if ( ((PrimitiveObjectInspector) elemOI).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING ) {
            throw new UDFArgumentTypeException(1, "The array element must be String");
        }
        fieldOI = (StringObjectInspector) elemOI;

        List<?> snames = snamesOI.getWritableConstantValue();
        struct_fields = new ArrayList<String>();
        for (Object nm: snames) {
            if (nm instanceof org.apache.hadoop.io.Text) {
                struct_fields.add(nm.toString());
            }
        }

        // Is the input an array<>
        if( arguments[0].getCategory() != ObjectInspector.Category.LIST )
            throw new UDFArgumentTypeException(0,"The first argument to ArrayStructDistinctUDF should be "
                    + "Array<Struct>"
                    + " but " + arguments[0].getTypeName() + " is found");

        // Is the input an array<struct<>>
        // Get the object inspector for the list(array) elements; this should be a StructObjectInspector
        // Then check that the struct has the correct fields.
        // Also, store the ObjectInspectors for use later in the evaluate() method
        loi = ((ListObjectInspector) arguments[0]);
        soi = ((StructObjectInspector) loi.getListElementObjectInspector());

        // Are there the correct number of fields?
        if( soi.getAllStructFieldRefs().size() != struct_fields.size() )
            throw new UDFArgumentTypeException(0,"Incorrect number of fields in the struct. "
                    + "The single argument to ArrayStructDistinctUDF should be "
                    + "Array<Struct>"
                    + " but " + arguments[0].getTypeName() + " is found");

        // Are the fields the ones we want?
        for (String field: struct_fields) {
            StructField target = soi.getStructFieldRef(field);

            if (target == null)
                throw new UDFArgumentTypeException(0, "No \"target\" field in input structure " + arguments[0].getTypeName());

            // Are they of the correct types? (primitives WritableLong, WritableInt, WritableFloat)
            // We store these Object Inspectors for use in the evaluate() method.
            toi = target.getFieldObjectInspector();

            // First, are they primitives?
            if (toi.getCategory() != ObjectInspector.Category.PRIMITIVE)
                throw new UDFArgumentTypeException(0, "Is input primitive? target field must be primitive; found " + toi.getTypeName());

            // Second, are they the correct type of primitive?
            if (((PrimitiveObjectInspector) toi).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE)
                throw new UDFArgumentTypeException(0, "Is input correct primitive? target field must be DOUBLE; found " + toi.getTypeName());
        }

        // If we get to here, the input is an array<struct>

        // HOW TO RETURN THE OUTPUT?
        // A struct<> is stored as an Object[], with the elements being ,,...
        // See GenericUDFNamedStruct
        // The object inspector that we set up below and return at the end of initialize() takes care of the names,
        // so the Object[] only holds the values.
        // A java ArrayList is converted to a hive array<>, so the output is an ArrayList
        ret = new ArrayList();

        // Now set up and return the object inspector for the output of the UDF

        // Define the field names for the struct<> and their types
        ArrayList structFieldNames = new ArrayList();
        ArrayList structFieldObjectInspectors = new ArrayList();

        for (String sname: struct_fields) {
            structFieldNames.add(sname);
            structFieldObjectInspectors.add( PrimitiveObjectInspectorFactory.writableDoubleObjectInspector );
        }
        // To get instances of PrimitiveObjectInspector, we use the PrimitiveObjectInspectorFactory

        // Set up the object inspector for the struct<> for the output
        StructObjectInspector si2;
        si2 = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);

        // Set up the list object inspector for the output, and return it
        ListObjectInspector li2;
        li2 = ObjectInspectorFactory.getStandardListObjectInspector( si2 );
        return li2;
    }

    @Override
    // The evaluate() method. The input is passed in as an array of DeferredObjects, so that
    // computation is not wasted on deserializing them if they're not actually used
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        // Empty the return array (re-used between calls)
        ret.clear();

        // Should be exactly one argument
        assert ( arguments.length == 2 );

        // Iterate over the elements of the input array
        // Convert the struct<>'s to the new format
        // Put them into the output array
        // Return the output array

        Set<Map<String, Double>> input = new LinkedHashSet<Map<String, Double>>();

        final int nelements = loi.getListLength(arguments[0].get());
        for (int i = 0; i < nelements; i++) {

            Map<String, Double> elem = new HashMap<String, Double>();
            // getStructFieldData() returns an Object; however, it's actually a LazyLong
            // (How do we know it's a LazyLong, as the documentation says that getStructFieldData() returns an Object?
            //We know, because during development, the error in the hadoop task log was "can't cast LazyLong to ...")
            // How do you get the data out of a LazyLong? Using a LongObjectInspector...
            for (String sname: struct_fields) {
                final Object list_elem = soi.getStructFieldData(loi.getListElement(arguments[0].get(), i), soi.getStructFieldRef(sname));
                if (list_elem != null) {
                    DoubleWritable field_elem = (DoubleWritable) list_elem;
                    final double value = ((DoubleObjectInspector) toi).get(field_elem);
                    elem.put(sname, value);
                }
            }

            if (elem.size() == struct_fields.size()) {
                input.add(elem);
            }
        }

        final int len = struct_fields.size();

        for (Map<String, Double> elem: input) {
            Object[] e;
            e = new Object[len];

            // The field values must be inserted in the same order as defined in the ObjectInspector for the output
            // The fields must also be hadoop writable/text classes
            for (int i=0; i<len; i++) {
                final String key = struct_fields.get(i);
                e[i] = new DoubleWritable(elem.get(key));
            }
            ret.add(e);
        }

        return ret;
    }

    @Override
    public String getDisplayString(String[] children) {
        assert( children.length>0 );

        StringBuilder sb = new StringBuilder();
        sb.append("ArrayStructDistinctUDF(");
        sb.append(children[0]);
        sb.append(")");

        return sb.toString();
    }
}
