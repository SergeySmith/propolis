package org.hive.propolis;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


@Description(name = "subtract_file",
        value = "_FUNC_(array<str>, filename) - Removes all elements of the list that appear in the file")


public class SubtractFileUDF extends GenericUDF {

    private HashSet<String> set;
    private transient ListObjectInspector listObjectInspector;
    private transient ObjectInspector strObjectInspector;
    private transient ObjectInspector fileObjectInspector;

    static final Log LOG = LogFactory.getLog(IntersectFileUDF.class.getName());

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "IntersectFile() accepts exactly 2 arguments.");
        }

        ObjectInspector inputListOI = arguments[0];
        if (inputListOI.getCategory() != ObjectInspector.Category.LIST){
            throw new UDFArgumentTypeException(0,
                    "The first argument must be Array, but "
                            + inputListOI.getCategory().name()
                            + " was passed.");
        }

        listObjectInspector = (ListObjectInspector) inputListOI;
        if (listObjectInspector.getListElementObjectInspector().getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("The Array element type must be Primitive.");
        }

        strObjectInspector = listObjectInspector.getListElementObjectInspector();
        fileObjectInspector = arguments[1];

        if (!isTypeCompatible(strObjectInspector)) {
            throw new UDFArgumentTypeException(0, "The first " +
                    "argument of function IntersectFile must be a string, " +
                    "char or varchar but " +
                    strObjectInspector.toString() + " was given.");
        }

        if (((PrimitiveObjectInspector) fileObjectInspector).getPrimitiveCategory() !=
                PrimitiveObjectInspector.PrimitiveCategory.STRING ||
                !ObjectInspectorUtils.isConstantObjectInspector(fileObjectInspector)) {
            throw new UDFArgumentTypeException(1, "The second " +
                    "argument of IntersectFile() must be a constant string but " +
                    fileObjectInspector.toString() + " was given.");
        }

        String returnElemType = strObjectInspector.getTypeName();
        PrimitiveObjectInspector elemOI = GetObjectInspectorForTypeName(returnElemType);

        ObjectInspector ret_type = ObjectInspectorFactory.getStandardListObjectInspector(elemOI);

        return ret_type;
    }

    private boolean isTypeCompatible(ObjectInspector argument) {
        PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) argument);
        return
                poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING ||
                        poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.CHAR ||
                        poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.VARCHAR ||
                        poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.VOID;
    }

    private static PrimitiveObjectInspector GetObjectInspectorForTypeName(String typeString) {
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString);

        return (PrimitiveObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    }

    @Override
    public String[] getRequiredFiles() {
        return new String[] {
                ObjectInspectorUtils.getWritableConstantValue(fileObjectInspector)
                        .toString()
        };
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null || arguments[1].get() == null) {
            return null;
        }

        List<?> list = listObjectInspector.getList(arguments[0].get());
        List<String> output = new ArrayList<String>();

        if (set == null) {
            String filePath = (String)ObjectInspectorUtils.copyToStandardJavaObject(
                    arguments[1].get(), fileObjectInspector);
            loadFromFile(filePath);
        }

        for (int i = 0; i < listObjectInspector.getListLength(arguments[0].get()); i++) {
            Object elem = listObjectInspector.getListElement(arguments[0].get(), i);
            if (elem != null) {
                String str = ObjectInspectorUtils.copyToStandardJavaObject(elem, strObjectInspector).toString();
                if (!set.contains(str)) {
                    output.add(str);
                }
            }
        }

        if (output.isEmpty()) {
            return null;
        } else
            return output;
    }

    private BufferedReader getReaderFor(String filePath) throws HiveException {
        try {
            Path fullFilePath = FileSystems.getDefault().getPath(filePath);
            Path fileName = fullFilePath.getFileName();
            if (Files.exists(fileName)) {
                return Files.newBufferedReader(fileName, Charset.defaultCharset());
            }
            else
            if (Files.exists(fullFilePath)) {
                return Files.newBufferedReader(fullFilePath, Charset.defaultCharset());
            }
            else {
                throw new HiveException("Could not find \"" + fileName + "\" or \"" + fullFilePath + "\" in subtract_file() UDF.");
            }
        }
        catch(IOException exception) {
            throw new HiveException(exception);
        }
    }

    private void loadFromFile(String filePath) throws HiveException {
        set = new HashSet<String>();

        try (BufferedReader reader = getReaderFor(filePath)) {
            String line;
            while((line = reader.readLine()) != null) {
                set.add(line);
            }
        } catch (Exception e) {
            throw new HiveException(e);
        }
    }

    @Override
    public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
        // Asserts the class invariant. (Same types.)
        super.copyToNewInstance(newInstance);
        SubtractFileUDF that = (SubtractFileUDF) newInstance;
        if (that != this) {
            that.set = (this.set == null ? null : (HashSet<String>) this.set.clone());
            that.strObjectInspector = this.strObjectInspector;
            that.fileObjectInspector = this.fileObjectInspector;
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 2);
        return getStandardDisplayString("subtract_file", children);
    }
}
