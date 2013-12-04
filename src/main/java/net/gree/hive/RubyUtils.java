package net.gree.hive;

import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

public class RubyUtils {
    public static final String CONF_RB_SCRIPT = "rb.script";
    public static final String CONF_JRB_LOAD_PATH = "jruby.load_path";

    public static ObjectInspector resolveOI(ObjectInspector arg) {

        switch (arg.getCategory()) {
            case PRIMITIVE:
                // VOID, BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, STRING, TIMESTAMP, BINARY, DECIMAL, UNKNOWN
                PrimitiveObjectInspector poi = (PrimitiveObjectInspector) arg;
                if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT ||
                        poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.SHORT) {

                    return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                            PrimitiveObjectInspector.PrimitiveCategory.LONG
                    );
                } else {
                    return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                            poi.getPrimitiveCategory()
                    );
                }
            case LIST:
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        resolveOI(((ListObjectInspector) arg).getListElementObjectInspector())
                );
            case MAP:
                return ObjectInspectorFactory.getStandardMapObjectInspector(
                        resolveOI(((MapObjectInspector) arg).getMapKeyObjectInspector()),
                        resolveOI(((MapObjectInspector) arg).getMapValueObjectInspector())
                );
            case STRUCT:
                StructObjectInspector soi = (StructObjectInspector) arg;
                int size = soi.getAllStructFieldRefs().size();
                ArrayList<String> fnl = new ArrayList<String>(size);
                ArrayList<ObjectInspector> foil = new ArrayList<ObjectInspector>(size);

                for (StructField sf : ((StructObjectInspector) arg).getAllStructFieldRefs()) {
                    fnl.add(sf.getFieldName());
                    foil.add(resolveOI(sf.getFieldObjectInspector()));
                }

                return ObjectInspectorFactory.getStandardStructObjectInspector(fnl, foil);
            default:
                return arg;
        }
    }
}
