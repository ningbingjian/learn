package tv.huan.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created   on 2016/12/11.
 */
@Description(name = "map_tag_weight", value = "_FUNC_(Map(tag,weight)) - "
        + "tag means level 1 of tag or level 2 of tag[tag_level1-taglevel2]")
public class GenericUDFMapTagWeight  extends GenericUDF{
    private transient ObjectInspectorConverters.Converter mapConverters;
    LinkedHashMap<Object, Object> ret = new LinkedHashMap<Object, Object>();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length != 1){
            throw new UDFArgumentLengthException(
                    "Arguments length must be 1");
        }
        if(!(arguments[0] instanceof MapObjectInspector)){
            throw new UDFArgumentTypeException(0, "your type must be a map");
        }
        GenericUDFUtils.ReturnObjectInspectorResolver keyOIResolver =
                new GenericUDFUtils.ReturnObjectInspectorResolver(true);
        GenericUDFUtils.ReturnObjectInspectorResolver valueOIResolver =
                new GenericUDFUtils.ReturnObjectInspectorResolver(true);
        ObjectInspector keyOI =
                keyOIResolver.get(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        ObjectInspector valueOI =
                valueOIResolver.get(ObjectInspectorFactory.getStandardMapObjectInspector(
                        keyOIResolver.get(PrimitiveObjectInspectorFactory.javaStringObjectInspector),
                        valueOIResolver.get(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector)));
        mapConverters = ObjectInspectorConverters.getConverter(arguments[0],valueOI);
        return ObjectInspectorFactory.getStandardMapObjectInspector(keyOI, valueOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        ret.clear();
        Map<Object,Object> tagWeightMap = (Map<Object,Object>) mapConverters.convert(arguments[0].get());
        System.out.println(tagWeightMap.getClass());
        System.out.println(tagWeightMap);
        tagWeightMap.entrySet().forEach(entry ->{
            String tag = entry.getKey().toString();
            Double weight = Double.valueOf(entry.getValue().toString());
            String[]fields = tag.split("-");
            String tagl1 = null;
            if(fields.length == 2){
                tagl1 = fields[0] ;
            }else{
                tagl1 = tag ;
            }
            Map<Object,Object> value = (Map<Object,Object>)ret.get(tagl1);
            if(value == null ){
                value = new LinkedHashMap<>();
                ret.put(tagl1,value);
            }
            if(fields.length == 1){
                value.put("weight",weight);
            }else{
                value.put(fields[1],weight);
            }
        });
        return ret;
    }

    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("map(Map(tag,weight)");

        sb.append(")");
        return sb.toString();
    }
}
