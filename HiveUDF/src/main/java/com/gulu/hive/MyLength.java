package com.gulu.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author Gollum
 * @date 2025-03-10 22:20
 */
public class MyLength extends GenericUDF {

    /**
     * 可以获取到函数的参数信息，对参数进行校验
     *
     * @param objectInspectors
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("只接受一个参数");
        }
        ObjectInspector inspector = objectInspectors[0];
        if (inspector.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("只接受一个基本类型参数");
        }
        PrimitiveObjectInspector objectInspector = (PrimitiveObjectInspector) inspector;
        if (objectInspector.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("只接受string类型的参数");
        }

        // 返回函数的返回值类型
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object o = deferredObjects[0].get();
        return o == null ? 0 : o.toString().length();
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
