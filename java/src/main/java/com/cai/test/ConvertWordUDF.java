package com.cai.test;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ConvertWordUDF extends GenericUDF {
    @Description(name = "convert_word",
            value = "_FUNC_(input) - return a format string",
            extended = "Example:\n"
                    + "> SELECT _FUNC_('A12B23');\n"
                    + "> ,A:12,B:23"
    )

    private ObjectInspectorConverters.Converter converter;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length != 1){
            throw new UDFArgumentException(
                    "convert_word() requires 1 arguments, got " + arguments.length
            );
        }

        if(arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentException(
                    "convert_word() only take primitive String type, got " + arguments[0].getTypeName()
            );
        }
        converter = ObjectInspectorConverters.getConverter(arguments[0], PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        // 返回值的 ObjectInspector，返回String类型
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        if (arguments[0].get() == null || arguments[0].get().equals("")) {
            return null;
        }
        String word = (String) converter.convert(arguments[0].get());
        //System.out.println(word);
        //StringBuilder result_word = new StringBuilder();
        String result_word = "";
        for (int i = 0; i < word.length(); i++) {
            if (Character.isUpperCase(word.charAt(i))) {
                //result_word.append(',' + word.charAt(i) + ':');
                result_word += "," + word.charAt(i) + ":";
            }
            else {
                //result_word.append(word.charAt(i));
                result_word += word.charAt(i);
            }
        }
        //return result_word.toString();
        return result_word;
    }

    @Override
    public String getDisplayString(String[] children) {
        //这个方法用于当实现的GenericUDF出错的时候，打印出提示信息。而提示信息就是你实现该方法最后返回的字符串。
        // 会出现在执行计划中
        // generate the logs to show in the HQL explain clause
        return children[0];
    }
}
