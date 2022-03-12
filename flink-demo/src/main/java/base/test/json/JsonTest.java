package base.test.json;

import base.test.bean.Person;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class JsonTest {

    public static void main(String[] args) {


        test2Array();
        JSONObject a = null;
        JSONArray b = null;

//        String s = JSON.toJSONString(new Person());
//
//        System.out.println(s);
//
//        System.out.println(JSON.parseObject("{\"NAME\":\"amy\",\"age\":2,\"birth\":1647079537253,\"money\":3.0}", Person.class));
    }

    public static void test2Array() {
        List<Person> list = Arrays.asList(new Person(),new Person());
        String jsonOutput = JSON.toJSONString(list, SerializerFeature.BeanToArray);

        System.out.println(jsonOutput);
    }
}
