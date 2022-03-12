package base.test.bean;

import com.alibaba.fastjson.annotation.JSONField;

import java.util.Date;


public class Person {

    @JSONField(name = "NAME", ordinal = 2)
    public String name = "amy";

    @JSONField(ordinal = 1)
    private int age = 2;

    @JSONField(ordinal = 3, serialize = false)
    protected double money = 3.0D;

    @JSONField(format = "dd/MM/yyyy", ordinal = 0, deserialize = false)
    private Date birth = new Date();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public double getMoney() {
        return money;
    }

    public void setMoney(double money) {
        this.money = money;
    }


    public Date getBirth() {
        return birth;
    }

    public void setBirth(Date birth) {
        this.birth = birth;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", money=" + money +
                ", birth=" + birth +
                '}';
    }
}
