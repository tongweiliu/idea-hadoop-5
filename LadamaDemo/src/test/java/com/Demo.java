package com;

/**
 * @author liutongwei
 * @create 2018-10-23 9:44
 */
public class Demo {
    private String name;
    private Integer sex;
    private String age;

    public Demo(String age, Integer sex) {
        this.sex = sex;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getSex() {
        return sex;
    }

    public void setSex(Integer sex) {
        this.sex = sex;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }
}
