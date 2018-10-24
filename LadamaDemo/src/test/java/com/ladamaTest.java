package com;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author liutongwei
 * @create 2018-10-23 9:42
 */
public class ladamaTest {
    @Test
    public void filterTest() {
        List<Double> cost = Arrays.asList(10.0, 20.0, 30.0, 40.0);
        List<Double> filteredCost = cost.stream().filter(x -> x > 25).collect(Collectors.toList());
        filteredCost.forEach(x-> System.out.println(x));
    }
    @Test
    public void mapReduceTest() {
        List<Double> cost = Arrays.asList(10.0, 20.0,30.0);
        Double allCost = cost.stream().map(x -> x + x * 0.05).reduce((sum, x) -> sum + x).get();
        System.out.println(allCost);
    }
    @Test
    public void mapTest() {
        List<Double> cost = Arrays.asList(10.0, 20.0, 30.0);
        cost.stream().map(x -> x+x*0.05).forEach(x-> System.out.println(x));
    }
    @Test
    public void iterTest() {
        List<String> languages = Arrays.asList("java", "scala", "python");
        languages.forEach(x -> System.out.println(x));
        System.out.println("=====================");
        languages.forEach(System.out::println);
    }
    @Test
    public void test3(){
        new Thread(() -> System.out.println("It's a lambda function!")).start();
    }
    @Test
    public void test2(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("The old runable now is using!");
            }
        }).start();
    }
    @Test
    public void test1() {
        List<Student> list = new ArrayList<>();
        Student student1 = new Student();
        student1.setAge("12");
        student1.setSex(0);
        Student student2 = new Student();
        student2.setAge("13");
        student2.setSex(2);
        Student student3 = new Student();
        student3.setAge("11");
        student3.setSex(1);
        Student student4 = new Student();
        student4.setAge("18");
        student4.setSex(1);
        Student student5 = new Student();
        student5.setAge("18");
        student5.setSex(0);
        Student student6 = new Student();
        student6.setAge("18");
        student6.setSex(2);
        Student student7 = new Student();
        student7.setAge("18");
        student7.setSex(2);
        list.add(student1);
        list.add(student2);
        list.add(student3);
        list.add(student4);
        list.add(student5);
        list.add(student6);
        list.add(student7);
        List<Demo> demos = new ArrayList<Demo>();
//        原始数据
        System.out.println("原始数据 组装list<demo>**********");
        demos=list.stream().map(student -> new Demo(student.getAge(),student.getSex())).collect(Collectors.toList());
        demos.forEach(demo -> {
            System.out.println("年龄"+demo.getAge()+"性别"+demo.getSex()+",");
        });
//        只取sex为0
        System.out.println("只取sex为0************");
        List<Demo> demorm=demos.stream().filter(demo -> demo.getSex()==0).distinct().collect(Collectors.toList());
        demorm.forEach(demo -> {
            System.out.println("年龄 " + demo.getAge() + "  性别 " + demo.getSex() + ",");
        });
//        筛选大于12岁的用户
        List<Demo>demoFilter=demos.stream().filter(demo -> Integer.parseInt(demo.getAge())>12).collect(Collectors.toList());
        demoFilter.forEach(demo -> {
            System.out.println("年龄 " + demo.getAge() + "  性别 " + demo.getSex() + ",");
        });
//      排序
        System.out.println("排序*********************");
        List<Demo>demoSort=demos.stream().sorted((s1,s2)->s1.getAge().compareTo(s2.getAge())).collect(Collectors.toList());
        demoSort.forEach(demo -> {
            System.out.println("年龄 " + demo.getAge() + "  性别 " + demo.getSex() + ",");
        });

    }
}
