package com.test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author liutongwei
 * @create 2018-10-23 10:32
 */
public class lama {

    public static void main(String[] args) {
        List<String> languages = Arrays.asList("Java","Python","scala","Shell","R");
        System.out.println("Language starts with J: ");
        filterTest(languages,x->x.startsWith("J"));
    }

    public static void filterTest(List<String> languages, Predicate<String> condition){
        languages.stream().filter(x->condition.test(x)).forEach(x-> System.out.println(x+","));
    }
}
