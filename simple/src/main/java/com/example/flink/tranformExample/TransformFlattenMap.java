package com.example.flink.tranformExample;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransformFlattenMap {

    public static void main(String[] args) {
        List<List<String>> objects = new ArrayList<>();

        List<String> cities = new ArrayList<>();
        cities.add("Pune");
        cities.add("Mumbai");
        cities.add("Delhi");

        List<String> companies = new ArrayList<>();
        companies.add("NICE");
        companies.add("Google");
        companies.add("Amazon");
        companies.add("Microsoft");

        List<String> seasons = new ArrayList<>();
        seasons.add("Rainy");
        seasons.add("Summer");
        seasons.add("Winter");

        objects.add(cities);
        objects.add(companies);
        objects.add(seasons);

        System.out.println(objects.stream().flatMap(Collection::stream).collect(Collectors.toList()));

        System.out.println();
    }
}
