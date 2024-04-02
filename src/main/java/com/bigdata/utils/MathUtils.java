package com.bigdata.utils;

import java.util.regex.Pattern;

public class MathUtils {
    private static final Pattern SCIENTIFIC_NOTATION_PATTERN = Pattern.compile("^[-+]?\\d+(\\.\\d+)?[eE][-+]?\\d+$");

    public static boolean isScientificNotation(String value) {
        return SCIENTIFIC_NOTATION_PATTERN.matcher(value).matches();
    }

    public static void main(String[] args) {
        String[] tests = {"1.23E4", "-3.14e-2", "abc", "0.1", "12"};
        for (String test : tests) {
            if (isScientificNotation(test)) {
                System.out.println(test + " is scientific notation");
            } else {
                System.out.println(test + " is not scientific notation");
            }
        }
    }
}
