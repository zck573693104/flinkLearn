package com.flink.utils;

import java.math.BigDecimal;
import java.text.NumberFormat;

public class FMathUtil {
    private static final int DEF_DIV_SCALE = 10;

    public FMathUtil() {
    }

    public static double add(double v1, double v2) {
        BigDecimal b1 = new BigDecimal(Double.toString(v1));
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return b1.add(b2).doubleValue();
    }

    public static double remainder(double v1, double v2) {
        BigDecimal b1 = new BigDecimal(Double.toString(v1));
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return b1.divideAndRemainder(b2)[1].doubleValue();
    }

    public static double sub(double v1, double v2) {
        BigDecimal b1 = new BigDecimal(Double.toString(v1));
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return b1.subtract(b2).doubleValue();
    }

    public static double mul(double v1, double v2) {
        BigDecimal b1 = new BigDecimal(Double.toString(v1));
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return b1.multiply(b2).doubleValue();
    }

    public static double mul(BigDecimal v1, double v2) {
        BigDecimal b2 = new BigDecimal(Double.toString(v2));
        return v1.multiply(b2).doubleValue();
    }

    public static double div(double v1, double v2) {
        return div(v1, v2, 10);
    }

    public static double div(double v1, double v2, int scale) {
        if (scale < 0) {
            throw new IllegalArgumentException("The scale must be a positive integer or zero");
        } else {
            BigDecimal b1 = new BigDecimal(Double.toString(v1));
            BigDecimal b2 = new BigDecimal(Double.toString(v2));
            return b1.divide(b2, scale, 4).doubleValue();
        }
    }

    public static double round(double v, int scale) {
        if (scale < 0) {
            throw new IllegalArgumentException("The scale must be a positive integer or zero");
        } else {
            BigDecimal b = new BigDecimal(Double.toString(v));
            BigDecimal one = new BigDecimal("1");
            return b.divide(one, scale, 4).doubleValue();
        }
    }

    public static String percent(double p1, double p2) {
        double p3 = p1 / p2;
        NumberFormat nf = NumberFormat.getPercentInstance();
        nf.setMinimumFractionDigits(2);
        String str = nf.format(p3);
        return str;
    }

    public static double getStepWidth(double max) {
        double n = Math.floor(Math.log10(max));
        double a = max / Math.pow(10.0D, n);
        double A = Math.ceil(a);
        double step = A * Math.pow(10.0D, n - 1.0D);
        return step;
    }

    public static double calcUnTaxPrice(double taxPrice, double taxRate) {
        return div(taxPrice, add(1.0D, div(Double.valueOf(taxRate), 100.0D)));
    }

    public static double calcTaxPrice(double unTaxPrice, double taxRate) {
        return mul(unTaxPrice, add(1.0D, div(Double.valueOf(taxRate), 100.0D)));
    }
}
