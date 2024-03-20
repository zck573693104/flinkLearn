package com.bigdata.utils;

public class Dms2dUtils {
    /**
     * 度分秒转经纬度
     *
     * @param du min sec  116 25 7.85
     * @return 116.418847
     */
    public static double changeToDu(String du, String min, String sec) {
        int d = Integer.parseInt(du);
        int f = Integer.parseInt(min);
        double m = Double.parseDouble(sec);
        double fen = f + (m / 60);
        return (fen / 60) + Math.abs(d);
    }
}
