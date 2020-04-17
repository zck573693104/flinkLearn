package org.fuwushe.utils;



import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class DateUtil {


    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    public static final String YYYY_MM_DD = "yyyy-MM-dd";

    public static final String YYYY_MM = "yyyy-MM";

    /**
     * 得到当前日期格式化后的字符串，格式：yyyy-MM-dd(年-月-日)
     *
     * @return 当前日期格式化后的字符串
     */
    public static String getTodayStr() {
        return new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    }



    /**
     * 时间戳，格式:yyyy-MM-dd HH:mm:ss(年-月-日 时：分：秒)
     *
     * @return 简单的时间戳
     */
    public static String dateToStr(Date date,String model) {
        return new SimpleDateFormat(model).format(date);
    }

    public static Date strToDate(String str,String model) {
        SimpleDateFormat sim = new SimpleDateFormat(model);
        Date dateTime = null;
        try {
            dateTime = sim.parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dateTime;
    }



    public static List<String> rangeMonth(String beginDate,String endDate){
        List<String> rangeMonths = new ArrayList<>();
        try{
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");//格式化为年月

            Calendar min = Calendar.getInstance();
            Calendar max = Calendar.getInstance();

            min.setTime(sdf.parse(beginDate));
            min.set(min.get(Calendar.YEAR), min.get(Calendar.MONTH), 1);

            max.setTime(sdf.parse(endDate));
            max.set(max.get(Calendar.YEAR), max.get(Calendar.MONTH), 2);

            Calendar curr = min;
            while (curr.before(max)) {
                rangeMonths.add(sdf.format(curr.getTime()));
                curr.add(Calendar.MONTH, 1);
            }



        }catch (Exception e){
            System.out.println("异常"+e.getMessage());
        }
        return rangeMonths;
    }

    public static String monthFirDay(String date){
        // 获取当月第一天
        date = date+ "-01";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cale = Calendar.getInstance();
        try {
            cale.setTime(format.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String firstday;
        // 获取前月的第一天
        cale.add(Calendar.MONTH, 0);
        cale.set(Calendar.DAY_OF_MONTH, 1);
        firstday = format.format(cale.getTime());
        return firstday;

    }

    public static String monthEndDay(String date){
        // 获取前月的最后一天
        date = date+ "-01";
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cale = Calendar.getInstance();
        try {
            cale.setTime(format.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String lastday;
        cale.add(Calendar.MONTH, 1);
        cale.set(Calendar.DAY_OF_MONTH, 0);
        lastday = format.format(cale.getTime());
        return lastday;

    }

    /**
     * 上月第一天
     * @param date
     * @return
     */
    public static Date getMonBeforeDayFir(String date){
        Calendar c = Calendar.getInstance();
        c.setTime(strToDate(date,YYYY_MM_DD));
        c.add(Calendar.MONTH, -1);
        Date m = c.getTime();
       return m;
    }

    /**
     * 上月最后一天
     * @param date
     * @return
     */
    public static String getMonBeforeDayEnd(String date){
        Calendar cale = Calendar.getInstance();
        cale.setTime(strToDate(date,YYYY_MM_DD));
        cale.add(Calendar.MONTH, -1);
        Date m = cale.getTime();
        return monthEndDay(dateToStr(m,YYYY_MM_DD));

    }
    public static void main(String[] args) {

        System.out.println(1/0);
        System.out.println(LocalDate.now().toString());
        System.out.println("12345".substring(0,4));
        System. out.println(getMonBeforeDayFir("2019-06-01"));

    }
} 