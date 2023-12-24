package io.sustc.service.impl;

import java.io.*;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Logger {
    private final String dir;
    private static File file;
    private static BufferedWriter bw ;
    public Logger(){
        Date today = new Date();
        SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
        String time=ft.format(today);
        dir = "..\\"+time;
        System.out.println("The log info would be stored at "+dir);
        try {
            file = new File(dir);
            if (!file.exists()){
                file.createNewFile();
            }
            //创建BufferedWriter对象并向文件写入内容
            bw = new BufferedWriter(new FileWriter(file));
            bw.flush();
        }catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static void debug(String info){
        try {
            bw.write("[DEBUG] "+info);
            bw.flush();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void sql(String info){
        try {
            bw.write("[SQL] "+info);
            bw.flush();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void function(String info){
        try {
            bw.write("[Function] "+info);
            bw.flush();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
