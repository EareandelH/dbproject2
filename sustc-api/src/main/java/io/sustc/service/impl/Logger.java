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
        try{
            File logfiles = new File("..\\logs");
            logfiles.mkdir();
        }catch (Exception e){
            e.printStackTrace();
        }
        dir = "..\\logs\\logdata_"+time+".txt";
//        System.out.println("The log info would be stored at "+dir);
        try {
            file = new File(dir);
            if (!file.exists()){
                file.createNewFile();
                System.out.println("The log info would be stored at "+dir);
            }
            //创建BufferedWriter对象并向文件写入内容
        }catch (Exception e) {
            e.printStackTrace();
        }

    }
    public void debug(String info){
        try {
            bw = new BufferedWriter(new FileWriter(file));
            bw.write("[DEBUG] "+info+" [Time]"+new Date());
            bw.newLine();
            bw.flush();
            bw.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public void sql(String info){
        try {
            bw = new BufferedWriter(new FileWriter(file));
            bw.write("[SQL] "+info+" [Time]"+new Date());
            bw.newLine();
            bw.flush();
            bw.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public void function(String info){
        try {
            bw = new BufferedWriter(new FileWriter(file,true));
            bw.write("[Function] "+info+" [Time]"+new Date());
            bw.newLine();
            bw.flush();
            bw.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
