package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.DatabaseService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import io.sustc.dto.PostVideoReq;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.security.SecureRandom;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j

public class UserServiceImpl implements io.sustc.service.UserService {
    private DataSource dataSource;
    public long register(RegisterUserReq req){
        try{
            Connection con = dataSource.getConnection();
            String password = req.getPassword();
            if(password==null||password.equals("")){
                System.out.println("Your password cannot be empty.");
                return -1;
            }
            Random random = new Random();
            long mid=Math.abs(random.nextLong());
            while(selectUser_mid(mid)!=null){
                mid=Math.abs(random.nextLong());
            }
            String qq=req.getQq();
            String wechat=req.getWechat();
            String name= req.getName();
            RegisterUserReq.Gender sex=req.getSex();
            String birthday = req.getBirthday();
            String sign = req.getSign();
            if(name==null||name.equals("")){
                System.out.println("Your name cannot be empty.");
                return -1;
            }
            if(sex==null){
                System.out.println("Your sex cannot be empty.");
                return -1;
            }
            if(!check_birthday(birthday)){
                System.out.println("Your birthday is invalid.");
                System.out.println("The form must be like /'X月X日/'.");
                return -1;
            }
            if(selectUser_name(name)!=null){
                System.out.println("The name has been used.");
                return -1;
            }
            if(selectUser_qq(qq)!=null){
                System.out.println("The qq has been used.");
                return -1;
            }
            if(selectUser_wechat(wechat)!=null){
                System.out.println("The wechat has been used.");
                return -1;
            }
            String sql ="insert into t_user (mid, coins,name, sex, birthday, level, sign, identity,password,qq,wechat) " +
                    "values (?,?,?,?,?,?,?,?,?,?,?)";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setLong(1,mid);
            statement.setInt(2,0);
            statement.setString(3,name);
            statement.setString(4,sex.toString());
            statement.setString(5,birthday);
            statement.setInt(6,1);
            statement.setString(7,sign);
            statement.setString(8,"USER");
            statement.setString(9,password);
            statement.setString(10,qq);
            statement.setString(11,wechat);
            int affected=statement.executeUpdate();
            if(affected>0){
                System.out.println(name+" welcome!");
                return mid;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return -1;
    }
    public boolean deleteAccount(AuthInfo auth, long mid){
        try{
            Connection con =dataSource.getConnection();
            UserRecord userRecord_mid = selectUser_mid(mid);
            if(userRecord_mid==null){
                System.out.println("Cannot find a user corresponding to the mid: "+mid);
                return false;
            }
            if(!checkUser(auth)){
                System.out.println("The auth is invalid");
                return false;
            }
            UserRecord userRecord_current=selectUser_mid(auth.getMid());
            if(userRecord_current.getIdentity()== UserRecord.Identity.USER&&auth.getMid()!=mid){
                System.out.println("Deer regular user ,you cannot delete the account which not belong to you");
                return false;
            }
            if(auth.getMid()!=mid&&userRecord_current.getIdentity()== UserRecord.Identity.SUPERUSER&&userRecord_mid.getIdentity()== UserRecord.Identity.SUPERUSER){
                System.out.println("You cannot delete the account belong to another SUPERUSER");
                return false;
            }
            String sql="delete from t_user where mid=?";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setLong(1,mid);
            int affected = statement.executeUpdate();
            if(affected>0){
                System.out.println("You have delete the account "+mid);
                return true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
    public boolean follow(AuthInfo auth, long followeeMid){
        try{
            Connection con =dataSource.getConnection();
            long followerMid =auth.getMid();

            if(!checkUser(auth)){
                System.out.println("The auth is invalid");
                return false;
            }
            UserRecord userRecord_followee=selectUser_mid(followeeMid);
            if(userRecord_followee==null){
                System.out.println("Cannot find a mid corresponding to the "+followeeMid+".");
                return false;
            }
            UserRecord userRecord_current=selectUser_mid(auth.getMid());
            if(userRecord_current.getMid()==userRecord_followee.getMid()){
                System.out.println("You cannot follow yourself.");
                return false;
            }
            /*
            优化想法，这里包含一个查询，一个删除操作，可以直接使用删除操作看affected是否为0
            为0即是不存在这一行
             */
            if(user_following(followeeMid).contains(followerMid)){
                System.out.println("You canceled the follow.");
                cancel_follow(followeeMid,followerMid);
                return false;
            }
            String sql="insert into follows (followee,follower) values(?,?)";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setLong(1,followeeMid);
            statement.setLong(2,followerMid);
            int affected = statement.executeUpdate();
            if(affected>0){
                System.out.println("Successfully Followed!");
                return true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
    public UserInfoResp getUserInfo(long mid){
        UserRecord userRecord=selectUser_mid(mid);
        if(userRecord==null){
            System.out.println("Cannot find a user corresponding to the mid: "+mid);
            return null;
        }
        UserInfoResp userInfoResp= new UserInfoResp();
        userInfoResp.setMid(mid);
        userInfoResp.setCoin(userRecord.getCoin());
        userInfoResp.setCollected(user_collected(mid).toArray(new String[0]));
        userInfoResp.setLiked(user_liked(mid).toArray(new String[0]));
        userInfoResp.setFollowing(ArrayUtils.toPrimitive(user_following(mid).toArray(new Long[0])));
        userInfoResp.setFollower(ArrayUtils.toPrimitive(user_follower(mid).toArray(new Long[0])));
        userInfoResp.setWatched(user_watched(mid).toArray(new String[0]));
        userInfoResp.setPosted(user_watched(mid).toArray(new String[0]));
        return userInfoResp;
    }

    //----------------------------------------------------------------------------------------
    public  UserRecord selectUser_mid(long mid){
        try{
            Connection con = dataSource.getConnection();
            ResultSet re;
            String sql = "select * from t_user where mid=?";
            PreparedStatement statement= con.prepareStatement(sql);
            statement.setLong(1,mid);
            re=statement.executeQuery();
            if(re.next()){
                UserRecord userRecord=new UserRecord(re.getLong("mid"),re.getString("name"),
                        re.getString("sex"),re.getString("birthday"),re.getShort("level"),
                        re.getInt("coin"),re.getString("sign"),(UserRecord.Identity.valueOf(re.getString("identity"))),
                        re.getString("password"),re.getString("qq"),re.getString("wechat"));
                return userRecord;
            }
            else return null;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public  UserRecord selectUser_name(String name){
        try{
            Connection con = dataSource.getConnection();
            ResultSet re;
            String sql = "select * from t_user where name=?";
            PreparedStatement statement= con.prepareStatement(sql);
            statement.setString(1,name);
            re=statement.executeQuery();
            if(re.next()){
                UserRecord userRecord=new UserRecord(re.getLong("mid"),re.getString("name"),
                        re.getString("sex"),re.getString("birthday"),re.getShort("level"),
                        re.getInt("coin"),re.getString("sign"),(UserRecord.Identity.valueOf(re.getString("identity"))),
                        re.getString("password"),re.getString("qq"),re.getString("wechat"));
                return userRecord;
            }
            else return null;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public boolean cancel_follow(long followeeMid,long followerMid){
        try{
            Connection con =dataSource.getConnection();
            ResultSet re;
            String sql="delete from follows where followee=? and follower=?";
            PreparedStatement statement=con.prepareStatement(sql);
            statement.setLong(1,followeeMid);
            statement.setLong(2,followerMid);
            int affected =statement.executeUpdate();
            if(affected>0)
                System.out.println("Follow cancel succeed!");
            else System.out.println("Cancel failed!");
        }catch (Exception e){
            e.printStackTrace();
        }
        return false;
    }
    public  UserRecord selectUser_qq(String qq){
        try{
            Connection con = dataSource.getConnection();
            ResultSet re;
            String sql = "select * from t_user where qq=?";
            PreparedStatement statement= con.prepareStatement(sql);
            statement.setString(1,qq);
            re=statement.executeQuery();
            if(re.next()){
                UserRecord userRecord=new UserRecord(re.getLong("mid"),re.getString("name"),
                        re.getString("sex"),re.getString("birthday"),re.getShort("level"),
                        re.getInt("coin"),re.getString("sign"),(UserRecord.Identity.valueOf(re.getString("identity"))),
                        re.getString("password"),re.getString("qq"),re.getString("wechat"));
                return userRecord;
            }
            else return null;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public  UserRecord selectUser_wechat(String wechat){
        try{
            Connection con = dataSource.getConnection();
            ResultSet re;
            String sql = "select * from t_user where mid=?";
            PreparedStatement statement= con.prepareStatement(sql);
            statement.setString(1,wechat);
            re=statement.executeQuery();
            if(re.next()){
                UserRecord userRecord=new UserRecord(re.getLong("mid"),re.getString("name"),
                        re.getString("sex"),re.getString("birthday"),re.getShort("level"),
                        re.getInt("coin"),re.getString("sign"),(UserRecord.Identity.valueOf(re.getString("identity"))),
                        re.getString("password"),re.getString("qq"),re.getString("wechat"));
                return userRecord;
            }
            else return null;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public boolean checkUser(AuthInfo auth){
        String sql_checkMID = "select password,qq,wechat,identity\n" +
                "from t_user\n" +
                "where mid = ?;";
        String password;
        String qq;
        String wechat;
        String identity;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql_checkMID)){
            stmt.setLong(1,auth.getMid());
            log.info("SQL:{}",stmt);
            ResultSet rs = stmt.executeQuery();//获取结果集
            if(rs.next()){
                password = rs.getString("password");
                qq = rs.getString("qq");
                wechat = rs.getString("wechat");
                identity = rs.getString("identity");
            }else {//找不到mid
                return false;
            }
        } catch (SQLException e) {
            return false;
        }

        if(auth.getQq() != null && !auth.getQq().equals(qq) || auth.getWechat() != null && !auth.getWechat().equals(wechat))return false;//判断qq、wechat是本人
        if(qq == null && wechat == null && password == null) return false;//三个同时无
        return true;
    }
    public boolean check_birthday(String birthday){
        // 使用正则表达式匹配“XX月XX日”格式，月份和日期可以是单个或两个数字
        String regex = "^(0?[1-9]|1[0-2])月(0?[1-9]|[12][0-9]|3[01])日$";

        // 编译正则表达式
        Pattern pattern = Pattern.compile(regex);

        // 使用正则表达式匹配生日字符串
        Matcher matcher = pattern.matcher(birthday);

        // 如果格式匹配，进一步判断月份和日期的合法性
        if (matcher.matches()) {
            int month = Integer.parseInt(matcher.group(1));
            int day = Integer.parseInt(matcher.group(2));

            // 判断月份和日期的合法性
            if ((month == 2 && day <= 29) ||                     // 2月最多29天
                    ((month == 4 || month == 6 || month == 9 || month == 11) && day <= 30) ||  // 4、6、9、11月最多30天
                    ((month == 1 || month == 3 || month == 5 || month == 7 || month == 8 || month == 10 || month == 12) && day <= 31)) {  // 其他月份最多31天
                return true;
            }
        }

        // 生日不合法
        return false;

    }
    public ArrayList<String> user_coined(long mid){
        try{
            Connection con = dataSource.getConnection();
            ResultSet re;
            ArrayList<String> coins_Array=new ArrayList<>();
            String sql="select bv from coin where mid=?";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setLong(1,mid);
            re=statement.executeQuery();
            while(re.next()){
                String temp = re.getString("bv");
                coins_Array.add(temp);
            }
            return coins_Array;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public ArrayList<String> user_liked(long mid){
        try{
            Connection con = dataSource.getConnection();
            ResultSet re;
            ArrayList<String> liked_Array=new ArrayList<>();
            String sql="select bv from liker where mid=?";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setLong(1,mid);
            re=statement.executeQuery();
            while(re.next()){
                String temp = re.getString("bv");
                liked_Array.add(temp);
            }
            return liked_Array;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public ArrayList<String> user_collected(long mid){
        try{
            Connection con = dataSource.getConnection();
            ResultSet re;
            ArrayList<String> collected_Array=new ArrayList<>();
            String sql="select bv from favorite where mid=?";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setLong(1,mid);
            re=statement.executeQuery();
            while(re.next()){
                String temp = re.getString("bv");
                collected_Array.add(temp);
            }
            return collected_Array;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public ArrayList<Long> user_following(long mid){
        try{
            Connection con = dataSource.getConnection();
            ResultSet re;
            ArrayList<Long> following_Array=new ArrayList<>();
            String sql="select follower from follows where followee=?";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setLong(1,mid);
            re=statement.executeQuery();
            while(re.next()){
                Long temp = re.getLong("follower");
                following_Array.add(temp);
            }
            return following_Array;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public ArrayList<Long> user_follower(long mid){
        try{
            Connection con = dataSource.getConnection();
            ResultSet re;
            ArrayList<Long> follower_Array=new ArrayList<>();
            String sql="select followee from follows where follower=?";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setLong(1,mid);
            re=statement.executeQuery();
            while(re.next()){
                Long temp = re.getLong("followee");
                follower_Array.add(temp);
            }
            return follower_Array;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public ArrayList<String> user_watched(long mid){
        try{
            Connection con = dataSource.getConnection();
            ResultSet re;
            ArrayList<String> watched_Array=new ArrayList<>();
            String sql="select bv from views where mid=?";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setLong(1,mid);
            re=statement.executeQuery();
            while(re.next()){
                String temp = re.getString("bv");
                watched_Array.add(temp);
            }
            return watched_Array;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    public ArrayList<String> user_posted(long mid){
        try{
            Connection con = dataSource.getConnection();
            ResultSet re;
            ArrayList<String> posted_Array=new ArrayList<>();
            String sql="select bv from videos where mid=?";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setLong(1,mid);
            re=statement.executeQuery();
            while(re.next()){
                String temp = re.getString("bv");
                posted_Array.add(temp);
            }
            return posted_Array;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

}
