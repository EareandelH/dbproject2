package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
@Service

@Slf4j
public class DanmuServiceImpl implements DanmuService {
    private DataSource dataSource;
    static long id_max = -1;
    public void getID(){
        String sql_selectID = "select count(*) from Danmu;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql_selectID)) {
            log.info("SQL: {}", stmt);//日志输出，用于打印即将执行的SQL操作

            ResultSet rs = stmt.executeQuery();//执行查询操作，用于获取查询结果集
            rs.next();
            id_max = rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    /*
找不到与mid对应的用户
身份验证无效
qq和微信都不是空的，但它们不对应同一用户
mid无效，而qq和微信都无效（空或找不到）
当前用户是常规用户，而中间用户不是他/她的
当前用户是超级用户，而mid既不是普通用户的mid，也不是他/她的mid
cannot find a user corresponding to the mid
the auth is invalid
both qq and wechat are non-empty while they do not correspond to same user
mid is invalid while qq and wechat are both invalid (empty or not found)
the current user is a regular user while the mid is not his/hers

public class AuthInfo {
      The user's mid.
    private long mid;
      The password used when login by mid.
    private String password;
      OIDC login by QQ, does not require a password.
    private String qq;
      OIDC login by WeChat, does not require a password.
    private String wechat;
}
*/
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
    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        if(!checkUser(auth) || Objects.equals(content, "") || content == null) return -1;
        if(id_max == -1) getID();
        String sql_insertDanmu = "insert into values (?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql_insertDanmu)) {
            id_max++;
            LocalDateTime currentDateTime = LocalDateTime.now(); // 获取当前日期时间
            Timestamp timestamp = Timestamp.valueOf(currentDateTime);
            stmt.setLong(1,id_max);
            stmt.setString(2, bv);
            stmt.setLong(3, auth.getMid());
            stmt.setBigDecimal(4, BigDecimal.valueOf(time));
            stmt.setString(5,content);
            stmt.setTimestamp(6, timestamp);//时间格式可能需要修改
            log.info("SQL: {}", stmt);//日志输出，用于打印即将执行的SQL操作
            int affectedRows = stmt.executeUpdate();//执行插入操作，返回受影响的行数
            if (affectedRows > 0) {
                ResultSet generatedKeys = stmt.getGeneratedKeys();
                if (generatedKeys.next()) {
                    return generatedKeys.getInt(1); // 返回插入的danmu的id
                }else {
                    return -1;
                }
            }else {
                return -1;
            }
        } catch (SQLException e) {//找不到相应的bv
            return -1;
        }
    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {

        List<Long> danmuID = new ArrayList<>();

        if(timeStart < 0 || timeEnd < 0 || timeEnd < timeStart) return null;

        long duration;
        String sql_findDuration = "select duration\n" +
                "from videos where bv = '?';";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql_findDuration)){
            stmt.setString(1,bv);
            log.info("SQL:{}",stmt);
            ResultSet rs = stmt.executeQuery();//获取结果集
            duration = rs.getLong("duration");//找到bv，找到时长
        } catch (SQLException e) {//找不到bv，错误
            return null;
            //throw new RuntimeException(e);检查是否代码正确
        }

        if(timeStart > duration || timeEnd > duration) return null;
        String sql_findID;
        if(!filter){
            sql_findID = "select id\n" +
                    "from Danmu where bv = '?'\n" +
                    "and time between ? and ? ;";
        }else {
            sql_findID = "select min(id)\n" +
                    "from Danmu\n" +
                    "where bv = '?'\n" +
                    "and time between ? and ?\n" +
                    "group by content;";
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql_findID)){
            stmt.setString(1,bv);
            stmt.setDouble(2,timeStart);
            stmt.setDouble(3,timeEnd);
            log.info("SQL:{}",stmt);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){
                long id = rs.getLong("id");
                danmuID.add(id);
            }
        }catch (SQLException e){
            throw new RuntimeException(e);//检测sql正确
        }
        return danmuID;
    }
    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {//danmu的id
        if(!checkUser(auth))return false;
        String sql_check = "select d.id\n" +
                "from Danmu_like d\n" +
                "where id = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql_check)){
            stmt.setLong(2, id);

            log.info("SQL: {}", stmt);//日志输出，用于打印即将执行的SQL操作
            int affectedRows = stmt.executeUpdate();//执行插入操作，返回受影响的行数
            if (affectedRows > 0) {//说明找到id
                String sql_check_likeBy = "select d.id\n" +
                        "from Danmu_like d\n" +
                        "where id = 213\n" +
                        "and d.likeBy = 123;";
                try (PreparedStatement stmt_2 = conn.prepareStatement(sql_check_likeBy)){
                    stmt_2.setLong(1, id);
                    stmt_2.setLong(2,auth.getMid());
                    log.info("SQL:{}",stmt_2);
                    int affectedRows_2 = stmt_2.executeUpdate();
                    if(affectedRows_2 > 0){//已经点过赞
                        String sql_delete = "delete from Danmu_like where id = ? and likeBy = ?;";
                        try (PreparedStatement stmt_3 = conn.prepareStatement(sql_delete)){
                            stmt_3.setLong(1,id);
                            stmt_3.setLong(2,auth.getMid());
                            log.info("SQL:{}",stmt_3);
                            stmt.executeUpdate();
                            return true;
                        }
                    }else {//未点过赞
                        String sql_like = "insert into Danmu values (?, ?)";
                        try (PreparedStatement stmt_3 = conn.prepareStatement(sql_like)){
                            stmt_3.setLong(1,id);
                            stmt_3.setLong(2,auth.getMid());
                            log.info("SQL:{}",stmt_3);
                            stmt.executeUpdate();
                            return true;
                        }
                    }
                }
            }else {//找不到这个id
                return false;
            }
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }
}
