package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@Data
public class DanmuServiceImpl implements DanmuService {
    @Autowired
    private DataSource dataSource;

    public boolean checkUser(AuthInfo auth) throws Exception {
        AESCipher aesCipher = new AESCipher();
        Logger logger = new Logger();
        String sql_checkMid = "select * from t_user where mid = ?;";
        String sql_checkWechat = "select * from t_user where wechat = ?;";
        String sql_checkQq = "select * from t_user where qq = ?;";

        String password;

        boolean result1;
        boolean result2;
        boolean result3;
        Connection conn = null;
        try {
            conn = ConnectionPool.getConnection();
            PreparedStatement statement_1 = conn.prepareStatement(sql_checkMid);
            statement_1.setLong(1, auth.getMid());
            ResultSet rs_1 = statement_1.executeQuery();
            logger.sql(statement_1.toString());
            if (rs_1.next()) {
                password = rs_1.getString("password");
                String P = aesCipher.decrypt(password);
                result1 = P.equals(auth.getPassword());
            } else {
                result1 = false;
            }
        } catch (SQLException e) {
            ConnectionPool.releaseConnection(conn);
            return false;
        }
        try {
            PreparedStatement statement_2 = conn.prepareStatement(sql_checkWechat);
            statement_2.setString(1, auth.getWechat());
            ResultSet rs_2 = statement_2.executeQuery();
            result2 = rs_2.next();
            logger.sql(statement_2.toString());
        } catch (SQLException e) {
            ConnectionPool.releaseConnection(conn);
            return false;
        }
        try {
            PreparedStatement statement_3 = conn.prepareStatement(sql_checkQq);
            statement_3.setString(1, auth.getQq());
            ResultSet rs_3 = statement_3.executeQuery();
            result3 = rs_3.next();
            logger.sql(statement_3.toString());
        } catch (SQLException e) {
            ConnectionPool.releaseConnection(conn);
            return false;
        } finally {
            ConnectionPool.releaseConnection(conn);
        }
        return result1 || result2 || result3;
    }

    static long get_mid(AuthInfo auth, String sentence1, String sentence2) {
        long mid = 0;
        Logger logger = new Logger();
        Connection conn = null;
        if (auth.getQq() != null) {//have qq
            try {
                conn = ConnectionPool.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sentence1);
                stmt.setString(1, auth.getQq());
                logger.sql(stmt.toString());
                ResultSet rs_1 = stmt.executeQuery();
                if (rs_1.next()) {
                    mid = rs_1.getLong("mid");
                    stmt.close();
                }
            } catch (SQLException e) {
                System.out.println("select wrong");
            } finally {
                ConnectionPool.releaseConnection(conn);
            }
        } else {//have wechat
            try {
                conn = ConnectionPool.getConnection();
                PreparedStatement stmt = conn.prepareStatement(sentence2);
                stmt.setString(1, auth.getWechat());
                logger.sql(stmt.toString());
                ResultSet rs_2 = stmt.executeQuery();
                if (rs_2.next()) {
                    mid = rs_2.getLong("mid");
                    stmt.close();
                }
            } catch (SQLException e) {
                System.out.println("select wrong");
            } finally {
                ConnectionPool.releaseConnection(conn);
            }
        }
        return mid;
    }

    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        Logger logger = new Logger();
        String sql_insertDanmu = "insert into Danmu (mid, time, content, postTime, bv) values (?, ?, ?, ?, ?)";
        String sql_findMid_q = "select mid from t_user where qq = ?;";
        String sql_findMid_w = "select mid from t_user where wechat = ?;";
        String sql_isView = "select mid from view where bv = ? and mid = ?;";
        String sql_duration = "select duration from videos where bv = ?;";
        long mid = auth.getMid();
        if (mid == 0) {
            mid = get_mid(auth, sql_findMid_q, sql_findMid_w);
        }
        Connection conn = null;
        try {
            conn = ConnectionPool.getConnection();
            conn.setAutoCommit(false);
            //check user
            if (!checkUser(auth) || content == null) {
                System.out.println("The auth is invalid");
                return -1;
            }
            //check duration check view insert
            PreparedStatement stmt_1 = conn.prepareStatement(sql_isView);
            PreparedStatement stmt_2 = conn.prepareStatement(sql_duration);
            PreparedStatement stmt_3 = conn.prepareStatement(sql_insertDanmu, Statement.RETURN_GENERATED_KEYS);

            stmt_1.setString(1, bv);
            stmt_1.setLong(2, mid);
            stmt_2.setString(1, bv);
            ResultSet rst_2 = stmt_2.executeQuery();
            ResultSet rst = stmt_1.executeQuery();
            if (!rst.next()) {
                return -5;
            }
            rst_2.next();
            float duration = rst_2.getFloat("duration");
            if (time < 0 || time > duration) return -6;

            LocalDateTime currentDateTime = LocalDateTime.now(); // 获取当前日期时间
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
            String formattedDateTime = currentDateTime.format(formatter);
            Timestamp timestamp = Timestamp.valueOf(formattedDateTime);
            stmt_3.setLong(1, mid);//no mid
            stmt_3.setDouble(2, time);
            stmt_3.setString(3, content);
            stmt_3.setTimestamp(4, timestamp);//时间格式可能需要修改
            stmt_3.setString(5, bv);
            logger.sql(stmt_3.toString());
            int affectedRows = stmt_3.executeUpdate();//执行插入操作，返回受影响的行数
            conn.commit();
            if (affectedRows > 0) {
                ResultSet generatedKeys = stmt_3.getGeneratedKeys();
                if (generatedKeys.next()) {
                    System.out.println("success");//delete
                    return generatedKeys.getInt(1); // 返回插入的danmu的id
                } else {
                    System.out.println("failure insert__second");//delete
                    return -2;
                }
            } else {
                System.out.println("failure insert__first");//delete
                return -3;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            ConnectionPool.releaseConnection(conn);
        }
    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        Logger logger = new Logger();
        if (timeStart < 0 || timeEnd < 0 || timeEnd < timeStart) {
            System.out.println("bound of time is wrong");
            return null;
        }
        long duration;
        String sql_findDuration = "select duration from videos where bv = ?;";
        Connection conn = null;
        try {
            conn = ConnectionPool.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql_findDuration);
            stmt.setString(1, bv);
            logger.sql(stmt.toString());
            ResultSet rs = stmt.executeQuery();//获取结果集
            if (rs.next()) { // 移动游标到第一行
                duration = rs.getLong("duration"); // 获取时长
                stmt.close();
            } else {
                System.out.println("no duration");//delete
                stmt.close();
                return null;
            }
        } catch (SQLException e) {//找不到bv，错误
            logger.debug(e.getMessage());
            System.out.println("Wrong SQL : " + e.getMessage());//delete
            return null;
            //throw new RuntimeException(e);检查是否代码正确
        } finally {
            ConnectionPool.releaseConnection(conn);
        }

        if (timeStart > duration || timeEnd > duration) {
            logger.debug("wrong time");
            System.out.println("wrong time");//delete
            return null;
        }
        String sql_findID;
        if (!filter) {
            sql_findID = """
                    select id
                    from Danmu where bv = ?
                    and time between ? and ? ;""";
        } else {
            sql_findID = """
                    select id
                    from Danmu
                    where bv = ?
                    and time between ? and ?
                    group by content, id;""";
        }

        List<Long> danmuID = new ArrayList<>();
        conn = null;
        try {
            conn = ConnectionPool.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql_findID);
            stmt.setString(1, bv);
            stmt.setDouble(2, timeStart);
            stmt.setDouble(3, timeEnd);
            logger.sql(stmt.toString());
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                long id = rs.getLong("id");
                danmuID.add(id);
            }
            stmt.close();
            if (danmuID.size() != 0) {
                System.out.println("success");//delete
                return danmuID;
            }
        } catch (SQLException e) {
            logger.debug(e.getMessage());
            System.out.println("wrong message: " + e.getMessage());//delete
        } finally {
            ConnectionPool.releaseConnection(conn);
        }
        return null;
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {//danmu的id
        Logger logger = new Logger();
        try {
            if (!checkUser(auth)) return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //需要判断是否看过视频，看过视频，下面操作，没看过，直接false
        String sql_findMid_q = "select mid from t_user where qq = ?;";
        String sql_findMid_w = "select mid from t_user where wechat = ?;";
        Connection conn = null;
        long mid = auth.getMid();
        if (mid == 0) {
            mid = get_mid(auth, sql_findMid_q, sql_findMid_w);
        }
        //find mid
        String bv;
        String find_bv = "select bv from danmu where id = ?;";
        try {
            conn = ConnectionPool.getConnection();
            PreparedStatement stmt = conn.prepareStatement(find_bv);
            stmt.setLong(1, id);
            ResultSet rst = stmt.executeQuery();
            if (rst.next()) {
                bv = rst.getString("bv");
                stmt.close();
            } else {
                //System.out.println("can't find bv");//delete
                return false;
            }
        } catch (SQLException e) {
            //System.out.println("find_bv wrong");//delete
            return false;
        } finally {
            ConnectionPool.releaseConnection(conn);
        }
        //find bv
        String is_view = "select * from view where mid = ? and bv = ?;";
        try {
            conn = ConnectionPool.getConnection();
            PreparedStatement stmt = conn.prepareStatement(is_view);
            stmt.setLong(1, mid);
            stmt.setString(2, bv);
            ResultSet rst = stmt.executeQuery();
            if (!rst.next()) {
                //System.out.println("user haven't seen");
                return false;
            }
        } catch (SQLException e) {
            //System.out.println("is_view sql wrong");//delete
            return false;
        }finally {
            ConnectionPool.releaseConnection(conn);
        }
        String sql_check_likeBy = """
                select d.id
                from Danmu_like d
                where id = ?
                and d.likeBy = ?;""";
        try {
            conn = ConnectionPool.getConnection();
            PreparedStatement stmt_2 = conn.prepareStatement(sql_check_likeBy);
            stmt_2.setLong(1, id);
            stmt_2.setLong(2, auth.getMid());
            logger.sql(stmt_2.toString());
            int affectedRows_2 = stmt_2.executeUpdate();
            if (affectedRows_2 > 0) {//已经点过赞
                String sql_delete = "delete from Danmu_like where id = ? and likeBy = ?;";
                try (PreparedStatement stmt_3 = conn.prepareStatement(sql_delete)) {
                    stmt_3.setLong(1, id);
                    stmt_3.setLong(2, auth.getMid());
                    logger.sql(stmt_3.toString());
                    stmt_3.executeUpdate();
                    conn.commit();
                    return false;
                }finally {
                    ConnectionPool.releaseConnection(conn);
                }
            } else {//未点过赞
                String sql_like = "insert into Danmu_like values (?, ?)";
                try (PreparedStatement stmt_3 = conn.prepareStatement(sql_like)) {
                    stmt_3.setLong(1, id);
                    stmt_3.setLong(2, auth.getMid());
                    logger.sql(stmt_3.toString());
                    stmt_3.executeUpdate();
                    conn.commit();
                    return true;
                }finally {
                    ConnectionPool.releaseConnection(conn);
                }
            }
        } catch (SQLException e) {
            //System.out.println("like or no like wrong");//delete
            logger.debug("debug :" + e);
            throw new RuntimeException(e);
        } finally {
            ConnectionPool.releaseConnection(conn);
        }
    }
}
