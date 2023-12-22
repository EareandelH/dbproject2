package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DanmuService;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    //private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        //throw new UnsupportedOperationException("TODO: replace this with your own student id");
        return Arrays.asList(12211615, 12210001, 12210002);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        long start_time = System.currentTimeMillis();
        String sql_user = "insert into t_user (mid, coins, name, sex, birthday, level, sign, identity, password, qq, wechat) " +
                "values (?,?,?,?,?,?,?,?,?,?,?)";
        String sql_following = "insert into follows (followee, follower) " +
                "values (?,?)";
        String sql_video = "insert into videos (bv,title,mid,name,commit_time,review_time,public_time,duration,description,reviewer) " +
                "values(?,?,?,?,?,?,?,?,?,?)";
        String sql_liker = "insert into liker (bv,mid) values(?,?)";
        String sql_coin = "insert into coin (bv,mid) values(?,?)";
        String sql_favorite = "insert into favorite (bv,mid) values(?,?)";
        String sql_View = "insert into View (bv,mid,view) values(?,?,?)";
        String sql_danmu = "insert into danmu (id, bv, mid, time, content, postTime) values(?,?,?,?,?,?)";//add postTime
        String sql_Danmu_like = "insert into Danmu_like (id, likeBy) values (?, ?)";
        final int BATCH_SIZE = 100000;
        final int BATCH2_SIZE = 100000;
        Connection conn = null;
        try {
            conn = ConnectionPool.getConnection();
            PreparedStatement statement_user = conn.prepareStatement(sql_user);
            PreparedStatement statement_following = conn.prepareStatement(sql_following);
            PreparedStatement statement_video = conn.prepareStatement(sql_video);
            PreparedStatement statement_liker = conn.prepareStatement(sql_liker);
            PreparedStatement statement_coin = conn.prepareStatement(sql_coin);
            PreparedStatement statement_favorite = conn.prepareStatement(sql_favorite);
            PreparedStatement statement_View = conn.prepareStatement(sql_View);
            PreparedStatement statement_danmu = conn.prepareStatement(sql_danmu);
            PreparedStatement statement_Danmu_like = conn.prepareStatement(sql_Danmu_like);
            conn.setAutoCommit(false);
            int cnt = 0;
            int cnt_following = 0;
            for (UserRecord userRecord : userRecords) {
                cnt++;
                statement_user.setLong(1, userRecord.getMid());
                statement_user.setLong(2, userRecord.getCoin());
                statement_user.setString(3, userRecord.getName());
                statement_user.setString(4, userRecord.getSex());
                statement_user.setString(5, userRecord.getBirthday());
                statement_user.setInt(6, userRecord.getLevel());
                statement_user.setString(7, userRecord.getSign());
                statement_user.setString(8, String.valueOf(userRecord.getIdentity()));
                statement_user.setString(9, userRecord.getPassword());
                statement_user.setString(10, userRecord.getQq());
                statement_user.setString(11, userRecord.getWechat());
                statement_user.addBatch();
                System.out.println(cnt);
                if (cnt % BATCH_SIZE == 0) {
                    statement_user.executeBatch();
                    statement_user.clearBatch();
                    conn.commit();
                }
                //cnt_following = 0;
            }
            statement_user.executeBatch();
            statement_user.clearBatch();
            conn.commit();
            statement_user.close();

            for (UserRecord userRecord : userRecords) {
                int following = 0;
                for (int i = 0; i < userRecord.getFollowing().length; i++) {
                    statement_following.setLong(1, userRecord.getMid());
                    statement_following.setLong(2, userRecord.getFollowing()[following]);
                    statement_following.addBatch();
                    cnt_following++;
                    following++;
                    System.out.println(cnt_following);
                    if (cnt_following % BATCH_SIZE == 0) {
                        statement_following.executeBatch();
                        statement_following.clearBatch();
                        conn.commit();
                    }
                }
            }
            statement_following.executeBatch();
            statement_following.clearBatch();
            conn.commit();
            statement_following.close();
            cnt = 0;

            System.out.println("***************video********************");

            int cnt_liker = 0;
            int cnt_coin = 0;
            int cnt_favourite = 0;
            int cnt_view = 0;
            for (VideoRecord videoRecord : videoRecords) {
                cnt++;
                statement_video.setString(1, videoRecord.getBv());
                statement_video.setString(2, videoRecord.getTitle());
                statement_video.setLong(3, videoRecord.getOwnerMid());
                statement_video.setString(4, videoRecord.getOwnerName());
                statement_video.setTimestamp(5, videoRecord.getCommitTime());
                statement_video.setTimestamp(6, videoRecord.getReviewTime());
                statement_video.setTimestamp(7, videoRecord.getPublicTime());
                statement_video.setFloat(8, videoRecord.getDuration());
                statement_video.setString(9, videoRecord.getDescription());
                statement_video.setLong(10, videoRecord.getReviewer());
                statement_video.addBatch();
                System.out.println(cnt);
                if (cnt % BATCH2_SIZE == 0) {
                    statement_video.executeBatch();
                    statement_video.clearBatch();
                    conn.commit();
                }
            }
            statement_video.executeBatch();
            statement_video.clearBatch();
            conn.commit();
            statement_video.close();

            for (VideoRecord videoRecord : videoRecords) {
                int liker = 0;
                for (int i = 0; i < videoRecord.getLike().length; i++) {
                    statement_liker.setString(1, videoRecord.getBv());
                    statement_liker.setLong(2, videoRecord.getLike()[liker]);
                    statement_liker.addBatch();
                    cnt_liker++;
                    liker++;
                    if (cnt_liker % BATCH2_SIZE == 0) {
                        statement_liker.executeBatch();
                        statement_liker.clearBatch();
                        conn.commit();
                    }
                    System.out.println(cnt_liker);
                }
            }
            statement_liker.executeBatch();
            statement_liker.clearBatch();
            conn.commit();
            statement_liker.close();

            for (VideoRecord videoRecord : videoRecords) {
                int coin = 0;
                for (int i = 0; i < videoRecord.getCoin().length; i++) {
                    statement_coin.setString(1, videoRecord.getBv());
                    statement_coin.setLong(2, videoRecord.getCoin()[coin]);
                    statement_coin.addBatch();
                    cnt_coin++;
                    coin++;
                    if (cnt_coin % BATCH2_SIZE == 0) {
                        statement_coin.executeBatch();
                        statement_coin.clearBatch();
                        conn.commit();
                    }
                    System.out.println(cnt_coin);
                }
            }
            statement_coin.executeBatch();
            statement_coin.clearBatch();
            conn.commit();
            statement_coin.close();

            for (VideoRecord videoRecord : videoRecords) {
                int favourite = 0;
                for (int i = 0; i < videoRecord.getFavorite().length; i++) {
                    statement_favorite.setString(1, videoRecord.getBv());
                    statement_favorite.setLong(2, videoRecord.getFavorite()[favourite]);
                    statement_favorite.addBatch();
                    cnt_favourite++;
                    favourite++;
                    if (cnt_favourite % BATCH2_SIZE == 0) {
                        statement_favorite.executeBatch();
                        statement_favorite.clearBatch();
                        conn.commit();
                    }
                    System.out.println(cnt_favourite);
                }
            }
            statement_favorite.executeBatch();
            statement_favorite.clearBatch();
            conn.commit();
            statement_favorite.close();
            System.out.println("View");
            for (VideoRecord videoRecord : videoRecords) {
                int view = 0;
                for (int i = 0; i < videoRecord.getViewerMids().length; i++) {
                    statement_View.setString(1, videoRecord.getBv());
                    statement_View.setLong(2, videoRecord.getViewerMids()[view]);
                    statement_View.setFloat(3, videoRecord.getViewTime()[view]);
                    statement_View.addBatch();
                    cnt_view++;
                    view++;
                    if (cnt_view % BATCH2_SIZE == 0) {
                        statement_View.executeBatch();
                        statement_View.clearBatch();
                        conn.commit();
                    }
                    System.out.println(cnt_view);
                }
            }
            statement_View.executeBatch();
            statement_View.clearBatch();
            conn.commit();
            statement_View.close();
            cnt = 0;
            System.out.println("***************danmu********************");

            int cnt_like = 0;
            for (DanmuRecord danmuRecord : danmuRecords) {
                cnt++;
                statement_danmu.setLong(1, cnt);
                statement_danmu.setString(2, danmuRecord.getBv());
                statement_danmu.setLong(3, danmuRecord.getMid());
                statement_danmu.setFloat(4, danmuRecord.getTime());
                statement_danmu.setString(5, danmuRecord.getContent());
                statement_danmu.setTimestamp(6, danmuRecord.getPostTime());
                statement_danmu.addBatch();

                int like = 0;
                for (int i = 0; i < danmuRecord.getLikedBy().length; i++) {
                    statement_Danmu_like.setLong(1, cnt);
                    statement_Danmu_like.setLong(2, danmuRecord.getLikedBy()[like]);
                    statement_Danmu_like.addBatch();
                    cnt_like++;
                    like++;
                    if (cnt_like % BATCH2_SIZE == 0) {
                        statement_Danmu_like.executeBatch();
                        statement_Danmu_like.clearBatch();
                        conn.commit();
                    }
                    System.out.println(cnt + "_______" +cnt_like);

                }
                statement_Danmu_like.executeBatch();
                statement_Danmu_like.clearBatch();
                conn.commit();

                if (cnt % BATCH2_SIZE == 0) {
                    statement_danmu.executeBatch();
                    statement_danmu.clearBatch();
                    conn.commit();
                }
                System.out.println(cnt);
            }
            statement_danmu.executeBatch();
            statement_danmu.clearBatch();
            conn.commit();
            statement_danmu.close();
            statement_Danmu_like.close();
            System.out.println("数据插入用时：" + (System.currentTimeMillis() - start_time) / 1000.000+"【单位：秒】");


        } catch (SQLException e) {
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            throw new RuntimeException(e);//检测sql正确
        }
    }

    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.
        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";
        Connection conn;
        try{
            conn = ConnectionPool.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";
        Connection conn;
        try {
            conn = ConnectionPool.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
