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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
    private DataSource dataSource;
    @Override
    public List<Integer> getGroupMembers() {
        //TODO: replace this with your own student IDs in your group
        return Arrays.asList(12210000, 12210001, 12210002);
    }
    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
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
        String sql_danmu = "insert into danmu (bv, mid, time, content, postTime) values(?,?,?,?,?)";//add postTime
        String sql_Danmu_like = "insert into Danmu_like (id, likeBy) values (?, ?)";

//        Logger logger = new Logger();
        AESCipher aesCipher = null;
        try {
            aesCipher = new AESCipher();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Connection conn = null;
        ExecutorService thread = Executors.newFixedThreadPool(10);
        try  {
            conn = ConnectionPool.getConnection();
            conn.setAutoCommit(false);
            for (UserRecord userRecord : userRecords) {
                AESCipher finalAesCipher1 = aesCipher;
                Connection finalConn = conn;
                thread.submit(() -> {
                    try (PreparedStatement statement_user = finalConn.prepareStatement(sql_user)) {
                        AESCipher finalAesCipher = finalAesCipher1;
                        String P = finalAesCipher.encrypt(userRecord.getPassword());
                        statement_user.setLong(1, userRecord.getMid());
                        statement_user.setLong(2, userRecord.getCoin());
                        statement_user.setString(3, userRecord.getName());
                        statement_user.setString(4, userRecord.getSex());
                        statement_user.setString(5, userRecord.getBirthday());
                        statement_user.setInt(6, userRecord.getLevel());
                        statement_user.setString(7, userRecord.getSign());
                        statement_user.setString(8, String.valueOf(userRecord.getIdentity()));
                        statement_user.setString(9, P);
                        statement_user.setString(10, userRecord.getQq());
                        statement_user.setString(11, userRecord.getWechat());
                        statement_user.addBatch();
                        statement_user.executeBatch();
                    } catch (SQLException e) {
//                        logger.debug(e.toString());
                        e.printStackTrace();
                    }
                });
            }
            thread.shutdown();
            // 等待所有任务完成
            thread.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            conn.commit();
        } catch (SQLException | InterruptedException e) {
//            logger.debug(e.toString());
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(conn);
        }


// 创建新的线程池
        ExecutorService followingThread = Executors.newFixedThreadPool(10);
        try {
            conn = ConnectionPool.getConnection();
            conn.setAutoCommit(false);

            for (UserRecord userRecord : userRecords) {
                followingThread.submit(() -> {
                    Connection finalConn = null;
                    try {
                        finalConn = ConnectionPool.getConnection();
                        finalConn.setAutoCommit(false);

                        PreparedStatement statement_following = finalConn.prepareStatement(sql_following);
                        long[] followingList = userRecord.getFollowing();

                        for (long following : followingList) {
                            statement_following.setLong(1, userRecord.getMid());
                            statement_following.setLong(2, following);
                            statement_following.addBatch();
                        }
                        // 执行批处理
                        statement_following.executeBatch();
                        finalConn.commit(); // 在这里进行提交
                    } catch (SQLException e) {
////                        logger.debug(e.toString());
                        e.printStackTrace();
                    } finally {
                        if (finalConn != null) {
                            ConnectionPool.releaseConnection(finalConn); // 确保在任务结束后释放连接
                        }
                    }
                });
            }
            followingThread.shutdown();
            followingThread.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS); // 等待所有任务完成
            conn.commit(); // 在此处进行提交主连接的事务
        } catch (SQLException | InterruptedException ex) {
//            logger.debug(ex.toString());
            throw new RuntimeException(ex);
        } finally {
            ConnectionPool.releaseConnection(conn);
        }

        ExecutorService videoThread = Executors.newFixedThreadPool(10);
        try {
            conn = ConnectionPool.getConnection();
            conn.setAutoCommit(false);
            for (VideoRecord videoRecord : videoRecords) {
                Connection finalConn1 = conn;
                videoThread.submit(() -> {
                    try (PreparedStatement statement_video = finalConn1.prepareStatement(sql_video)) {
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
                        statement_video.executeBatch();
                    } catch (SQLException e) {
//                        logger.debug(e.toString());
                        e.printStackTrace();
                    }
                });
            }
            videoThread.shutdown();
            videoThread.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            conn.commit();
        } catch (SQLException | InterruptedException e) {
//            logger.debug(e.toString());
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(conn);
        }

        ExecutorService likeThread = Executors.newFixedThreadPool(10);
        try {
            conn = ConnectionPool.getConnection();
            conn.setAutoCommit(false);
            for (VideoRecord videoRecord : videoRecords) {
                likeThread.submit(() -> {
                    Connection finalConn = null;
                    try {
                        finalConn = ConnectionPool.getConnection();
                        finalConn.setAutoCommit(false);

                        PreparedStatement statement_like = finalConn.prepareStatement(sql_liker);
                        long[] favouriteList = videoRecord.getLike();
                        for(long favourite:favouriteList){
                            statement_like.setString(1, videoRecord.getBv());
                            statement_like.setLong(2, favourite);
                            statement_like.addBatch();
                        }
                        statement_like.executeBatch();
                        finalConn.commit();;
                    } catch (SQLException e) {
//                        logger.debug(e.toString());
                        e.printStackTrace();
                    } finally {
                        if (finalConn != null) {
                            ConnectionPool.releaseConnection(finalConn); // 确保在任务结束后释放连接
                        }
                    }
                });
            }
            likeThread.shutdown();
            likeThread.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            conn.commit();
        } catch (SQLException | InterruptedException ex) {
//            logger.debug(ex.toString());
            throw new RuntimeException(ex);
        } finally {
            ConnectionPool.releaseConnection(conn);
        }

        ExecutorService coinThread = Executors.newFixedThreadPool(10);
        try {
            conn = ConnectionPool.getConnection();
            conn.setAutoCommit(false);
            for (VideoRecord videoRecord : videoRecords) {
                coinThread.submit(() -> {
                    Connection finalConn = null;
                    try {
                        finalConn = ConnectionPool.getConnection();
                        finalConn.setAutoCommit(false);

                        PreparedStatement statement_coin = finalConn.prepareStatement(sql_coin);
                        long[] coinList = videoRecord.getCoin();
                        for(long coin:coinList){
                            statement_coin.setString(1, videoRecord.getBv());
                            statement_coin.setLong(2, coin);
                            statement_coin.addBatch();
                        }
                        statement_coin.executeBatch();
                        finalConn.commit();;
                    } catch (SQLException e) {
//                        logger.debug(e.toString());
                        e.printStackTrace();
                    } finally {
                        if (finalConn != null) {
                            ConnectionPool.releaseConnection(finalConn); // 确保在任务结束后释放连接
                        }
                    }
                });
            }
            coinThread.shutdown();
            coinThread.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            conn.commit();
        } catch (SQLException | InterruptedException ex) {
//            logger.debug(ex.toString());
            throw new RuntimeException(ex);
        } finally {
            ConnectionPool.releaseConnection(conn);
        }

        ExecutorService favouriteThread = Executors.newFixedThreadPool(10);
        try {
            conn = ConnectionPool.getConnection();
            conn.setAutoCommit(false);
            for (VideoRecord videoRecord : videoRecords) {
                favouriteThread.submit(() -> {
                    Connection finalConn = null;
                    try {
                        finalConn = ConnectionPool.getConnection();
                        finalConn.setAutoCommit(false);

                        PreparedStatement statement_favorite = finalConn.prepareStatement(sql_favorite);
                        long[] favouriteList = videoRecord.getFavorite();
                        for(long favourite:favouriteList){
                            statement_favorite.setString(1, videoRecord.getBv());
                            statement_favorite.setLong(2, favourite);
                            statement_favorite.addBatch();
                        }
                        statement_favorite.executeBatch();
                        finalConn.commit();;
                    } catch (SQLException e) {
//                        logger.debug(e.toString());
                        e.printStackTrace();
                    } finally {
                        if (finalConn != null) {
                            ConnectionPool.releaseConnection(finalConn); // 确保在任务结束后释放连接
                        }
                    }
                });
            }
            favouriteThread.shutdown();
            favouriteThread.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            conn.commit();
        } catch (SQLException | InterruptedException ex) {
//            logger.debug(ex.toString());
            throw new RuntimeException(ex);
        } finally {
            ConnectionPool.releaseConnection(conn);
        }

        ExecutorService viewThread = Executors.newFixedThreadPool(10);
        try {
            conn = ConnectionPool.getConnection();
            conn.setAutoCommit(false);
            for (VideoRecord videoRecord : videoRecords) {
                viewThread.submit(() -> {
                    Connection finalConn = null;
                    try {
                        finalConn = ConnectionPool.getConnection();
                        finalConn.setAutoCommit(false);

                        PreparedStatement statement_View = finalConn.prepareStatement(sql_View);
                        long[] viewList1 = videoRecord.getViewerMids();
                        float[] viewList2 = videoRecord.getViewTime();
                        for (int i = 0; i < viewList1.length; i++) {
                            statement_View.setString(1, videoRecord.getBv());
                            statement_View.setLong(2, viewList1[i]);
                            statement_View.setFloat(3, viewList2[i]);
                            statement_View.addBatch();
                        }
                        statement_View.executeBatch();
                        finalConn.commit();;
                    } catch (SQLException e) {
//                        logger.debug(e.toString());
                        e.printStackTrace();
                    } finally {
                        if (finalConn != null) {
                            ConnectionPool.releaseConnection(finalConn); // 确保在任务结束后释放连接
                        }
                    }
                });
            }
            viewThread.shutdown();
            viewThread.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            conn.commit();
        } catch (SQLException | InterruptedException ex) {
//            logger.debug(ex.toString());
            throw new RuntimeException(ex);
        } finally {
            ConnectionPool.releaseConnection(conn);
        }

        ExecutorService danmuThread = Executors.newFixedThreadPool(10);
        try{
            conn = ConnectionPool.getConnection();
            conn.setAutoCommit(false);

            for (DanmuRecord danmuRecord : danmuRecords) {
                danmuThread.submit(() -> {
                   Connection finalConn = null;
                   try {
                       finalConn = ConnectionPool.getConnection();
                       finalConn.setAutoCommit(false);

                       PreparedStatement statement_danmu = finalConn.prepareStatement(sql_danmu, Statement.RETURN_GENERATED_KEYS);
                       PreparedStatement statement_Danmu_like = finalConn.prepareStatement(sql_Danmu_like);

                       statement_danmu.setString(1, danmuRecord.getBv());
                       statement_danmu.setLong(2, danmuRecord.getMid());
                       statement_danmu.setFloat(3, danmuRecord.getTime());
                       statement_danmu.setString(4, danmuRecord.getContent());
                       statement_danmu.setTimestamp(5, danmuRecord.getPostTime());
                       statement_danmu.addBatch();
                       statement_danmu.executeBatch();
                       ResultSet generatedKeys = statement_danmu.getGeneratedKeys();
                       if (generatedKeys.next()) {
                           int generatedId = generatedKeys.getInt(1); // 获取插入的自增id值
                           long[] danmuLike = danmuRecord.getLikedBy();
                           for (long liker:danmuLike) {
                               statement_Danmu_like.setLong(1, generatedId);
                               statement_Danmu_like.setLong(2, liker);
                               statement_Danmu_like.addBatch();
                           }
                       }
                       statement_Danmu_like.executeBatch();
                       finalConn.commit();
                   }catch (SQLException e){
//                       logger.debug(e.toString());
                       e.printStackTrace();
                   }finally {
                       if(finalConn != null){
                           ConnectionPool.releaseConnection(finalConn);
                       }
                   }
                });
            }
            danmuThread.shutdown();
            danmuThread.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            conn.commit();
        }catch (SQLException | InterruptedException ex){
//            logger.debug(ex.toString());
            throw new RuntimeException(ex);
        }finally {
            ConnectionPool.releaseConnection(conn);
        }
    }
    // TODO: implement your import logic

    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
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
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
//        Logger logger = new Logger();
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
//            logger.sql(stmt.toString());
            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
//            logger.debug(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
