package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.RecommenderService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
@Slf4j
@Service
@Data
public class RecommenderServiceImpl implements RecommenderService {
    /**
     * Recommends a list of top 5 similar videos for a video.
     * The similarity is defined as the number of users (in the database) who have watched both videos.
     *
     * @param bv the current video
     * @return a list of video {@code bv}s
     * @apiNote You may consider the following corner cases:
     * <ul>
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     * </ul>
     * If any of the corner case happened, {@code null} shall be returned.
     */

    UserServiceImpl userService = new UserServiceImpl();

    @Override
    public List<String> recommendNextVideo(String bv) {
        /**
         * Recommends a list of top 5 similar videos for a video.
         * The similarity is defined as the number of users (in the database) who have watched both videos.
         *
         * @param bv the current video
         * @return a list of video {@code bv}s
         * @apiNote You may consider the following corner cases:
         * <ul>
         *   <li>cannot find a video corresponding to the {@code bv}</li>
         * </ul>
         * If any of the corner case happened, {@code null} shall be returned.
         */
        Connection con = null;

        io.sustc.service.impl.Logger logger = new Logger();
        try {
            //logger.function("recommendNextVideo");
            con = ConnectionPool.getConnection();
            ResultSet re;
            String sql = "select * from videos where bv = ?";

            PreparedStatement ps = con.prepareStatement(sql);
            ps.setString(1, bv);
            re = ps.executeQuery();
            boolean flag = false;
            String BV_String = null;
            ArrayList<String> s_list = new ArrayList<String>();
            while (re.next()) {
                flag = true;
                BV_String = re.getString("bv");
            }
            if (!flag) {
                return null;
            }
            sql = "SELECT v1.bv, COUNT(*) as similarity\n" +
                    "                  FROM View v1\n" +
                    "                    JOIN View v2 ON v1.mid = v2.mid AND v1.bv != v2.bv\n" +
                    "                    WHERE v2.bv = 'BV1Rg411w7uU'\n" +
                    "                    GROUP BY v1.bv\n" +
                    "                    ORDER BY similarity DESC, v1.bv ASC \n" +
                    "                    LIMIT 5;";
            ps = con.prepareStatement(sql);
            ps.setString(1, bv);
            re = ps.executeQuery();
            while (re.next()) {
                String similarBv = re.getString("bv");
                int similarity = re.getInt("similarity");
                //System.out.println("BV: " + similarBv + ", Similarity: " + similarity);
                s_list.add(similarBv);
            }
            return s_list;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            ConnectionPool.releaseConnection(con);
        }
    }

    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        /**
         * Recommends videos for anonymous users, based on the popularity.
         * Evaluate the video's popularity from the following aspects:
         * <ol>
         *   <li>"like": the rate of watched users who also liked this video</li>
         *   <li>"coin": the rate of watched users who also donated coin to this video</li>
         *   <li>"fav": the rate of watched users who also collected this video</li>
         *   <li>"danmu": the average number of danmus sent by one watched user</li>
         *   <li>"finish": the average video watched percentage of one watched user</li>
         * </ol>
         * The recommendation score can be calculated as:
         * <pre>
         *   score = like + coin + fav + danmu + finish
         * </pre>
         *
         * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
         * @param pageNum  the page number, starts from 1
         * @return a list of video {@code bv}s, sorted by the recommendation score
         * @implNote
         * Though users can like/coin/favorite a video without watching it, the rates of these values should be clamped to 1.
         * If no one has watched this video, all the five scores shall be 0.
         * If the requested page is empty, return an empty list.
         * @apiNote You may consider the following corner cases:
         * <ul>
         *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
         * </ul>
         * If any of the corner case happened, {@code null} shall be returned.
         */
        Connection con = null;
        Logger logger = new Logger();
        try {
            //logger.function("generalRecommendations"+" "+pageSize+" "+pageNum);
            if (pageNum <= 0 || pageSize <= 0) {
                return null;
            }
            con = ConnectionPool.getConnection();
            ResultSet re;
            String sql = "SELECT sub.bv,\n" +
                    "       sub.like_rate,\n" +
                    "       sub.coin_rate,\n" +
                    "       sub.fav_rate,\n" +
                    "       AVG(sub.danmu_count) AS avg_danmu,\n" +
                    "       sub.avg_finish,\n" +
                    "       (sub.like_rate + sub.coin_rate + sub.fav_rate + AVG(sub.danmu_count) + sub.avg_finish) AS score,\n" +
                    "       sub.total_views as total_view\n" +
                    "FROM (\n" +
                    "    SELECT v.bv,\n" +
                    "           COALESCE(l.like_count, 0.0) / NULLIF(vws.total_views, 0) AS like_rate,\n" +
                    "           COALESCE(c.coin_count, 0.0) / NULLIF(vws.total_views, 0) AS coin_rate,\n" +
                    "           COALESCE(f.fav_count, 0.0) / NULLIF(vws.total_views, 0) AS fav_rate,\n" +
                    "           COALESCE(d.danmu_count,0.0) / NULLIF(vws.total_views, 0.0) AS danmu_count,\n" +
                    "           SUM(vw.view) / NULLIF(COUNT(vw.view) * MAX(v.duration), 0) AS avg_finish,\n" +
                    "           vws.total_views as total_views\n" +
                    "    FROM videos v\n" +
                    "    LEFT JOIN View vw ON v.bv = vw.bv\n" +
                    "    LEFT JOIN (\n" +
                    "        SELECT bv, COUNT(distinct mid) as total_views\n" +
                    "        FROM View\n" +
                    "        GROUP BY bv\n" +
                    "    ) vws ON v.bv = vws.bv\n" +
                    "    LEFT JOIN (\n" +
                    "        SELECT bv, COUNT(*) as like_count\n" +
                    "        FROM liker\n" +
                    "        GROUP BY bv\n" +
                    "    ) l ON v.bv = l.bv\n" +
                    "    LEFT JOIN (\n" +
                    "        SELECT bv, COUNT(*) as coin_count\n" +
                    "        FROM coin\n" +
                    "        GROUP BY bv\n" +
                    "    ) c ON v.bv = c.bv\n" +
                    "    LEFT JOIN (\n" +
                    "        SELECT bv, COUNT(*) as fav_count\n" +
                    "        FROM favorite\n" +
                    "        GROUP BY bv\n" +
                    "    ) f ON v.bv = f.bv\n" +
                    "    LEFT JOIN (\n" +
                    "        SELECT bv, COUNT(*) as danmu_count\n" +
                    "        FROM Danmu\n" +
                    "        GROUP BY bv\n" +
                    "    ) d ON v.bv = d.bv\n" +
                    "    GROUP BY v.bv, vws.total_views, l.like_count, c.coin_count, f.fav_count, d.danmu_count\n" +
                    ") sub\n" +
                    "GROUP BY sub.bv, sub.like_rate, sub.coin_rate, sub.fav_rate, sub.avg_finish, sub.total_views\n" +
                    "ORDER BY score DESC\n" +
                    "LIMIT ? OFFSET ?;";
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setInt(1, pageSize);
            ps.setInt(2, (pageNum - 1) * pageSize);
            re = ps.executeQuery();
            ArrayList<String> s_list = new ArrayList<String>();
            boolean flag = false;
            while (re.next()) {
                flag = true;
                String bv = re.getString("bv");
                //System.out.println("BV: " + bv);
                s_list.add(bv);
            }
            if (!flag) {
                return new ArrayList<String>();
            }
            return s_list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            ConnectionPool.releaseConnection(con);
        }
    }

    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        /**
         * Recommends videos for a user, restricted on their interests.
         * The user's interests are defined as the videos that the user's friend(s) have watched,
         * filter out the videos that the user has already watched.
         * Friend(s) of current user is/are the one(s) who is/are both the current user' follower and followee at the same time.
         * Sort the videos by:
         * <ol>
         *   <li>The number of friends who have watched the video</li>
         *   <li>The video owner's level</li>
         *   <li>The video's public time (newer videos are preferred)</li>
         * </ol>
         *
         * @param auth     the current user's authentication information to be recommended
         * @param pageSize the page size, if there are less than {@code pageSize} videos, return all of them
         * @param pageNum  the page number, starts from 1
         * @return a list of video {@code bv}s
         * @implNote
         * If the current user's interest is empty, return {@link io.sustc.service.RecommenderService#generalRecommendations(int, int)}.
         * If the requested page is empty, return an empty list
         * @apiNote You may consider the following corner cases:
         * <ul>
         *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
         *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
         * </ul>
         * If any of the corner case happened, {@code null} shall be returned.
         */
        Connection con = null;
        Logger logger = new Logger();
        try {
            //logger.function("recommendVideosForUser");
            con = ConnectionPool.getConnection();
            if (pageNum <= 0 || pageSize <= 0) {
                return null;
            }
            if (checkUser(auth) == false) {
                return null;
            }
            long MID = getMid(auth);
            ResultSet re;
            String sql = "SELECT COUNT(DISTINCT vw.bv) AS videos_watched_by_friends\n" +
                    "FROM follows f1\n" +
                    "JOIN follows f2 ON f1.follower = f2.followee AND f1.followee = f2.follower\n" +
                    "JOIN View vw ON f1.followee = vw.mid\n" +
                    "WHERE f1.follower = ? -- 当前用户的mid\n";
            PreparedStatement _ps = con.prepareStatement(sql);
            _ps.setLong(1, MID);
            re = _ps.executeQuery();
            int videos_watched_by_friends = 0;
            if (re.next()) {
                videos_watched_by_friends = re.getInt("videos_watched_by_friends");
            }
            if (videos_watched_by_friends == 0) {
                return generalRecommendations(pageSize, pageNum);
            }

            String _sql = "SELECT v.bv, v.title, COUNT(distinct f1.follower) AS friend_count, v.mid, u.level, v.public_time\n" +
                    "FROM follows f1\n" +
                    "JOIN follows f2 ON f1.follower = f2.followee AND f1.followee = f2.follower\n" +
                    "JOIN View vw ON f1.follower = vw.mid\n" +
                    "JOIN videos v ON vw.bv = v.bv\n" +
                    "JOIN t_user u ON v.mid = u.mid\n" +
                    "WHERE f1.followee = ?\n" +
                    "  AND NOT EXISTS (\n" +
                    "      SELECT 1 FROM View\n" +
                    "      WHERE mid = ? AND bv = v.bv\n" +
                    "  )\n" +
                    "GROUP BY v.bv, v.title, v.mid, u.level, v.public_time\n" +
                    "ORDER BY friend_count DESC, u.level DESC, v.public_time DESC\n" +
                    "LIMIT ? OFFSET ?;";
            PreparedStatement ps = con.prepareStatement(_sql);
            ps.setLong(1, MID);
            ps.setLong(2, MID);
            ps.setInt(3, pageSize);
            ps.setInt(4, (pageNum - 1) * pageSize);
            re = ps.executeQuery();
            ArrayList<String> s_list = new ArrayList<String>();
            boolean flag = false;
            while (re.next()) {
                flag = true;
                String bv = re.getString("bv");
                //System.out.println("BV: " + bv);
                s_list.add(bv);
            }
            if (!flag) {
                return new ArrayList<String>();
            }
            return s_list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            ConnectionPool.releaseConnection(con);
        }
    }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        /**
         * Recommends friends for a user, based on their common followings.
         * Find all users that are not currently followed by the user, and have at least one common following with the user.
         * Sort the users by the number of common followings, if two users have the same number of common followings,
         * sort them by their {@code level}.
         *
         * @param auth     the current user's authentication information to be recommended
         * @param pageSize the page size, if there are less than {@code pageSize} users, return all of them
         * @param pageNum  the page number, starts from 1
         * @return a list of {@code mid}s of the recommended users
         * @implNote If the requested page is empty, return an empty list
         * @apiNote You may consider the following corner cases:
         * <ul>
         *   <li>{@code auth} is invalid, as stated in {@link io.sustc.service.UserService#deleteAccount(AuthInfo, long)}</li>
         *   <li>{@code pageSize} and/or {@code pageNum} is invalid (any of them <= 0)</li>
         * </ul>
         * If any of the corner case happened, {@code null} shall be returned.
         */
        Connection con = null;
        Logger logger = new Logger();
        try {
            con = ConnectionPool.getConnection();
            //logger.function("recommendFriends");
            ResultSet re;
            if (pageNum <= 0 || pageSize <= 0) {
                return null;
            }
            if (checkUser(auth) == false) {
                return null;
            }
            String sql = "SELECT u.mid, COUNT(f2.followee) AS common_followings, u.level\n" +
                    "FROM follows f1\n" +
                    "JOIN follows f2 ON f1.followee = f2.followee AND f1.follower != f2.follower\n" +
                    "JOIN t_user u ON f2.follower = u.mid\n" +
                    "WHERE f1.follower = ? -- 当前用户的mid\n" +
                    "  AND f2.follower NOT IN (\n" +
                    "    SELECT followee FROM follows WHERE follower = ?\n" +
                    "  ) -- 排除已关注的用户\n" +
                    "GROUP BY u.mid, u.level\n" +
                    "ORDER BY common_followings DESC, u.level\n" +
                    "LIMIT ? OFFSET ?;";
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setLong(1, auth.getMid());
            ps.setLong(2, auth.getMid());
            ps.setInt(3, pageSize);
            ps.setInt(4, (pageNum - 1) * pageSize);
            re = ps.executeQuery();
            ArrayList<Long> s_list = new ArrayList<Long>();
            boolean flag = false;
            while (re.next()) {
                flag = true;
                long mid = re.getLong("mid");
                //System.out.println("mid: " + mid);
                s_list.add(mid);
            }
            if (!flag) {
                return new ArrayList<Long>();
            }
            return s_list;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            ConnectionPool.releaseConnection(con);
        }
    }

    boolean checkUser(AuthInfo auth) {
        Connection con = null;
        String password = new String();
        String qq;
        String wechat;
        String identity;
        boolean flag = false;
        try {
            con = ConnectionPool.getConnection();
            //logger.function("checkUser");
            ResultSet re;
            String sql_check_mid = "SELECT * FROM t_user WHERE mid = ?";
            PreparedStatement ps = con.prepareStatement(sql_check_mid);
            ps.setLong(1, auth.getMid());
            re = ps.executeQuery();
            if (re.next()) {
                password = re.getString("password");
                qq = re.getString("qq");
                wechat = re.getString("wechat");
                identity = re.getString("identity");
                flag = true;
            }
            if (auth.getMid() != 0) {
                if (flag == false) {
                    return false;
                }
                AESCipher aesCipher = new AESCipher();
                String P = aesCipher.decrypt(password);
                if (!auth.getPassword().equals(P)) {
                    return false;
                }
            }
            if (auth.getMid() == 0 && auth.getQq() == null && auth.getWechat() == null) {
                return false;
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            ConnectionPool.releaseConnection(con);
        }
    }

    long getMid(AuthInfo auth) {
        if (auth.getMid() != 0) {
            return auth.getMid();
        }
        Connection con = null;
        try {
            con = ConnectionPool.getConnection();
            if (auth.getQq() != null) {
                ResultSet re;
                String sql_check_qq = "SELECT * FROM t_user WHERE qq = ?";
                PreparedStatement ps = con.prepareStatement(sql_check_qq);
                ps.setString(1, auth.getQq());
                re = ps.executeQuery();
                if (re.next()) {
                    return re.getLong("mid");
                }
            }
            if (auth.getWechat() != null) {
                ResultSet re;
                String sql_check_wechat = "SELECT * FROM t_user WHERE wechat = ?";
                PreparedStatement ps = con.prepareStatement(sql_check_wechat);
                ps.setString(1, auth.getWechat());
                re = ps.executeQuery();
                if (re.next()) {
                    return re.getLong("mid");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            ConnectionPool.releaseConnection(con);
        }
        return 0;
    }
}