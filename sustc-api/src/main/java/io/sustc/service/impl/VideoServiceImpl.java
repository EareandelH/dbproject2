package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.security.SecureRandom;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

@Service
@Slf4j
public class VideoServiceImpl implements io.sustc.service.VideoService {
    @Autowired
    private DataSource dataSource;
    Logger logger =new Logger();
    public String postVideo(AuthInfo auth, PostVideoReq req){
        Connection con=null;
        try{
            con = ConnectionPool.getConnection();
            ResultSet re;
            String sql = "insert into videos (bv,title,mid,name,commit_time," +
                    "review_time,public_time,duration,description,reviewer) " +
                    "values(?,?,?,?,?,?,?,?,?,?)";

            PreparedStatement statement = con.prepareStatement(sql);
            String title= req.getTitle();
            if(title.equals("")){
                System.out.println("Title cannot be null.");
                return null;
            }
            long mid=auth.getMid();
            VideoRecord video = selcetVideo_Title_Mid(title,mid);
            UserServiceImpl userService=new UserServiceImpl();
            if(!userService.checkUser(auth)){
                System.out.println("The auth is invalid");
                return null;
            }
            UserRecord userRecord=userService.selectUser_mid(mid);
            if(video!=null){
                System.out.println("The video has been posted");
                return null;
            }
            String bv=BVbuilder();
            String name=userRecord.getName();
            Timestamp public_time=req.getPublicTime();
            Timestamp now=Timestamp.valueOf(LocalDateTime.now());
            if(public_time.compareTo(now)<0){
                System.out.println("You cannot post a video before the current time.");
                return null;
            }
            float duration = req.getDuration();
            if(duration<=10){
                System.out.println("You cannot post a video less than 10 seconds.");
                return null;
            }
            String description= req.getDescription();
            statement.setString(1,bv);
            statement.setString(2,title);
            statement.setLong(3,mid);
            statement.setString(4,name);
            statement.setTimestamp(5,now);
            statement.setTimestamp(6,null);
            statement.setTimestamp(7,public_time);
            statement.setFloat(8,duration);
            statement.setString(9,description);
            statement.setLong(10,-1);
            int affected =statement.executeUpdate();
            if(affected>0){
                System.out.println(name+" ,you have published "+title+" BV("+bv+") successfully!");
                return bv;
            }
            else {
                System.out.println("Post failed");
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return null;
    }
    public boolean deleteVideo(AuthInfo auth, String bv){
        Connection con = null;
        try {
            con = ConnectionPool.getConnection();
            ResultSet re;
            String sql="delete from videos where bv=? and mid=?";
            PreparedStatement statement=con.prepareStatement(sql);
            UserServiceImpl userService=new UserServiceImpl();
            long mid=auth.getMid();
            if(!userService.checkUser(auth)){
                System.out.println("The auth is invalid");
                return false;
            }
            VideoRecord videoRecord = select_BV(bv);
            if(videoRecord==null){
                System.out.println("Cannot find a video corresponding to the "+bv+".");
                return false;
            }
            UserRecord userRecord=userService.selectUser_mid(mid);
            if(videoRecord.getOwnerMid()!=mid&&userRecord.getIdentity()!= UserRecord.Identity.SUPERUSER){
                System.out.println("You are not the owner of the video nor a superuser.");
                return false;
            }
            statement.setString(1,bv);
            statement.setLong(2,mid);
            int affected =statement.executeUpdate();
            log.info("SQL:{}",statement);
            if(affected>0){
                System.out.println("Video"+bv+"has been deleted!");
                return true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return false;
    }
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req){
        Connection con=null;
        try{
            con = ConnectionPool.getConnection();
            ResultSet re;
            UserServiceImpl userService=new UserServiceImpl();
            long mid=auth.getMid();
            String new_title=req.getTitle();
            if(new_title.equals("")){
                System.out.println("Title cannot be null.");
                return false;
            }
            if(!userService.checkUser(auth)){
                System.out.println("The auth is invalid");
                return false;
            }
            VideoRecord videoRecord = select_BV(bv);
            if(videoRecord==null){
                System.out.println("Cannot find a video corresponding to the "+bv+".");
                return false;
            }
            UserRecord userRecord=userService.selectUser_mid(mid);
            if(videoRecord.getOwnerMid()!=mid&&userRecord.getIdentity()!= UserRecord.Identity.SUPERUSER){
                System.out.println("You are not the owner of the video nor a superuser.");
                return false;
            }
            String new_description=req.getDescription();
            float new_duration= req.getDuration();
            String old_title=videoRecord.getTitle();
            String old_description=videoRecord.getDescription();
            float old_duration=videoRecord.getDuration();
            long ownerMid= videoRecord.getOwnerMid();
            if(ownerMid!= auth.getMid()){
                System.out.println("You don't have the authority " +
                        "to update the video's information");
                return false;
            }
            if(old_duration!=new_duration){
                System.out.println("You changed the duration.");
                return false;
            }
            if(old_title.equals(new_title)&&old_duration==new_duration&&old_description.equals(new_description)){
                System.out.println("You changed nothing.");
                return false;
            }
//            String sql="update video set title = \'"+new_title+"\' , description = \'"+
//                    new_description+"\' +where bv =\'"+bv+"\'";
            String sql ="update video set title =? ,description=?,review_time=?,reviewer=?,where bv =?";
            PreparedStatement statement=con.prepareStatement(sql);
            statement.setString(1,new_title);
            statement.setString(2,new_description);
            statement.setString(3,bv);
            statement.setTimestamp(4,null);
            statement.setLong(5,-1);
            int affected = statement.executeUpdate();
            if(affected>0){
                System.out.println("Update "+bv+" successfully!");
                return true;
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return false;
    }
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum){
        Connection con = null;
        try {
            con =ConnectionPool.getConnection();
            UserServiceImpl userService=new UserServiceImpl();
            List<String> videos=new ArrayList<>();
            Map<String, Integer> relevanceMap = new HashMap<>();
            if(!userService.checkUser(auth)){
                System.out.println("The auth is invalid.");
                return null;
            }
            if(keywords==null||keywords.equals("")){
                System.out.println("You should input keywords to search videos.");
                return  null;
            }
            if(pageSize<=0){
                System.out.println("PageSize is invalid.");
                return null;
            }
            if(pageNum<=0){
                System.out.println("PageNum is invalid.");
                return null;
            }
            String sql_1="select bv, title, description, name " +
                    "from videos where lower(title) like ? or lower(description) like ? or lower(name) like ?";
            String[] keywords_list=keywords.toLowerCase().split(" ");
            for(String keyword:keywords_list){
                PreparedStatement statement=con.prepareStatement(sql_1);
                statement.setString(1,"%"+keyword+"%");
                statement.setString(2,"%"+keyword+"%");
                statement.setString(3,"%"+keyword+"%");
                ResultSet re = statement.executeQuery();
                while(re.next()){
                    String bv = re.getString("bv");
                    int relevance = relevanceMap.getOrDefault(bv, 0);
                    relevance += countOccurrences(re.getString("title"), keyword);
                    relevance += countOccurrences(re.getString("description"), keyword);
                    relevance += countOccurrences(re.getString("name"), keyword);
                    relevanceMap.put(bv, relevance);
                }
            }
            List<String> row =new ArrayList<>(relevanceMap.keySet());
            Comparator<String> comparator =new Comparator<String>() {
                @Override
                public int compare(String bv1, String bv2) {
                    int relevance_compare=Integer.compare(relevanceMap.get(bv2),relevanceMap.get(bv1));
                    if(relevance_compare==0){
                        return Integer.compare(video_view(bv2).size(),video_view(bv1).size());
                    }
                    return 0;
                }
            };
            Collections.sort(row,comparator);
            int start_index=(pageNum-1)*pageSize;
            int end_index=Math.min(start_index+pageSize,row.size());
            for(int i=start_index;i<=end_index;i++){
                videos.add(row.get(i));
            }
            return videos;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }

        return null;
    }

    public double getAverageViewRate(String bv){
        logger.function("getAverageViewRate "+bv);
        try{
            VideoRecord videoRecord=select_BV(bv);
            if(videoRecord==null){
                System.out.println("Cannot find a video corresponding to the "+bv+".");
                return -1;
            }
            ArrayList<Long> views = video_view(bv);
            if(views.isEmpty()||views==null){
                System.out.println("No one has watched this video .");
                return -1;
            }
            double sum=0;
            for(long view :views){
                sum+=view;
            }
            double avg=sum/(views.size()*videoRecord.getDuration());
            return avg;
        }catch (Exception e){
            e.printStackTrace();
        }
        return -1;
    }

    public Set<Integer> getHotspot(String bv){
        logger.function("getHotspot");
        Set<Integer> hotspotChunks = new HashSet<>();
        Connection con=null;
        try {
            con =ConnectionPool.getConnection();
            String sql="select floor(time / 10) as chunk, count(*) as danmu_count\n" +
                    "from Danmu\n" +
                    "where bv = ?  " +
                    "group by chunk\n" +
                    "having count(*) = (\n" +
                    "    select MAX(danmu_count)\n" +
                    "    from (\n" +
                    "        select floor(time / 10) as sub_chunk, count(*) as danmu_count\n" +
                    "        from Danmu\n" +
                    "        where bv = ?  " +
                    "        group by sub_chunk\n" +
                    "    ) as subquery\n" +
                    ");\n";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setString(1,bv);
            statement.setString(2,bv);
            ResultSet re=statement.executeQuery();
            while(re.next()){
                hotspotChunks.add(re.getInt("chunk"));
            }
            return hotspotChunks;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return hotspotChunks;
    }

    public boolean reviewVideo(AuthInfo auth, String bv){
        Connection con = null ;
        try{
            con =ConnectionPool.getConnection();
            ResultSet re;
            long mid= auth.getMid();
            UserServiceImpl userService=new UserServiceImpl();
            if(!userService.checkUser(auth)){
                System.out.println("The auth is invalid");
                return false;
            }
            VideoRecord videoRecord = select_BV(bv);
            if(videoRecord==null){
                System.out.println("Cannot find a video corresponding to the "+bv+".");
                return false;
            }
            UserRecord userRecord=userService.selectUser_mid(mid);
            if(userRecord.getIdentity()!= UserRecord.Identity.SUPERUSER){
                System.out.println("You are not a superuser.");
                return false;
            }
            if(userRecord.getMid()==videoRecord.getOwnerMid()){
                System.out.println("You cannot review your own video.");
                return false;
            }
            if(videoRecord.getReviewTime()!=null){
                System.out.println("The video has been reviewed by "+videoRecord.getReviewer()
                        +" on "+videoRecord.getReviewTime()+".");
                return false;
            }
            Timestamp now = Timestamp.valueOf(LocalDateTime.now());
            String sql ="update video set reviewer="+auth.getMid()+",review_time=\'"+now.toString()+
                    "\' where bv=\'"+bv+"\'";
            PreparedStatement statement=con.prepareStatement(sql);
            int affected=statement.executeUpdate();
            if(affected>0){
                System.out.println("You reviewed the video.");
                return true;
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return false;
    }


    public boolean coinVideo(AuthInfo auth, String bv){
        Connection con=null;
        try{
            con =ConnectionPool.getConnection();
            long mid =auth.getMid();
            UserServiceImpl userService=new UserServiceImpl();
            if(!userService.checkUser(auth)){
                System.out.println("The auth is invalid");
                return false;
            }
            VideoRecord videoRecord = select_BV(bv);
            if(videoRecord==null){
                System.out.println("Cannot find a video corresponding to the "+bv+".");
                return false;
            }
            UserRecord userRecord=userService.selectUser_mid(mid);
            if(userRecord.getMid()==videoRecord.getOwnerMid()){
                System.out.println("You cannot coin your own video.");
                return false;
            }
            Timestamp now = Timestamp.valueOf(LocalDateTime.now());
            if(videoRecord.getReviewTime()==null|| now.before(videoRecord.getPublicTime())){
                System.out.println("The video is invalid now .");
                return false;
            }
            if(userRecord.getCoin()==0){
                System.out.println("You have no coin now.");
                return false;
            }
            if(userService.user_coined(mid).contains(bv)){
                System.out.println("You have coined the video");
                return false;
            }
            String sql="insert into coin (bv,mid) values(?,?)";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setString(1,bv);
            statement.setLong(2,mid);
            int affected = statement.executeUpdate();
            if(affected>0){
                System.out.println("Successfully deposited coins!");
                return true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return false;
    }


    public boolean likeVideo(AuthInfo auth, String bv){
        Connection con = null;
        try{
            con =ConnectionPool.getConnection();
            long mid =auth.getMid();
            UserServiceImpl userService=new UserServiceImpl();
            if(!userService.checkUser(auth)){
                System.out.println("The auth is invalid");
                return false;
            }
            VideoRecord videoRecord = select_BV(bv);
            if(videoRecord==null){
                System.out.println("Cannot find a video corresponding to the "+bv+".");
                return false;
            }
            UserRecord userRecord=userService.selectUser_mid(mid);
            if(userRecord.getMid()==videoRecord.getOwnerMid()){
                System.out.println("You cannot like your own video.");
                return false;
            }
            Timestamp now = Timestamp.valueOf(LocalDateTime.now());
            if(videoRecord.getReviewTime()==null|| now.before(videoRecord.getPublicTime())){
                System.out.println("The video is invalid now .");
                return false;
            }
            /*
            优化想法，这里包含一个查询，一个删除操作，可以直接使用删除操作看affected是否为0
            为0即是不存在这一行
             */
            if(userService.user_liked(mid).contains(bv)){
                System.out.println("You canceled the like.");
                cancel_like(mid,bv);
                return false;
            }
            String sql="insert into liker (bv,mid) values(?,?)";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setString(1,bv);
            statement.setLong(2,mid);
            int affected = statement.executeUpdate();
            if(affected>0){
                System.out.println("Successfully liked!");
                return true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return false;
    }


    public boolean collectVideo(AuthInfo auth, String bv){
        Connection con =null;
        try{
            con =ConnectionPool.getConnection();
            long mid =auth.getMid();
            UserServiceImpl userService=new UserServiceImpl();
            if(!userService.checkUser(auth)){
                System.out.println("The auth is invalid");
                return false;
            }
            VideoRecord videoRecord = select_BV(bv);
            if(videoRecord==null){
                System.out.println("Cannot find a video corresponding to the "+bv+".");
                return false;
            }
            UserRecord userRecord=userService.selectUser_mid(mid);
            if(userRecord.getMid()==videoRecord.getOwnerMid()){
                System.out.println("You cannot collect your own video.");
                return false;
            }
            Timestamp now = Timestamp.valueOf(LocalDateTime.now());
            if(videoRecord.getReviewTime()==null|| now.before(videoRecord.getPublicTime())){
                System.out.println("The video is invalid now .");
                return false;
            }
            if(userService.user_collected(mid).contains(bv)){
                System.out.println("You have collected the video");
                cancel_collect(mid,bv);
                return false;
            }
            String sql="insert into favorite (bv,mid) values(?,?)";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setString(1,bv);
            statement.setLong(2,mid);
            int affected = statement.executeUpdate();
            if(affected>0){
                System.out.println("Successfully collect the video!");
                return true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return false;
    }

    //--------------------------------------------------------------------------------------
    public boolean cancel_like(long mid,String bv){
        Connection con = null;
        try{
            con =ConnectionPool.getConnection();
            ResultSet re;
            String sql_like_cancel="delete from liker where bv=? and mid=?";
            PreparedStatement preparedStatement_like_cancel=con.prepareStatement(sql_like_cancel);
            preparedStatement_like_cancel.setString(1,bv);
            preparedStatement_like_cancel.setLong(2,mid);
            int affected =preparedStatement_like_cancel.executeUpdate();
            if(affected>0)
                System.out.println("Like cancel succeed!");
            else System.out.println("Cancel failed!");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return false;
    }
    public boolean cancel_collect(long mid,String bv){
        Connection con = null;
        try{
            con =ConnectionPool.getConnection();
            ResultSet re;
            String sql="delete from favorite where bv=? and mid=?";
            PreparedStatement statement=con.prepareStatement(sql);
            statement.setString(1,bv);
            statement.setLong(2,mid);
            int affected =statement.executeUpdate();
            if(affected>0)
                System.out.println("Collect cancel succeed!");
            else System.out.println("Cancel failed!");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return false;
    }
    public VideoRecord selcetVideo_Title_Mid(String title,long mid){
        Connection con = null;
        try{
            con = ConnectionPool.getConnection();
            ResultSet re;
            String sql = "select * from videos where title=? and mid=?";
            PreparedStatement statement= con.prepareStatement(sql);
            statement.setString(1,title);
            statement.setLong(2,mid);
            re=statement.executeQuery();
            log.info("SQL:{}",statement);
            if(re.next()){
                VideoRecord video= new VideoRecord(re.getString("bv"),re.getString("title"),
                        re.getLong("mid"),re.getString("name"),re.getTimestamp("commit_time"),
                        re.getTimestamp("review_time"),re.getTimestamp("public_time"),re.getFloat("duration"),
                        re.getString("description"),re.getLong("reviewer"));
                return video;
            }
            else return null;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return null;
    }
    public VideoRecord select_BV(String bv){
        Connection con = null;
        try{
            con=ConnectionPool.getConnection();
            ResultSet re=null;
            String sql_select_BV = "select * from videos where bv=?";
            PreparedStatement statement= con.prepareStatement(sql_select_BV);
            statement.setString(1, bv);
            re=statement.executeQuery();
            log.info("SQL:{}",statement);
            if(re.next()){
                VideoRecord video= new VideoRecord(re.getString("bv"),re.getString("title"),
                        re.getLong("mid"),re.getString("name"),re.getTimestamp("commit_time"),
                        re.getTimestamp("review_time"),re.getTimestamp("public_time"),re.getFloat("duration"),
                        re.getString("description"),re.getLong("reviewer"));
                return video;
            }
            else return null;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return null;
    }
    public ArrayList<Long> video_view(String bv){
        Connection con = null;
        try{
            con = ConnectionPool.getConnection();
            ResultSet re;
            ArrayList<Long> view_Array=new ArrayList<>();
            String sql="select view from View where bv=?";
            PreparedStatement statement = con.prepareStatement(sql);
            statement.setString(1,bv);
            re=statement.executeQuery();
            while(re.next()){
                long temp = re.getLong("view");
                view_Array.add(temp);
            }
            return view_Array;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            ConnectionPool.releaseConnection(con);
        }
        return null;
    }
    public int countOccurrences(String text,String keyword){
        if (text == null || keyword == null || text.isEmpty() || keyword.isEmpty()) {
            return 0;
        }
        int count = 0;
        int index = text.toLowerCase().indexOf(keyword);
        while (index != -1) {
            count++;
            index = text.toLowerCase().indexOf(keyword, index + keyword.length());
        }
        return count;
    }
    private String BVbuilder(){
        try {
            String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            StringBuilder randomString = new StringBuilder(10);
            SecureRandom random = new SecureRandom();
            for (int i = 0; i < 10; i++) {
                int randomIndex = random.nextInt(characters.length());
                char randomChar = characters.charAt(randomIndex);
                randomString.append(randomChar);
            }
            String generatedString = randomString.toString();
            String bv = "BV1" + generatedString;
            if (select_BV(bv)!=null)return BVbuilder();
            else return bv;
        }catch (Exception e){
            e.printStackTrace();
        }
        return "NULL";
    }
}
