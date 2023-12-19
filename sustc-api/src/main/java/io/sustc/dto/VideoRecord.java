package io.sustc.dto;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * The video record used for data import
 * @implNote You may implement your own {@link java.lang.Object#toString()} since the default one in {@link lombok.Data} prints all array values.
 */
@Data
public class VideoRecord implements Serializable {

    /**
     * The BV code of this video
     */
    private String bv;

    /**
     * The title of this video with length >= 1, the video titles of an owner cannot be the same
     */
    private String title;

    /**
     * The owner's {@code mid} of this video
     */
    private long ownerMid;

    /**
     * The owner's {@code name} of this video
     */
    private String ownerName;

    /**
     * The commit time of this video
     */
    private Timestamp commitTime;

    /**
     * The review time of this video, can be null
     */
    private Timestamp reviewTime;

    /**
     * The public time of this video, can be null
     */
    private Timestamp publicTime;

    /**
     * The length in seconds of this video
     */
    private float duration;

    /**
     * The description of this video
     */
    private String description;

    /**
     * The reviewer of this video, can be null
     */
    private Long reviewer;

    /**
     * The users' {@code mid}s who liked this video
     */
    private long[] like;

    /**
     * The users' {@code mid}s who gave coin to this video
     */
    private long[] coin;

    /**
     * The users' {@code mid}s who collected to this video
     */
    private long[] favorite;

    /**
     * The users' {@code mid}s who have watched this video
     */
    private long[] viewerMids;

    /**
     * The watch durations in seconds for the viewers {@code viewerMids}
     */
    private float[] viewTime;
    public VideoRecord(String bv,String title,long ownerMid,String ownerName,
                       Timestamp commitTime,Timestamp reviewTime,Timestamp publicTime,
                       float duration,String description,Long reviewer,
                       long[] like,long[] coin,long[] favorite,long[] viewerMids){
        this.bv=bv;
        this.title=title;
        this.ownerMid=ownerMid;
        this.ownerName=ownerName;
        this.commitTime=commitTime;
        this.reviewTime=reviewTime;
        this.publicTime=publicTime;
        this.duration=duration;
        this.description=description;
        this.reviewer=reviewer;
        this.like=like;
        this.coin=coin;
        this.favorite=favorite;
        this.viewerMids=viewerMids;
    }
    public VideoRecord(String bv,String title,long ownerMid,String ownerName,
                       Timestamp commitTime,Timestamp reviewTime,Timestamp publicTime,
                       float duration,String description,Long reviewer){
        this.bv=bv;
        this.title=title;
        this.ownerMid=ownerMid;
        this.ownerName=ownerName;
        this.commitTime=commitTime;
        this.reviewTime=reviewTime;
        this.publicTime=publicTime;
        this.duration=duration;
        this.description=description;
        this.reviewer=reviewer;
    }

    public float getDuration() {
        return duration;
    }

    public long getOwnerMid() {
        return ownerMid;
    }

    public Long getReviewer() {
        return reviewer;
    }

    public float[] getViewTime() {
        return viewTime;
    }

    public long[] getCoin() {
        return coin;
    }

    public long[] getFavorite() {
        return favorite;
    }

    public long[] getLike() {
        return like;
    }

    public String getBv() {
        return bv;
    }

    public long[] getViewerMids() {
        return viewerMids;
    }

    public String getDescription() {
        return description;
    }

    public String getOwnerName() {
        return ownerName;
    }

    public String getTitle() {
        return title;
    }

    public Timestamp getCommitTime() {
        return commitTime;
    }

    public Timestamp getPublicTime() {
        return publicTime;
    }

    public Timestamp getReviewTime() {
        return reviewTime;
    }

    public void setBv(String bv) {
        this.bv = bv;
    }

    public void setCoin(long[] coin) {
        this.coin = coin;
    }

    public void setCommitTime(Timestamp commitTime) {
        this.commitTime = commitTime;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setDuration(float duration) {
        this.duration = duration;
    }

    public void setFavorite(long[] favorite) {
        this.favorite = favorite;
    }

    public void setLike(long[] like) {
        this.like = like;
    }

    public void setOwnerMid(long ownerMid) {
        this.ownerMid = ownerMid;
    }

    public void setOwnerName(String ownerName) {
        this.ownerName = ownerName;
    }

    public void setPublicTime(Timestamp publicTime) {
        this.publicTime = publicTime;
    }

    public void setReviewer(Long reviewer) {
        this.reviewer = reviewer;
    }

    public void setReviewTime(Timestamp reviewTime) {
        this.reviewTime = reviewTime;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setViewerMids(long[] viewerMids) {
        this.viewerMids = viewerMids;
    }

    public void setViewTime(float[] viewTime) {
        this.viewTime = viewTime;
    }
}
