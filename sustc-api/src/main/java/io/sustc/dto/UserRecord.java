package io.sustc.dto;

import java.io.Serializable;

import lombok.Data;

/**
 * The user record used for data import
 * @implNote You may implement your own {@link java.lang.Object#toString()} since the default one in {@link lombok.Data} prints all array values.
 */
@Data
public class UserRecord implements Serializable {

    /**
<<<<<<< HEAD
     * The user's ID
=======
     * The user's unique ID
>>>>>>> upstream/main
     */
    private long mid;

    /**
     * The user's name
     */
    private String name;

    /**
     * The user's sex
     */
    private String sex;

    /**
<<<<<<< HEAD
     * The user's birthday
=======
     * The user's birthday, can be empty
>>>>>>> upstream/main
     */
    private String birthday;

    /**
     * The user's level
     */
    private short level;

    /**
     * The user's current number of coins
     */
    private int coin;

    /**
     * The user's personal sign, can be null or empty
     */
    private String sign;

    /**
     * The user's identity
     */
    private Identity identity;

    /**
     * The user's password
     */
    private String password;

    /**
<<<<<<< HEAD
     * The user's qq, may be null or empty
=======
     * The user's unique qq, may be null or empty (not unique when null or empty)
>>>>>>> upstream/main
     */
    private String qq;

    /**
<<<<<<< HEAD
     * The user's wechat, may be null or empty
=======
     * The user's unique wechat, may be null or empty (not unique when null or empty)
>>>>>>> upstream/main
     */
    private String wechat;

    /**
     * The users' {@code mid}s who are followed by this user
     */
    private long[] following;

    public enum Identity {
        USER,
        SUPERUSER,
    }
    public UserRecord(long mid,String name,String sex,String birthday,short level,int coin,
                      String sign,Identity identity,String password,String qq,String wechat){
        this.mid=mid;
        this.name=name;
        this.sex=sex;
        this.birthday=birthday;
        this.level=level;
        this.coin=coin;
        this.sign=sign;
        this.identity=identity;
        this.password=password;
        this.qq=qq;
        this.wechat=wechat;
    }
}
