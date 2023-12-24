package io.sustc.service.impl;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.Base64;

public class AESCipher {

    private static final String KEY_FILE = "secretKey.key";
    private SecretKey secretKey;

    public AESCipher() throws Exception {
        File keyFile = new File(KEY_FILE);
        if (keyFile.exists()) {
            // 加载现有密钥
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(KEY_FILE))) {
                this.secretKey = (SecretKey) ois.readObject();
            }
        } else {
            // 生成新密钥
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(128); // 128位密钥长度
            this.secretKey = keyGenerator.generateKey();
            // 将密钥保存到文件
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(KEY_FILE))) {
                oos.writeObject(this.secretKey);
            }
        }
    }
    public String encrypt(String strToEncrypt) {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey);
            return Base64.getEncoder().encodeToString(cipher.doFinal(strToEncrypt.getBytes("UTF-8")));
        } catch (Exception e) {
            System.out.println("Error while encrypting: " + e.toString());
        }
        return null;
    }



    public String decrypt(String strToDecrypt) {
        try {
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, secretKey);
            return new String(cipher.doFinal(Base64.getDecoder().decode(strToDecrypt)));
        } catch (Exception e) {
            System.out.println("Error while decrypting: " + e.toString());
        }
        return null;
    }
}
