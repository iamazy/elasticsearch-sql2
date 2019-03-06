package com.iamazy.springcloud.elasticsearch.dsl.utils;

import java.security.MessageDigest;


public class Md5Utils {

    /**
     * MD5加密 生成32位md5码
     *
     * @param inStr 输入字符串
     * @return 返回32位md5码
     */
    public static String md5Encode(String inStr) throws Exception {
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            System.out.println(e.toString());
            e.printStackTrace();
            return "";
        }

        byte[] byteArray = inStr.getBytes("UTF-8");
        byte[] md5Bytes = md5.digest(byteArray);
        StringBuffer hexValue = new StringBuffer();
        for (int i = 0; i < md5Bytes.length; i++) {
            int val = ((int) md5Bytes[i]) & 0xff;
            if (val < 16) {
                hexValue.append("0");
            }
            hexValue.append(Integer.toHexString(val));
        }
        return hexValue.toString();
    }

    public static String spliceStr(String instr) throws Exception {
        String str = new String(instr);
        String str1 = md5Encode(str);
        String str2 = md5Encode(str).substring(0, 10);
        String str3 = str1.substring(str1.length() - 3, str1.length());
        String splice = str2 + str3;
        return splice;
    }

    /**
     * 测试主函数
     *
     * @param args 传递参数
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
//        String str = new String("3547820611179391453880040001wsdc^&");
        String str = new String("http://14.190.152.211/onvifsnapshot/media_service/snapshot?channel=1000&amp;subtype=0");
        System.out.println("[****] md5Convert" + str);
        String str1 = md5Encode(str);
        String str2 = md5Encode(str).substring(0, 10);
        String str3 = str1.substring(str1.length() - 3, str1.length());
        String splice = str2 + str3;
        System.out.println(splice);

        System.out.println("字符串长度:" + str1.length() + "截取后:" + str2);
        System.out.println(str);
        System.out.println("原始：" + str);
        System.out.println("MD5后：" + md5Encode(str));
    }
}