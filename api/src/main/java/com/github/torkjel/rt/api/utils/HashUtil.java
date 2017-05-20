package com.github.torkjel.rt.api.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtil {

    private static final MessageDigest SHA256;

    static {
        try {
            SHA256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static String hash(long salt, String data) {
        return toHexString(SHA256.digest((salt + data).getBytes()));
    }

    private static String toHexString(byte[] digest) {
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(nibbleToHex((b & 0xf0) >> 4));
            sb.append(nibbleToHex(b & 0xf));
        }
        return sb.toString();
    }

    private static char nibbleToHex(int b) {
        return (char)(b >= 10 ? 'a' + b - 10 : '0' + b);
    }

}
