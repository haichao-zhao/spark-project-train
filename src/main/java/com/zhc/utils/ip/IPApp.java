package com.zhc.utils.ip;

import java.util.Map;

public class IPApp {
    public static void main(String[] args) {
        String ip = "49.233.85.250";

        IPUtils ipUtils = new IPUtils();

        Map<String, String> parse = ipUtils.parse(ip);
        for (Map.Entry<String, String> entry : parse.entrySet()) {

            System.out.println(entry.getKey() + " : " + entry.getValue());

        }

    }
}
