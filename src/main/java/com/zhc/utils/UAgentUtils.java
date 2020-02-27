package com.zhc.utils;


import com.zhc.domain.UserAgentInfo;
import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import org.apache.commons.lang3.StringUtils;


import java.io.IOException;

/**
 * 自定义UserAgent工具类
 */
public class UAgentUtils {

    private static UASparser parser;

    static {
        try {
            parser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static UserAgentInfo getUserAgentInfot(String ua) {
        UserAgentInfo info = new UserAgentInfo();


        try {
            if (StringUtils.isNotEmpty(ua)) {
                cz.mallat.uasparser.UserAgentInfo parse = parser.parse(ua);
                if (null != parse) {
                    info.setBrowserName(null == parse.getUaFamily() ? "unknown" : parse.getUaFamily());
                    info.setBrowserVersion(null == parse.getBrowserVersionInfo() ? "unknown" : parse.getBrowserVersionInfo());
                    info.setOsName(null == parse.getOsFamily() ? "unknown" : parse.getOsFamily());
                    info.setOsVersion(null == parse.getOsName() ? "unknown" : parse.getOsName());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();

        }

        return info;
    }
}
