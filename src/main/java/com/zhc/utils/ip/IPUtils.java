package com.zhc.utils.ip;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class IPUtils {

    private Logger logger = LoggerFactory.getLogger(IPUtils.class);


    public Map<String, String> parse(String log) {

        Map<String, String> logInfo = new HashMap<>();
        IPParser ipParse = IPParser.getInstance();
        if (StringUtils.isNotBlank(log)) {
            String[] splits = log.split(" ");

            String ip = splits[0];

            logInfo.put("ip", ip);

            IPParser.RegionInfo regionInfo = ipParse.analyseIp(ip);

            logInfo.put("country", regionInfo.getCountry());
            logInfo.put("province", regionInfo.getProvince());
            logInfo.put("city", regionInfo.getCity());

        } else {
            logger.error("日志记录的格式不正确：" + log);
        }

        return logInfo;
    }

}
