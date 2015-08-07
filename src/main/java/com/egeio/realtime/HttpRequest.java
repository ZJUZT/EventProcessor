package com.egeio.realtime;

import com.egeio.core.log.Logger;
import com.egeio.core.log.LoggerFactory;
import com.egeio.core.log.MyUUID;

import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * This class is used to send http request (the actual payload is user_id for now)
 * Created by think on 2015/8/2.
 */
public class HttpRequest {

    private static Logger logger = LoggerFactory.getLogger(HttpRequest.class);
    private static MyUUID uuid = new MyUUID();

    public static void sendPost(String url, String param) {
        HttpURLConnection conn = null;
        PrintWriter out = null;
        try {
            URL realUrl = new URL(url);
            conn = (HttpURLConnection) realUrl.openConnection();
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            out = new PrintWriter(conn.getOutputStream());
            //send u_id
            out.print(param);
            out.flush();
            conn.getInputStream();
        }
        catch (Exception e) {
            logger.info(uuid, "Send post request failed");
        }
        finally {
            if (conn != null) {
                conn.disconnect();
            }
            if (out != null) {
                out.close();
            }
        }
    }

}
