package com.egeio.realtime;

import com.google.gson.annotations.SerializedName;

/**
 * Created by think on 2015/8/3.
 */
public class UserNodeInfo {

    @SerializedName("user_id") private String UserID;

    @SerializedName("server_node") private String ServerNode;

    public UserNodeInfo(String userID, String serverNode) {
        this.ServerNode = serverNode;
        this.UserID = userID;
    }

    public String getUserID() {
        return this.UserID;
    }

    public String getServerNode() {
        return this.ServerNode;
    }

}
