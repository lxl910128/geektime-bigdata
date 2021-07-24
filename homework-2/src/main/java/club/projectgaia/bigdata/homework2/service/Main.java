package club.projectgaia.bigdata.homework2.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;

import club.projectgaia.bigdata.homework2.CallRpc;

/**
 * @author luoxiaolong <luoxiaolong@kuaishou.com>
 * Created on 2021-07-24
 */
public class Main {
    public static void main(String[] args) {
        RPC.Builder builder = new Builder(new Configuration());

        builder.setBindAddress("127.0.0.1");
        builder.setPort(1234);
        builder.setProtocol(CallRpc.class);
        builder.setInstance(new CallRpcImp());

        try {
            RPC.Server server = builder.build();
            server.start();
            System.out.println("start success");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
