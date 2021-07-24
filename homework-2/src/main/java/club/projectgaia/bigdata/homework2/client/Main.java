package club.projectgaia.bigdata.homework2.client;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import club.projectgaia.bigdata.homework2.CallRpc;

/**
 * @author luoxiaolong <luoxiaolong@kuaishou.com>
 * Created on 2021-07-24
 */
public class Main {
    public static void main(String[] args) {
        try {
            CallRpc proxy =
                    RPC.getProxy(CallRpc.class, RPC.getProtocolVersion(CallRpc.class), new InetSocketAddress("127.0.0.1", 1234), new Configuration());
            String ret1 = proxy.call("20210000000000");
            System.out.println(ret1 == null);
            String ret2 = proxy.call("20210123456789");
            System.out.println(ret2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
