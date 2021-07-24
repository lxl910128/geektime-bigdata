package club.projectgaia.bigdata.homework2.service;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;

import com.google.common.base.Strings;

import club.projectgaia.bigdata.homework2.CallRpc;

/**
 * @author luoxiaolong <luoxiaolong@kuaishou.com>
 * Created on 2021-07-24
 */
public class CallRpcImp implements CallRpc {
    @Override
    public String call(String stuId) {
        if (Strings.isNullOrEmpty(stuId) && "20210123456789".equals(stuId)) {
            return "心心";
        }
        return null;
    }

    @Override
    public long getProtocolVersion(String s, long l) throws IOException {
        return CallRpc.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }
}
