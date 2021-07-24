package club.projectgaia.bigdata.homework2;

import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * @author luoxiaolong <luoxiaolong@kuaishou.com>
 * Created on 2021-07-24
 */
public interface CallRpc extends VersionedProtocol {
    public static final long versionID = 123L;

    String call(String stuId);
}
