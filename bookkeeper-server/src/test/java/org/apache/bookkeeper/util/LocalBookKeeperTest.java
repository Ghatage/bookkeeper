package org.apache.bookkeeper.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.test.ZooKeeperCluster;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.bookkeeper.test.ZooKeeperClusterUtil;
import org.apache.zookeeper.*;

import org.junit.Before;
import org.junit.Test;

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;

/**
 * Unit tests for {@link LocalBookKeeper}.
 */
public class LocalBookKeeperTest {

    protected ZooKeeperCluster zkUtil;
    protected ZooKeeper zkc;
    protected static int numOfZKNodes = 3;

    @Before
    public void setup() {
        if (numOfZKNodes == 1) {
            zkUtil = new ZooKeeperUtil();
        } else {
            try {
                zkUtil = new ZooKeeperClusterUtil(numOfZKNodes);
            } catch (IOException | KeeperException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            startZKCluster();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void startZKCluster() throws Exception {
        zkUtil.startCluster();
        zkc = zkUtil.getZooKeeperClient();
    }

    protected File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        return dir;
    }

    @Test
    public void testInitializeZookeeper() throws Exception {
        File tmpDir = createTempDir("localbookie", "test");
        final String zkRoot = "/messaging/bookkeeper/ledgers";

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() })
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri(zkRoot))
                .setZkTimeout(5000);

        Path zkLedgersConfigPath = Paths.get(zkRoot);
        List<Op> multiOps = Lists.newArrayListWithExpectedSize(zkLedgersConfigPath.getNameCount() + 2);
        for(int i = 1; i <= zkLedgersConfigPath.getNameCount(); i++) {
            multiOps.add(
                    Op.create("/" + zkLedgersConfigPath.subpath(0,i).toString(),
                            new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        }

        multiOps.add(
                Op.create(zkRoot + "/" + AVAILABLE_NODE,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
        multiOps.add(
                Op.create(zkRoot + "/" + AVAILABLE_NODE + "/" + READONLY,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));

        zkc.multi(multiOps);
        return;
    }
}
