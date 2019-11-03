/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.meta.MetadataDrivers.runFunctionWithRegistrationManager;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import org.apache.bookkeeper.client.ClientUtil;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.versioning.Version;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the protocol upgrade procedure.
 */
public class UpgradeTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(FileInfo.class);

    private static final int bookiePort = PortManager.nextFreePort();

    public UpgradeTest() {
        super(0);
    }

    static void writeLedgerDir(File dir,
                               byte[] masterKey)
            throws Exception {
        long ledgerId = 1;

        File fn = new File(dir, IndexPersistenceMgr.getLedgerName(ledgerId));
        fn.getParentFile().mkdirs();
        FileInfo fi = new FileInfo(fn, masterKey, FileInfo.CURRENT_HEADER_VERSION);
        // force creation of index file
        fi.write(new ByteBuffer[]{ ByteBuffer.allocate(0) }, 0);
        fi.close(true);

        long logId = 0;
        ByteBuffer logfileHeader = ByteBuffer.allocate(1024);
        logfileHeader.put("BKLO".getBytes());
        FileChannel logfile = new RandomAccessFile(
                new File(dir, Long.toHexString(logId) + ".log"), "rw").getChannel();
        logfile.write((ByteBuffer) logfileHeader.clear());
        logfile.close();
    }

    static JournalChannel writeJournal(File journalDir, int numEntries, byte[] masterKey)
            throws Exception {
        long logId = System.currentTimeMillis();
        JournalChannel jc = new JournalChannel(journalDir, logId);

        BufferedChannel bc = jc.getBufferedChannel();

        long ledgerId = 1;
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 'X');
        long lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;

        for (int i = 1; i <= numEntries; i++) {
            ByteBuf packet = ClientUtil.generatePacket(ledgerId, i, lastConfirmed,
                                                          i * data.length, data);
            lastConfirmed = i;
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            lenBuff.putInt(packet.readableBytes());
            lenBuff.flip();

            bc.write(Unpooled.wrappedBuffer(lenBuff));
            bc.write(packet);
            packet.release();
        }
        bc.flushAndForceWrite(false);

        return jc;
    }

    static File newV1JournalDirectory() throws Exception {
        File d = IOUtils.createTempDir("bookie", "tmpdir");
        writeJournal(d, 100, "foobar".getBytes()).close();
        return d;
    }

    static File newV1LedgerDirectory() throws Exception {
        File d = IOUtils.createTempDir("bookie", "tmpdir");
        writeLedgerDir(d, "foobar".getBytes());
        return d;
    }

    static void createVersion2File(File dir) throws Exception {
        File versionFile = new File(dir, "VERSION");

        FileOutputStream fos = new FileOutputStream(versionFile);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(fos));
            bw.write(String.valueOf(2));
        } finally {
            if (bw != null) {
                bw.close();
            }
            fos.close();
        }
    }

    static File newV2JournalDirectory() throws Exception {
        File d = newV1JournalDirectory();
        createVersion2File(d);
        return d;
    }

    static File newV2LedgerDirectory() throws Exception {
        File d = newV1LedgerDirectory();
        createVersion2File(d);
        return d;
    }

    private String newDirectory() throws IOException {
        return newDirectory(true);
    }

    private String newDirectory(boolean createCurDir) throws IOException {
        File d = IOUtils.createTempDir("cookie", "tmpdir");
        if (createCurDir) {
            new File(d, "current").mkdirs();
        }
        tmpDirs.add(d);
        return d.getPath();
    }

    private static void testUpgradeProceedure(String zkServers, String journalDir, String ledgerDir) throws Exception {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setMetadataServiceUri("zk://" + zkServers + "/ledgers");
        conf.setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir })
            .setBookiePort(bookiePort);
        Bookie b = null;
        try {
            b = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException e) {
            // correct behaviour
            assertTrue("wrong exception", e.getMessage().contains("upgrade needed"));
        }

        FileSystemUpgrade.upgrade(conf); // should work fine
        b = new Bookie(conf);
        b.start();
        b.shutdown();
        b = null;

        FileSystemUpgrade.rollback(conf);
        try {
            b = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException e) {
            // correct behaviour
            assertTrue("wrong exception", e.getMessage().contains("upgrade needed"));
        }

        FileSystemUpgrade.upgrade(conf);
        FileSystemUpgrade.finalizeUpgrade(conf);
        b = new Bookie(conf);
        b.start();
        b.shutdown();
        b = null;
    }

    @Test
    public void testUpgradeV1toCurrent() throws Exception {
        File journalDir = newV1JournalDirectory();
        tmpDirs.add(journalDir);
        File ledgerDir = newV1LedgerDirectory();
        tmpDirs.add(ledgerDir);
        testUpgradeProceedure(zkUtil.getZooKeeperConnectString(), journalDir.getPath(), ledgerDir.getPath());
    }

    @Test
    public void testUpgradeV2toCurrent() throws Exception {
        File journalDir = newV2JournalDirectory();
        tmpDirs.add(journalDir);
        File ledgerDir = newV2LedgerDirectory();
        tmpDirs.add(ledgerDir);
        testUpgradeProceedure(zkUtil.getZooKeeperConnectString(), journalDir.getPath(), ledgerDir.getPath());
    }

    @Test
    public void testUpgradeCurrent() throws Exception {
        File journalDir = newV2JournalDirectory();
        tmpDirs.add(journalDir);
        File ledgerDir = newV2LedgerDirectory();
        tmpDirs.add(ledgerDir);
        testUpgradeProceedure(zkUtil.getZooKeeperConnectString(), journalDir.getPath(), ledgerDir.getPath());

        // Upgrade again
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setJournalDirName(journalDir.getPath())
            .setLedgerDirNames(new String[] { ledgerDir.getPath() })
            .setBookiePort(bookiePort)
            .setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        FileSystemUpgrade.upgrade(conf); // should work fine with current directory
        Bookie b = new Bookie(conf);
        b.start();
        b.shutdown();
    }

    @Test
    public void testCommandLine() throws Exception {
        PrintStream origerr = System.err;
        PrintStream origout = System.out;

        File output = IOUtils.createTempFileAndDeleteOnExit("bookie", "stdout");
        File erroutput = IOUtils.createTempFileAndDeleteOnExit("bookie", "stderr");
        System.setOut(new PrintStream(output));
        System.setErr(new PrintStream(erroutput));
        try {
            FileSystemUpgrade.main(new String[] { "-h" });
            try {
                // test without conf
                FileSystemUpgrade.main(new String[] { "-u" });
                fail("Should have failed");
            } catch (IllegalArgumentException iae) {
                assertTrue("Wrong exception " + iae.getMessage(),
                           iae.getMessage().contains("without configuration"));
            }
            File f = IOUtils.createTempFileAndDeleteOnExit("bookie", "tmpconf");
            try {
                // test without upgrade op
                FileSystemUpgrade.main(new String[] { "--conf", f.getPath() });
                fail("Should have failed");
            } catch (IllegalArgumentException iae) {
                assertTrue("Wrong exception " + iae.getMessage(),
                           iae.getMessage().contains("Must specify -upgrade"));
            }
        } finally {
            System.setOut(origout);
            System.setErr(origerr);
        }
    }

    @Test
    public void testOldCookieNewBookie() throws BookieException.UpgradeException {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        try {
            runFunctionWithRegistrationManager(conf, rm -> {
                try {
                    oldCookieNewBookieWorker(conf, rm);
                } catch (BookieException e) {
                    fail("Bookie was expected to run in compatibility mode: " + e.getMessage());
                } catch (IOException e) {
                    fail("Failed to read Cookie, Bookie was expected to run in compatibility mode: " + e.getMessage());
                }
                return null;
            });
        } catch (MetadataException | ExecutionException e) {
            throw new BookieException.UpgradeException(e);
        }
    }

    private void oldCookieNewBookieWorker (ServerConfiguration conf, RegistrationManager rm)
            throws BookieException, IOException {

        String journalDir = newDirectory();
        String ledgerDir = newDirectory();

        conf.setJournalDirName(journalDir)
                .setLedgerDirNames(new String[] { ledgerDir })
                .setBookiePort(bookiePort)
                .setMetadataServiceUri(zkUtil.getMetadataServiceUri());

        Cookie.Builder v4builder = Cookie.generateCookie(conf);
        v4builder = v4builder.setLayoutVersion(4);
        v4builder = v4builder.setIndexDirs("");
        Cookie v4Cookie = v4builder.build();

        // Write v4 cookie to ZK
        v4Cookie.writeToRegistrationManager(rm, conf, Version.NEW);

        // Bring up a bookie with v5 software. It should come up without errors.
        // Assuming the networkLocation etc fields are null
        try {
            Bookie b = new Bookie(conf);
        } catch (InterruptedException | RuntimeException e) {
            fail("Bookie was expected to run in compatibility mode: " + e.getMessage());
        }
    }
}
