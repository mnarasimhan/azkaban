package azkaban.jobs.builtin;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;
import org.easymock.classextension.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import azkaban.app.JobDescriptor;
import azkaban.app.JobManager;
import azkaban.common.utils.Props;
import azkaban.flow.FlowExecutionHolder;
import azkaban.flow.FlowManager;
import azkaban.flow.IndividualJobExecutableFlow;
import azkaban.jobs.AbstractProcessJob;
import azkaban.jobs.JobExecutorManager;
import azkaban.jobs.Status;
import azkaban.jobs.builtin.PigProcessJob;
import azkaban.jobs.builtin.ProcessJob;
import azkaban.monitor.MonitorImpl;

import azkaban.monitor.MonitorListener;
import azkaban.monitor.MonitorInterface.GlobalNotificationType;
import azkaban.monitor.MonitorInterface.WorkflowState;
import azkaban.monitor.MonitorInternalInterface.WorkflowAction;
import azkaban.monitor.model.WorkflowExecutionModel;
import azkaban.monitor.stats.ClassStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;
import azkaban.workflow.Job;

import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;

public class PigJobRunnerTest {
    private static Logger logger = Logger.getLogger(PigJobRunnerTest.class);  
    
    private final int _namenodePort = 9000;
    private final int _jobtrackerPort = 9001;
    private final int _datanodeCount = 1;
    private final int _tasktrackerCount = 1;
    private final int _tasktrackerPort = 9002;

    private MiniDFSCluster _dfsCluster;
    private MiniMRCluster _mrCluster;

    String NAMENODE = "fs.default.name";
    String JOBTRACKER = "mapred.job.tracker";

    private final String rootJobName = "MyJob";

    private static String classPaths;

    private int specificJobClassNotifications;

    final File testDirBase = new File("test/runAsUserTest");

    private static String currentUser;
    
    private final String testUser = "harry";
    
    private final String testUserGroup = "potter";

    @BeforeClass
    public static void init() throws Exception {      
        Properties prop = System.getProperties();     
        File f = new File("test/jobs/conf/");      
        classPaths = String.format("'%s'",
                prop.getProperty("java.class.path", null));
        //Add core-site.xml to the classpath
        classPaths += ":"+f.getAbsolutePath();       
    }

    @Before
    public void setUp() throws Exception {

        FileUtils.deleteDirectory(testDirBase);
        currentUser = UserGroupInformation.getCurrentUser().getUserName();

        testDirBase.mkdirs();

        FileUtils.copyFileToDirectory(new File(
                "examples/pig/script/script3-hadoop.pig"), testDirBase);
        FileUtils.copyFileToDirectory(new File(
                "examples/pig/resources/tutorial.jar"), testDirBase);
        FileUtils.copyFileToDirectory(new File(
                "examples/pig/resources/excite-small.log"), testDirBase);

    }

    @After
    public void tearDown() {
        
        stop();

    }

    @Test
    public void testSecureModeUser() throws Throwable {
        PigProcessJob job = null;
        JobDescriptor descriptor = null;
        Props props = null;
 
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        //Start MiniDFSCluster and MiniMRCluster
        start();
        conf.set("fs.default.name", conf.get(NAMENODE));
        conf.set("mapred.job.tracker",
                _mrCluster.createJobConf().get(JOBTRACKER));
        
        FileSystem hdfs = _dfsCluster.getFileSystem();
        //Create user directory for test user and set approp permissions
        Path userHome = new Path(hdfs.getHomeDirectory().getParent() + "/" + testUser + "/");
        hdfs.mkdirs(userHome);
        hdfs.setOwner(userHome, testUser, "supergroup");
        hdfs.setPermission(userHome, new FsPermission("777"));

        //Change perms of mapred directory
        Path mapredDir = new Path(hdfs.getHomeDirectory()
                + "/test/runAsUserTest/hadoop-tmp/mapred");
        hdfs.setPermission(mapredDir, new FsPermission("777"));
      
        //Output directory
        Path output = new Path(hdfs.getHomeDirectory().getParent() + "/" + testUser + "/script3-hadoop-results");
        if(hdfs.exists(output)){
            hdfs.delete(output);
        }
        System.out.println("output is "+output);

        descriptor = EasyMock.createMock(JobDescriptor.class);

        props = new Props();
        props.put(AbstractProcessJob.WORKING_DIR, testDirBase.getPath());
        props.put("type", "pig");
        props.put(PigProcessJob.PIG_SCRIPT, "script3-hadoop.pig");
        props.put("classpath", classPaths);
        props.put(PigProcessJob.HADOOP_UGI, testUser + "," +testUserGroup);
        props.put(PigProcessJob.HADOOP_SECURE_MODE, "true");
        props.put("udf.import.list", "com.test.SUM,com.test.MOD");
        props.put(JavaProcessJob.JVM_PARAMS,"-Dfs.default.name=hdfs://localhost:9000 -Dmapred.job.tracker=hdfs://localhost:9001");  
        props.put(JavaProcessJob.CLASSPATH, classPaths);
       

        org.easymock.EasyMock.expect(descriptor.getId()).andReturn("script").times(1);
        org.easymock.EasyMock.expect(descriptor.getProps()).andReturn(props).times(1);
        org.easymock.EasyMock.expect(descriptor.getFullPath()).andReturn(".").times(1);
        EasyMock.replay(descriptor);
        job = new PigProcessJob(descriptor);

        EasyMock.verify(descriptor);
        job.run();
       
        //Get the job_conf.xml and extract user from that
        JobConf jc = new JobConf(getJobConf());
        Assert.assertEquals(testUser, jc.getUser());
      

    }  

    public void start() throws IOException {
        
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        System.setProperty("test.build.data", testDirBase.getPath());
        System.setProperty("hadoop.log.dir", testDirBase + "/logs");
        System.setProperty("hadoop.tmp.dir", testDirBase + "/hadoop-tmp");
        conf.set("hadoop.proxyuser." + currentUser + ".groups", "*");
        conf.set("hadoop.proxyuser." + currentUser + ".hosts", "*");

        _dfsCluster = new MiniDFSCluster(_namenodePort, conf, _datanodeCount,
                true, true, StartupOption.REGULAR, null);
        System.out.println("started namenode on " + conf.get(NAMENODE));

        _mrCluster = new MiniMRCluster(_jobtrackerPort, 0, _tasktrackerCount,
                conf.get(NAMENODE), 1);
        System.out.println("started jobtracker on "
                + _mrCluster.createJobConf().get(JOBTRACKER));

    }

    public JobConf createJobConf() {
        if (_mrCluster == null) {
            throw new IllegalStateException("Cluster has not started yet...");
        }
        return _mrCluster.createJobConf();
    }

    public void stop() {

        _mrCluster.shutdown();
        _dfsCluster.shutdown();
    }
    
    public String getJobConf(){
        File dir = new File(System.getProperty("hadoop.log.dir"));

        File[] files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".xml");
            }
        });
        
        return files[0].getPath();
    }

}
