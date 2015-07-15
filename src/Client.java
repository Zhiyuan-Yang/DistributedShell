import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class Client {
	private YarnClient yarnClient;
	private Configuration conf;
	private ApplicationId appId;

	private String appName = "yzy's app";
	private String appMasterJar = "/Users/zyang/Desktop/dsh.jar";
	private String appMasterJarPath = "AppMaster.jar";
	private String shellCommand = "date";
	private String shellCommandPath = "shellCommands";
	private int amMemory = 10;
	private CharSequence appMasterMainClass = "ApplicationMaster";
	private int amVCores = 1;
	private int amPriority = 0;
	private String amQueue = "default";
	private int amNumContainers = 1;

	public Client() {
		conf = new YarnConfiguration();
		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
	}

	public static void main(String[] args) throws Exception {
		Client client = new Client();
		client.run();
	}

	private void run() throws Exception {
		yarnClient.start();
		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
		appId = appResponse.getApplicationId();
		ApplicationSubmissionContext appContext = app
				.getApplicationSubmissionContext();
		appContext.setKeepContainersAcrossApplicationAttempts(false);
		appContext.setApplicationName(appName);

		/* local resources */
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		FileSystem fs = FileSystem.get(conf);
		addToLocalResources(fs, appMasterJar, appMasterJarPath, localResources,
				null);
		addToLocalResources(fs, null, shellCommandPath, localResources,
				shellCommand);

		/* env var */
		Map<String, String> env = new HashMap<String, String>();
		StringBuilder classPathEnv = new StringBuilder(
				Environment.CLASSPATH.$$()).append(
				ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		for (String c : conf
				.getStrings(
						YarnConfiguration.YARN_APPLICATION_CLASSPATH,
						YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(c.trim());
		}
		env.put("CLASSPATH", classPathEnv.toString());
		System.out.println("******");
		System.out.println(conf.toString());
		System.out.println("******");

		/* cmd to exec appMaster */
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		vargs.add("-Xmx" + amMemory + "m");
		vargs.add(appMasterMainClass);
		vargs.add("--container_memory " + amMemory);
		vargs.add("--container_vcores " + amVCores);
		vargs.add("--num_containers " + amNumContainers);
		vargs.add("--priority " + amPriority);
		vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
				+ "/AppMaster.stdout");
		vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
				+ "/AppMaster.stderr");
		StringBuilder command = new StringBuilder();
		for (CharSequence str : vargs) {
			command.append(str).append(" ");
		}
		List<String> commands = new ArrayList<String>();
		commands.add(command.toString());

		/* container launch context */
		ContainerLaunchContext amContainer = ContainerLaunchContext
				.newInstance(localResources, env, commands, null, null, null);

		/* app submission context */
		Resource capability = Resource.newInstance(amMemory, amVCores);
		appContext.setResource(capability);
		appContext.setAMContainerSpec(amContainer);
		Priority pri = Priority.newInstance(amPriority);
		appContext.setPriority(pri);
		appContext.setQueue(amQueue);

		yarnClient.submitApplication(appContext);

		monitorApplication();
	}

	private void addToLocalResources(FileSystem fs, String fileSrcPath,
			String fileDstPath, Map<String, LocalResource> localResource,
			String resources) throws Exception {
		String suffix = appName + "/" + appId + "/" + fileDstPath;
		Path dst = new Path(fs.getHomeDirectory(), suffix);
		if (fileSrcPath == null) {
			FSDataOutputStream ostream = FileSystem.create(fs, dst,
					new FsPermission((short) 0710));
			ostream.writeUTF(resources);
			IOUtils.closeQuietly(ostream);
		} else {
			fs.copyFromLocalFile(new Path(fileSrcPath), dst);
		}
		FileStatus scFileStatus = fs.getFileStatus(dst);
		LocalResource scRsrc = LocalResource.newInstance(
				ConverterUtils.getYarnUrlFromPath(dst), LocalResourceType.FILE,
				LocalResourceVisibility.APPLICATION, scFileStatus.getLen(),
				scFileStatus.getModificationTime());
		localResource.put(fileDstPath, scRsrc);
	}

	private void monitorApplication() throws Exception {
		while (true) {
			Thread.sleep(1000);
			ApplicationReport report = yarnClient.getApplicationReport(appId);
			System.out.println("Got application report from ASM for"
					+ ", appId=" + appId.getId() + ", clientToAMToken="
					+ report.getClientToAMToken() + ", appDiagnostics="
					+ report.getDiagnostics() + ", appMasterHost="
					+ report.getHost() + ", appQueue=" + report.getQueue()
					+ ", appMasterRpcPort=" + report.getRpcPort()
					+ ", appStartTime=" + report.getStartTime()
					+ ", yarnAppState="
					+ report.getYarnApplicationState().toString()
					+ ", distributedFinalState="
					+ report.getFinalApplicationStatus().toString()
					+ ", appTrackingUrl=" + report.getTrackingUrl()
					+ ", appUser=" + report.getUser());

			YarnApplicationState state = report.getYarnApplicationState();
			if (state == YarnApplicationState.FINISHED) {
				System.out.println("succeeded");
				return;
			} else if (state == YarnApplicationState.KILLED
					|| state == YarnApplicationState.FAILED) {
				System.out.println("failed");
				return;
			}
		}
	}
}