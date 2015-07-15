import java.io.DataInputStream;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class ApplicationMaster {
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	protected AtomicInteger numRequestedContainers = new AtomicInteger();
	private AtomicInteger numFailedContainers = new AtomicInteger();
	protected AtomicInteger numAllocatedContainers = new AtomicInteger();
	public int numTotalContainers = 1;
	
	@SuppressWarnings("rawtypes")
	private AMRMClientAsync amRMClient;
	private NMClientAsync nmClientAsync;
	private NMCallbackHandler containerListener;
	private volatile boolean done;
	private List<Thread> launchThreads = new ArrayList<Thread>();
	
	private Configuration conf;
	private int containerMemory;
	private int containerVCores;
	private int requestPriority;
	
	
	private String shellCommand = "";
	private static final String shellCommandPath = "shellCommands";
	private Map<String, String> shellEnv = new HashMap<String, String>();
	/*
	private String shellArgs = "";
	private Map<String, String> shellEnv = new HashMap<String, String>();
	private static final String shellArgsPath = "shellArgs";
	*/

	public static void main(String[] args) throws Exception {
		ApplicationMaster appMaster = new ApplicationMaster();
		appMaster.init(args);
		appMaster.run();
		System.out.println(appMaster.finish());
	}

	private void init(String[] args) throws Exception {
		conf = new YarnConfiguration();
		containerMemory = 10;
		containerVCores = 1;
		DataInputStream ds = new DataInputStream(new FileInputStream(shellCommandPath));
		shellCommand = ds.readUTF();
		IOUtils.closeQuietly(ds);
	}

	private boolean finish() throws Exception {
		System.out.println("finishing");
		while (!done && (numCompletedContainers.get() != numTotalContainers))
			Thread.sleep(2000);
		for (Thread lauThread : launchThreads)
			lauThread.join(1000);
		nmClientAsync.stop();
		
		FinalApplicationStatus appStatus = (numFailedContainers.get() == 0 && numCompletedContainers.get() == numTotalContainers) ? FinalApplicationStatus.SUCCEEDED : FinalApplicationStatus.FAILED;
		amRMClient.unregisterApplicationMaster(appStatus, "", null);
		amRMClient.stop();
		return appStatus == FinalApplicationStatus.SUCCEEDED;
	}

	@SuppressWarnings({ "unchecked", "unchecked" })
	private void run() throws Exception {
		/* setup am-rm client */
		AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		amRMClient.init(conf);
		amRMClient.start();
		
		/* setup am-nm client */
		containerListener = new NMCallbackHandler(this);
		nmClientAsync = new NMClientAsyncImpl(containerListener);
		nmClientAsync.init(conf);
		nmClientAsync.start();
		
		/* register on rm */
		RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(NetUtils.getHostname(), -1, "");
		List<Container> previousAAMRunningContainers = response.getContainersFromPreviousAttempts();
		numAllocatedContainers.addAndGet(previousAAMRunningContainers.size());
		
		int numTotalContainersToRequest = numTotalContainers - previousAAMRunningContainers.size(); // FIXME
		for (int i = 0; i < numTotalContainersToRequest; i++)
			amRMClient.addContainerRequest(setupContainerAskForRM());
		numRequestedContainers.set(numTotalContainers);
	}

	private ContainerRequest setupContainerAskForRM() {
		Priority pri = Priority.newInstance(requestPriority);
		Resource capacity = Resource.newInstance(containerMemory, containerVCores);
		return new ContainerRequest(capacity, null, null, pri);
	}

	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

		@Override
		public float getProgress() {
			return (float) (numCompletedContainers.get() * 1.0 / numTotalContainers);
		}

		@Override
		public void onContainersAllocated(List<Container> allocatedContainers) {
			numAllocatedContainers.addAndGet(allocatedContainers.size());
			for (Container allocatedContainer : allocatedContainers) {
				LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, containerListener);
				Thread launchThread = new Thread(runnableLaunchContainer);
				
				launchThreads.add(launchThread);
				launchThread.start();
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onContainersCompleted(
				List<ContainerStatus> completedContainers) {
			for (ContainerStatus containerStatus : completedContainers) {
				int exitStatus = containerStatus.getExitStatus();
				if (exitStatus != 0) {
					if (exitStatus == ContainerExitStatus.ABORTED) { // FIXME
						numCompletedContainers.incrementAndGet();
						numFailedContainers.incrementAndGet();
					} else {
						numAllocatedContainers.decrementAndGet();
						numRequestedContainers.decrementAndGet();
					}
				} else {
					numCompletedContainers.incrementAndGet();
				}					
			}
			
			int askCount = numTotalContainers - numRequestedContainers.get();
			numRequestedContainers.addAndGet(askCount);
			if (askCount > 0)
				for (int i = 0; i  <askCount; i++)
					amRMClient.addContainerRequest(setupContainerAskForRM());

			if (numCompletedContainers.get() == numTotalContainers)
				done = true;
		}

		@Override
		public void onError(Throwable arg0) { // FIXME
			done = true;
			amRMClient.stop();
		}

		@Override
		public void onNodesUpdated(List<NodeReport> arg0) { // FIXME
		}

		@Override
		public void onShutdownRequest() { // FIXME
			done = true;
		}

	}
	
	static class NMCallbackHandler implements NMClientAsync.CallbackHandler {
		private final ApplicationMaster applicationMaster;
		private ConcurrentHashMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();

		public NMCallbackHandler(ApplicationMaster applicationMaster) {
			this.applicationMaster = applicationMaster;
		}

		@Override
		public void onContainerStarted(ContainerId containerId,
				Map<String, ByteBuffer> allServiceResponse) {
		}

		@Override
		public void onContainerStatusReceived(ContainerId arg0,
				ContainerStatus arg1) {
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			System.out.println("container stopped");
		}

		@Override
		public void onGetContainerStatusError(ContainerId arg0, Throwable arg1) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable t) {
			containers.remove(containerId);
			applicationMaster.numCompletedContainers.incrementAndGet(); // FIXME
			applicationMaster.numFailedContainers.incrementAndGet();
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable t) {
			// TODO Auto-generated method stub
			containers.remove(containerId);
		}

		public void addContainer(ContainerId id, Container container) {
			containers.put(id, container);
		}
	}
	
	private class LaunchContainerRunnable implements Runnable {
		Container container;
		NMCallbackHandler containerListener;
		
		public LaunchContainerRunnable(Container allocatedContainer,
				NMCallbackHandler containerListener) {
			this.container = allocatedContainer;
			this.containerListener = containerListener;
		}

		@Override
		public void run() {
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
			Vector<CharSequence> vargs = new Vector<CharSequence>(5);
			vargs.add(shellCommand);
			vargs.add("1>"+ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
			vargs.add("2>"+ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
			StringBuilder command = new StringBuilder();
		    for (CharSequence str : vargs)
		        command.append(str).append(" ");
		    List<String> commands = new ArrayList<String>();
		    commands.add(command.toString());
		    
		    ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(localResources, shellEnv, commands, null, null, null);
		    containerListener.addContainer(container.getId(), container);
		    nmClientAsync.startContainerAsync(container, ctx);
		}
	}
}