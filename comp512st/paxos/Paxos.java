package comp512st.paxos;
// Access to the GCL layer
import comp512.gcl.*;
import comp512.utils.*;
// Any other imports that you may need.
import java.io.*;
import java.util.logging.*;
import java.util.Objects;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.Math;
import java.lang.Comparable;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.*;

// ANY OTHER classes, etc., that you add must be private to this package and not visible to the application layer.
// extend / implement whatever interface, etc. as required.
// NO OTHER public members / methods allowed. broadcastTOMsg, acceptTOMsg, and shutdownPaxos must be the only visible methods to the application layer.
//		You should also not change the signature of these methods (arguments and return value) other aspects maybe changed with reasonable design needs.
public class Paxos {
	//TODO handle case where we need to reelect Leader
	//detect process failures and update quorum requirements and handle consequences
	//probably will be done via heartbeat mechanism
	String myProcess;
	GCL gcl;
	FailCheck failCheck;
	volatile String leader;
	//queue stores messages that have been accepted and received by the process
	//the head of the queue has the messages with the smallest sequence number
	PriorityBlockingQueue<AcceptedMessage> queue = new PriorityBlockingQueue<>();
	
	AtomicInteger ballotID = new AtomicInteger();
	AtomicInteger SendSeq = new AtomicInteger(); //current sequence number, starts at 0
	AtomicInteger ReceiveSeq = new AtomicInteger();
	AtomicBoolean waitingOnAccept = new AtomicBoolean(false);
	
	Thread listener;
	volatile int numProcesses = 0;
	volatile int quorum = 0;
	Lock lock = new ReentrantLock();
	Condition condition = lock.newCondition();
	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException {
		//TODO Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.myProcess = myProcess;
		this.failCheck = failCheck;
		numProcesses = allGroupProcesses.length;
		quorum = (int)Math.ceil((numProcesses+1)/2);
		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger) ;
		leader = null;
		//get port number to establish canonical order between processes
		//which will be used as first ballotID
		Pattern pattern = Pattern.compile(".*:(\\d+)$");
		Matcher matcher = pattern.matcher(myProcess);
		if (matcher.find()) ballotID.set(Integer.parseInt(matcher.group(1)));
		//start Paxos Listener
		this.listener = new Thread(new PaxosListener(this));
		listener.start();
	}
	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val) throws InterruptedException {
		// This is just a place holder.
		// Extend this to build whatever Paxos logic you need to make sure the messaging system is total order.
		// Here you will have to ensure that the CALL BLOCKS, and is returned ONLY when a majority (and immediately upon majority) of processes have accepted the value.
		//elect new leader if no leader
		//the same leader will remain elected until it fails, all processes will send their requests to the leader
		//we use the stable leader approach so that the leader can also determine total order on top of deciding consensus
		//TODO implement mechanism to detect Leader failure and trigger phase 1 again
		AcceptedMessage TO_val = new AcceptedMessage((int)((Object[])val)[0], (char)((Object[])val)[1], -1, myProcess);
		PaxosMessage msg;
		waitingOnAccept.set(true);
		lock.lock();
		if(leader == null) {//elect new leader
			msg = new PaxosMessage(ballotID.get(),PaxosType.PROPOSE,TO_val, myProcess);
			gcl.broadcastMsg(msg);
		} else if (leader.equals(myProcess)) {//leader is making request
			TO_val.seq = getNextSeq();
			msg = new PaxosMessage(ballotID.get(),PaxosType.ACCEPT,TO_val, myProcess);
			gcl.broadcastMsg(msg);
		} else {
			//there is a leader and current process is not the leader
			//so we forward request to the leader
			msg = new PaxosMessage(ballotID.get(),PaxosType.FWDTOLEADER,TO_val, myProcess);
			gcl.sendMsg(msg, leader);
		}
		
		//Ensure call blocks and returns only when value has been accepted and reached quorum
		try {
			while (waitingOnAccept.get()) {
				condition.await();
			}
		} finally {
			lock.unlock();
		}
	}
	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	// just check if head of queue matches expected sequence number
	public Object acceptTOMsg() throws InterruptedException {
		//busy wait until we get expected message delivered in queue, may be better way to do this
		//maybe use wait or await to on a lock that will signal that the queue is nonempty
		while(queue.peek() == null) {
		}
		while(queue.peek().seq != ReceiveSeq.get()) {
			//TODO add timeout?
		}
		ReceiveSeq.getAndIncrement();//increment sequence number
		return queue.poll().val;
	}
	// Add any of your own shutdown code into this method.
	public void shutdownPaxos() {
		//shutdown PaxosListener
		gcl.sendMsg(new PaxosMessage(0, PaxosType.STOP, null, myProcess), myProcess);
		try {
			listener.join(500); //wait up to 0.5 seconds for the listener to join
		} catch (InterruptedException e){
			//ignore
		} 
		gcl.shutdownGCL();
	}
	//updates and returns correct send sequence number
	synchronized int getNextSeq() {
		int seq = Math.max(SendSeq.get(), ReceiveSeq.get());
		SendSeq.set(seq + 1);
		return seq;
	}
}
enum PaxosType {
	PROPOSE,
	PROMISE,
	ACCEPT,
	ACCEPTACK,
	CONFIRM,
	DENY,
	STOP,
	FWDTOLEADER
}
class PaxosMessage implements Serializable {
	int ballotID;
	PaxosType type;
	Object val;
	String process; //sending process
	public PaxosMessage(int ballotID, PaxosType type, Object val, String process){
		this.ballotID = ballotID;
		this.type = type;
		this.val = val;
		this.process = process;
	}
}
class AcceptedMessage implements Comparable<AcceptedMessage>,Serializable {
	Object[] val = new Object[2];
	int seq;
	String origin;//name of process that requested update
	public AcceptedMessage(int player, char move, int seq, String origin) {
		this.val[0] = player;
		this.val[1] = move;
		this.seq = seq;
		this.origin = origin;
	}
	@Override
	public int compareTo(AcceptedMessage other) {
		return Integer.compare(this.seq, other.seq);
	}
}
//Thread to listen to all messages
class PaxosListener implements Runnable, Serializable {
	Paxos paxos;
	
	// Deduplication tracking
	Map<Integer, Integer> promiseCounts = new HashMap<>(); // ballot -> count
	Set<Integer> processedBallots = new HashSet<>(); // ballots that elected leader
	Set<Integer> processedDenies = new HashSet<>(); // ballots we've retried after DENY
	Set<Integer> acceptedSeqs = new HashSet<>(); // sequences we've already accepted
	Map<Integer, Set<String>> acceptAckSenders = new HashMap<>(); // seq -> set of processes that sent ACK
	Set<Integer> confirmedSeqs = new HashSet<>(); // sequences we've already confirmed
	
	public PaxosListener(Paxos paxos) {
		this.paxos = paxos;
	}
	
	public void run() {
		while(true) {
			try {
				GCMessage gcmsg = paxos.gcl.readGCMessage();
				//here we determine the message type
				switch (gcmsg.val) {
					case PaxosMessage m -> {
						if(m.type == PaxosType.STOP) return; //check for STOP signal
						handlePaxosMessage(m);
					}
					default -> {//means non paxos, may be unecessary?
						throw new IllegalArgumentException();
					}
				}
			} catch (IllegalArgumentException e) {
				//ignore
			} catch (InterruptedException e) {
				//ignore
			} catch (IllegalStateException e) {
				//ignore
			}
		}
	}
	void handlePaxosMessage(PaxosMessage m) throws IllegalArgumentException, InterruptedException {
		PaxosMessage msg;
		switch(m.type) {
			case FWDTOLEADER -> {
				//leader directly handles the forwarded request
				AcceptedMessage TO_val = (AcceptedMessage) m.val;
				TO_val.seq = paxos.getNextSeq();
				msg = new PaxosMessage(paxos.ballotID.get(), PaxosType.ACCEPT, TO_val, paxos.myProcess);
				paxos.gcl.broadcastMsg(msg);
			}
			case PROPOSE -> {
				if (m.ballotID >= paxos.ballotID.get()) { //we are able to make promise
					paxos.ballotID.set(m.ballotID);	
					msg = new PaxosMessage(m.ballotID, PaxosType.PROMISE, m.val, paxos.myProcess);
				} else {//DENY proposal
					msg = new PaxosMessage(paxos.ballotID.get(), PaxosType.DENY, m.val, paxos.myProcess);
				}
				paxos.gcl.sendMsg(msg, m.process);//send response
			}
			case PROMISE -> {
				if(m.ballotID == paxos.ballotID.get() && !processedBallots.contains(m.ballotID)) {
					int count = promiseCounts.getOrDefault(m.ballotID, 0) + 1;
					promiseCounts.put(m.ballotID, count);
					if (count == paxos.quorum) {//current process has been elected as leader
						processedBallots.add(m.ballotID); //mark this ballot as processed
						promiseCounts.remove(m.ballotID); //clean up
						paxos.leader = paxos.myProcess;
						AcceptedMessage TO_val = (AcceptedMessage) m.val;
						TO_val.seq = paxos.getNextSeq();
						msg = new PaxosMessage(paxos.ballotID.get(), PaxosType.ACCEPT, TO_val, paxos.myProcess);
						paxos.gcl.broadcastMsg(msg); //send accept message
					}
				}
			}
			case ACCEPT -> {
				//only process each sequence number once
				AcceptedMessage TO_val = (AcceptedMessage) m.val;
				if (!acceptedSeqs.contains(TO_val.seq)) {
					acceptedSeqs.add(TO_val.seq);
					if (paxos.leader == null) paxos.leader = m.process;
					paxos.queue.add(TO_val);
					msg = new PaxosMessage(paxos.ballotID.get(), PaxosType.ACCEPTACK, TO_val, paxos.myProcess);
					paxos.gcl.sendMsg(msg, paxos.leader);
				}
			}
			case ACCEPTACK -> {
				AcceptedMessage TO_val = (AcceptedMessage) m.val;
				if(m.ballotID == paxos.ballotID.get()) {
					//only count each sender once per sequence
					Set<String> senders = acceptAckSenders.computeIfAbsent(TO_val.seq, k -> new HashSet<>());
					if (!senders.contains(m.process)) {
						senders.add(m.process);
						int count = senders.size();
						
						if (count == paxos.quorum && !confirmedSeqs.contains(TO_val.seq)) {
							//mark as confirmed before broadcasting to prevent duplicates
							confirmedSeqs.add(TO_val.seq);
							msg = new PaxosMessage(paxos.ballotID.get(), PaxosType.CONFIRM, m.val, paxos.myProcess);
							paxos.gcl.broadcastMsg(msg);
							acceptAckSenders.remove(TO_val.seq); //clean up
						}
					}
				}
			}
			case CONFIRM -> {
				if((((AcceptedMessage)m.val).origin).equals(paxos.myProcess)) {
					paxos.lock.lock();
					try {
						paxos.waitingOnAccept.set(false);
						paxos.condition.signalAll(); //notify process requesting value change has been accepted
					} finally {
						paxos.lock.unlock();
					}
				}
			}
			case DENY -> {
				//only process the first DENY for each ballot ID to prevent cascade
				int currentBallot = paxos.ballotID.get();
				if (!processedDenies.contains(currentBallot)) {
					processedDenies.add(currentBallot);
					paxos.ballotID.set(currentBallot + 1);
					if(paxos.leader == null) {
						msg = new PaxosMessage(paxos.ballotID.get(), PaxosType.PROPOSE, m.val, paxos.myProcess);
						paxos.gcl.broadcastMsg(msg);
					} else if (paxos.leader.equals(paxos.myProcess)) {
						// TODO may be unnecessary
					} else {
						msg = new PaxosMessage(paxos.ballotID.get(), PaxosType.FWDTOLEADER, m.val, paxos.myProcess);
						paxos.gcl.sendMsg(msg, paxos.leader);
					}
				}
			}
			default -> {
				throw new IllegalArgumentException();
			}
		}
	}
}
