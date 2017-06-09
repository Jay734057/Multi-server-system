package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;

public class ControlSolution extends Control {
	private static final Logger log = LogManager.getLogger();

	/*
	 * additional variables as needed
	 */

	JSONParser parser = new JSONParser();
	// information for server itself
	private int localPort;
	private String localHostName;
	private final String authSecret;
	private String secret;
	private String id;
	// info for announce collection
	private ArrayList<String> serverIDs = new ArrayList<String>();
	private ArrayList<Integer> load = new ArrayList<Integer>();
	private ArrayList<String> hostName = new ArrayList<String>();
	private ArrayList<Integer> hostPort = new ArrayList<Integer>();
	// info for authenticated servers and logged users
	private ArrayList<Connection> servers = new ArrayList<Connection>();
	private ArrayList<Connection> clients = new ArrayList<Connection>();
	private ArrayList<String> loggedUser = new ArrayList<String>();
	private ArrayList<String> loggedSecret = new ArrayList<String>();
	// info for register and local storage
	private ArrayList<Connection> register = new ArrayList<Connection>();
	private ArrayList<String> registerUser = new ArrayList<String>();;
	private ArrayList<ArrayList<String>> registerFlag = new ArrayList<ArrayList<String>>();
	private ArrayList<String> usernames = new ArrayList<String>();
	private ArrayList<String> secrets = new ArrayList<String>();

	///////////////////////////////////////
	// since control and its subclasses are singleton, we get the singleton this
	/////////////////////////////////////// way
	public static ControlSolution getInstance() {
		if (control == null) {
			control = new ControlSolution();
		}
		return (ControlSolution) control;
	}

	public ControlSolution() {
		super();
		/*
		 * Do some further initialization here if necessary
		 */
		// initialization for server info
		localPort = Settings.getLocalPort();
		localHostName = Settings.getLocalHostname();
		id = Settings.nextSecret();
		secret = Settings.getSecret();
		if (secret == null) {
			authSecret = Settings.nextSecret();
			System.out.println(
				"The Secret to connect to the server:" + authSecret);
		} else {
			authSecret = secret;
		}

		// check if we should initiate a connection and do so if necessary
		initiateConnection();
		// start the server's activity loop
		// it will call doActivity every few seconds
		start();
	}

	/*
	 * a new incoming connection
	 */
	@Override
	public Connection incomingConnection(Socket s) throws IOException {
		Connection con = super.incomingConnection(s);
		/*
		 * do additional things here
		 */
		// nothing is needed to do here
		return con;
	}

	/*
	 * a new outgoing connection
	 */
	@Override
	public Connection outgoingConnection(Socket s) throws IOException {
		Connection con = super.outgoingConnection(s);
		/*
		 * do additional things here
		 */
		// try to authenticate to another server
		JSONObject obj = new JSONObject();
		obj.put("command", "AUTHENTICATE");
		obj.put("secret", this.secret);
		con.writeMsg(obj.toString());
		this.servers.add(con);
		return con;
	}

	/*
	 * the connection has been closed
	 */
	@Override
	public void connectionClosed(Connection con) {
		super.connectionClosed(con);
		/*
		 * do additional things here
		 */
		// nothing to do...
	}

	/*
	 * process incoming msg, from connection con return true if the connection
	 * should be closed, false otherwise
	 */
	@Override
	public synchronized boolean process(Connection con, String msg) {
		/*
		 * do additional work here return true/false as appropriate
		 */
		/////////////////////////////
		String line = msg;
		// log.info("Receiving msg: " + line);
		JSONObject obj;
		try {
			obj = (JSONObject) parser.parse(msg);
			Object command = obj.get("command");
			// chect command first
			if (command == null) {
				// missing command
				sendMsg(con, "INVALID_MESSAGE",
					"the received message did not contain a command");
				return true;
			} else {
				// process command
				switch (command.toString()) {
				case "CLOSE CONNECTION":
					sendMsg(con, "CLOSE CONNECTION", "");
					if (clients.indexOf(con) != -1) {
						this.loggedUser.remove(this.clients.indexOf(con));
						this.loggedSecret
							.remove(this.clients.indexOf(con));
						this.clients.remove(con);
					}
					return true;
				case "AUTHENTICATE":
					return this.authentication(con, obj);
				case "INVALID_MESSAGE":
					return this.invalidMsg(con, obj);
				case "AUTHENTICATION_FAIL":
					return this.authenticationFail(con, obj);
				case "LOGIN":
					return this.login(con, obj);
				case "LOGOUT":
					return this.logout(con, obj);
				case "ACTIVITY_MESSAGE":
					return this.activityMsg(con, obj);
				case "SERVER_ANNOUNCE":
					return this.serverAnnounce(con, obj);
				case "ACTIVITY_BROADCAST":
					return this.activityBroadcast(con, obj);
				case "REGISTER":
					return this.register(con, obj);
				case "LOCK_REQUEST":
					return this.lockRequest(con, obj);
				case "LOCK_DENIED":
					return this.lockDenied(con, obj);
				case "LOCK_ALLOWED":
					return this.lockAllowed(con, obj);
				default:
					// invalid command received
					sendMsg(con, "INVALID_MESSAGE", "invalid command");
					int key = clients.indexOf(con);
					if (key != -1) {
						this.loggedUser.remove(key);
						this.loggedSecret.remove(key);
						clients.remove(con);
					} else
						servers.remove(con);
					return true;
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
			sendMsg(con, "INVALID_MESSAGE",
				"JSON parse error while parsing message");
		}
		///////////////////////////////
		return false;
	}

	/*
	 * Called once every few seconds Return true if server should shut down,
	 * false otherwise
	 */
	@Override
	public boolean doActivity() {
		/*
		 * do additional work here return true/false as appropriate
		 */
		// broadcast the server announce regularly
		JSONObject obj = new JSONObject();
		obj.put("command", "SERVER_ANNOUNCE");
		obj.put("id", this.id);
		obj.put("load", this.clients.size());
		obj.put("hostname", this.localHostName);
		obj.put("port", this.localPort);
		for (int i = 0; i < servers.size(); i++) {
			servers.get(i).writeMsg(obj.toString());
		}

		return false;
	}

	/*
	 * Other methods as needed
	 */

	/*
	 * Called when a authentication message arrive
	 */
	private boolean authentication(Connection con, JSONObject obj) {
		Object o = obj.get("secret");
		// check secret first
		if (o == null) {
			sendMsg(con, "INVALID_MESSAGE", "Missing secret");
			return true;
		}
		String secret = o.toString();
		if (authSecret.equals(secret)) {
			// secret matches
			if (servers.indexOf(con) == -1) {
				// not authenticated yet
				servers.add(con);
				return false;
			} else {
				// already be authencated
				servers.remove(con);
				sendMsg(con, "AUTHENTICATION_FAIL",
					"server has already been authenticated");
				return true;
			}
		} else {
			// secret mismatched
			sendMsg(con, "AUTHENTICATION_FAIL",
				"the supplied secret is incorrect: " + secret);
			return true;
		}
	}

	/*
	 * Called when a command indicating invalid message arrive
	 */
	private boolean invalidMsg(Connection con, JSONObject obj) {
		// receive command indicating invalid msg
		String info = obj.get("info").toString();
		if (info != null)
			log.info(info);
		return true;
	}

	/*
	 * Called when a authentication fail message arrive
	 */
	private boolean authenticationFail(Connection con, JSONObject obj) {
		// authentication fails
		servers.remove(con);
		String info = obj.get("info").toString();
		if (info != null)
			log.info(info);
		return true;
	}

	/*
	 * Called when a login message arrive
	 */
	private boolean login(Connection con, JSONObject obj) {
		// check username first
		Object user = obj.get("username").toString();
		if (user == null) {
			// miss username
			sendMsg(con, "INVALID_MESSAGE", "Missing username");
			return true;
		}
		if (user.equals("anonymous")) {
			// anonymous user
			sendMsg(con, "LOGIN_SUCCESS", "logged in as user " + user);
			if (serverIDs.size() == 0) {
				// if no other servers in the network, no need for redirection
				this.clients.add(con);
				this.loggedUser.add(user.toString());
				this.loggedSecret.add("");
				return false;
			} else
				// redirect the user if necessary
				return redirect(con, user, secret);
		}
		Object secret = obj.get("secret").toString();
		if (secret == null) {
			// check secret
			sendMsg(con, "INVALID_MESSAGE", "Missing secret");
			return true;
		}
		if (this.loggedUser.indexOf(user.toString()) != -1) {
			// user has already logged in
			sendMsg(con, "LOGIN_FAILED",
				"username has already been logged");
			return true;
		}
		if (usernames.indexOf(user.toString()) == -1) {
			// username cannot be found in local storage
			sendMsg(con, "LOGIN_FAILED", "username not found");
			return true;
		} else {
			if (secrets.get(usernames.indexOf(user.toString()))
				.equals(secret.toString())) {
				// username and secret match
				sendMsg(con, "LOGIN_SUCCESS", "logged in as user " + user);
				if (serverIDs.size() == 0) {
					// no other servers in the network
					this.clients.add(con);
					this.loggedUser.add(user.toString());
					this.loggedSecret.add(secret.toString());
					return false;
				} else
					// redirect if necessary
					return redirect(con, user, secret);
			} else {
				// mismatch
				sendMsg(con, "LOGIN_FAILED",
					"attempt to login with wrong secret");
				return true;
			}
		}
	}

	/*
	 * Called when an activity message arrive
	 */
	private boolean activityMsg(Connection con, JSONObject obj)
		throws ParseException {
		if (clients.indexOf(con) == -1) {
			// client has not logged in yet
			sendMsg(con, "AUTHENTICATION_FAIL", "not logged in client");
			return true;
		}
		Object user = obj.get("username");
		Object secret = obj.get("secret");
		Object activity = obj.get("activity");

		if (user == null) {
			// missing username
			sendMsg(con, "INVALID_MESSAGE", "Missing username");
			this.loggedUser.remove(this.clients.indexOf(con));
			this.loggedSecret.remove(this.clients.indexOf(con));
			clients.remove(con);
			return true;
		}

		if (!user.toString().equals("anonymous")) {
			// not anonymous
			if (secret == null) {
				// missing secret
				sendMsg(con, "INVALID_MESSAGE", "Missing secret");
				this.loggedUser.remove(this.clients.indexOf(con));
				this.loggedSecret.remove(this.clients.indexOf(con));
				clients.remove(con);
				return true;
			}
			if (this.loggedUser.isEmpty()
				|| (this.loggedUser.indexOf(user.toString()) == -1)) {
				// user has not logged in yet
				sendMsg(con, "AUTHENTICATION_FAIL",
					user.toString() + " has not logged in yet");
				this.loggedUser.remove(this.clients.indexOf(con));
				this.loggedSecret.remove(this.clients.indexOf(con));
				clients.remove(con);
				return true;

			} else if (!this.loggedSecret
				.get(loggedUser.indexOf(user.toString()))
				.equals(secret.toString())) {
				// mismatch
				sendMsg(con, "AUTHENTICATION_FAIL",
					"do not match the logged in the user");
				this.loggedUser.remove(this.clients.indexOf(con));
				this.loggedSecret.remove(this.clients.indexOf(con));
				clients.remove(con);
				return true;
			}
		}

		if (activity == null) {
			// missing activity
			sendMsg(con, "INVALID_MESSAGE", "Missing activity");
			this.loggedUser.remove(this.clients.indexOf(con));
			this.loggedSecret.remove(this.clients.indexOf(con));
			clients.remove(con);
			return true;
		}

		// verify successfully
		JSONObject o = (JSONObject) parser.parse(activity.toString());
		// add authenticated_user to activity object
		o.put("authenticated_user", user.toString());
		// broadcast the activity
		JSONObject j = new JSONObject();
		j.put("command", "ACTIVITY_BROADCAST");
		j.put("activity", o);
		for (int i = 0; i < servers.size(); i++)
			servers.get(i).writeMsg(j.toString());

		for (int i = 0; i < clients.size(); i++)
			if (clients.get(i) != con)
				clients.get(i).writeMsg(j.toString());
		return false;
	}

	/*
	 * Called when a logout message arrive
	 */
	private boolean logout(Connection con, JSONObject obj) {
		sendMsg(con, "CLOSE CONNECTION", "");
		this.loggedUser.remove(this.clients.indexOf(con));
		this.loggedSecret.remove(this.clients.indexOf(con));
		this.clients.remove(con);
		return true;
	}

	/*
	 * Called when a server announce arrive
	 */
	private boolean serverAnnounce(Connection con, JSONObject obj) {
		if (servers.indexOf(con) == -1) {
			// unauthenticated server
			sendMsg(con, "INVALID_MESSAGE", "unauthenticated server");
			return true;
		}
		// check id first
		Object o = obj.get("id");
		if (o == null) {
			// missing id
			sendMsg(con, "INVALID_MESSAGE", "Missing id");
			return true;
		}
		// check the load
		o = obj.get("load");
		if (o == null) {
			sendMsg(con, "INVALID_MESSAGE", "Missing load");
			return true;
		}
		// check hostname
		o = obj.get("hostname");
		if (o == null) {
			sendMsg(con, "INVALID_MESSAGE", "Missing hostname");
			return true;
		}
		// check port number
		o = obj.get("port");
		if (o == null) {
			sendMsg(con, "INVALID_MESSAGE", "Missing port");
			return true;
		}

		if (serverIDs.indexOf(obj.get("id").toString()) == -1) {
			// receive new server id, add info to local storage
			serverIDs.add(obj.get("id").toString());
			load.add(Integer.parseInt(obj.get("load").toString()));
			hostName.add(obj.get("hostname").toString());
			hostPort.add(Integer.parseInt(obj.get("port").toString()));
		} else {
			// known server id, update info to the local storage
			load.set(serverIDs.indexOf(obj.get("id").toString()),
				Integer.parseInt(obj.get("load").toString()));
			hostName.set(serverIDs.indexOf(obj.get("id").toString()),
				obj.get("hostname").toString());
			hostPort.set(serverIDs.indexOf(obj.get("id").toString()),
				Integer.parseInt(obj.get("port").toString()));
		}
		// broadcast the receiving server announce
		for (int i = 0; i < servers.size(); i++)
			if (servers.get(i) != con) {
				servers.get(i).writeMsg(obj.toString());
			}
		return false;
	}

	/*
	 * Called when a activity broadcast arrive
	 */
	private boolean activityBroadcast(Connection con, JSONObject obj) {
		if (servers.indexOf(con) == -1) {
			// unauthenticated server
			sendMsg(con, "INVALID_MESSAGE", "unauthenticated server");
			return true;
		}
		// check activity
		Object o = obj.get("activity");
		if (o == null) {
			// missing activity
			sendMsg(con, "INVALID_MESSAGE", "Missing activity");
			return true;
		}
		// broadcast the activity
		for (int i = 0; i < this.servers.size(); i++)
			if (this.servers.get(i) != con)
				servers.get(i).writeMsg(obj.toString());
		for (int i = 0; i < this.clients.size(); i++)
			clients.get(i).writeMsg(obj.toString());
		return false;
	}

	/*
	 * Called when a register message arrive
	 */
	private boolean register(Connection con, JSONObject obj) {
		if (this.clients.indexOf(con) != -1) {
			// user has already logged
			sendMsg(con, "INVALID_MESSAGE", "user has already logged");
			return true;
		}
		// check username and secret
		Object user = obj.get("username");
		Object secret = obj.get("secret");
		if (user == null || secret == null) {
			// missing username or secret
			sendMsg(con, "INVALID_MESSAGE", "Missing username or secret");
			return true;
		}

		if (usernames.indexOf(user.toString()) != -1) {
			// usernames has already been known by the server
			sendMsg(con, "REGISTER_FAILED", user.toString()
				+ " is already registered with the system");
			return true;
		}

		if (serverIDs.size() == 0) {
			// no other servers in the network, register success, add info to
			// local storage
			usernames.add(user.toString());
			secrets.add(secret.toString());
			this.sendMsg(con, "REGISTER_SUCCESS",
				"register success for " + user.toString());
		}

		// add to register list
		this.registerUser.add(user.toString());
		this.register.add(con);
		ArrayList<String> flag = new ArrayList<String>();
		for (int i = 0; i < serverIDs.size(); i++)
			flag.add(serverIDs.get(i));
		this.registerFlag.add(flag);
		// broadcast the lock request.
		JSONObject o = new JSONObject();
		o.put("command", "LOCK_REQUEST");
		o.put("username", obj.get("username"));
		o.put("secret", obj.get("secret"));
		for (int i = 0; i < this.servers.size(); i++) {
			this.servers.get(i).writeMsg(o.toString());
		}
		return false;
	}

	/*
	 * Called when a lock request arrive
	 */
	private boolean lockRequest(Connection con, JSONObject obj) {
		if (this.servers.indexOf(con) == -1) {
			// unauthenticated server
			sendMsg(con, "INVALID_MESSAGE", "unauthenticated server");
			return true;
		}
		// check username and secret
		Object user = obj.get("username");
		Object secret = obj.get("secret");
		if (user == null || secret == null) {
			// mising username or secret
			sendMsg(con, "INVALID_MESSAGE", "Missing username or secret");
			return true;
		}

		JSONObject o = new JSONObject();
		if (usernames.indexOf(user.toString()) != -1) {
			// username has been already known
			o.put("command", "LOCK_DENIED");
			o.put("username", obj.get("username"));
			o.put("secret", obj.get("secret"));
		} else {
			// new username
			o.put("command", "LOCK_ALLOWED");
			o.put("username", obj.get("username"));
			o.put("secret", obj.get("secret"));
			o.put("server", this.id);
			// add new info to local storage
			usernames.add(user.toString());
			secrets.add(secret.toString());
		}
		// broadcast the lock request
		for (int i = 0; i < this.servers.size(); i++)
			if (this.servers.get(i) != con)
				servers.get(i).writeMsg(obj.toString());
		// broadcast the lock allowed or denied
		for (int i = 0; i < this.servers.size(); i++) {
			servers.get(i).writeMsg(o.toString());
		}
		return false;
	}

	/*
	 * Called when a lock allowed message arrive
	 */
	private boolean lockAllowed(Connection con, JSONObject obj) {
		if (this.servers.indexOf(con) == -1) {
			// unauthenticated server
			sendMsg(con, "INVALID_MESSAGE", "unauthenticated server");
			return true;
		}
		// check info
		Object user = obj.get("username");
		Object secret = obj.get("secret");
		Object server = obj.get("server");
		if (user == null || secret == null || server == null) {
			// missing some of the info
			sendMsg(con, "INVALID_MESSAGE", "Missing information");
			return true;
		}
		// check in register list
		if (this.registerUser.indexOf(user.toString()) != -1) {
			// the username is saved in the register list, modify the flag
			this.registerFlag
				.get(this.registerUser.indexOf(user.toString()))
				.remove(server.toString());
			if (this.registerFlag
				.get(this.registerUser.indexOf(user.toString()))
				.isEmpty()) {
				// all servers reply lock allowed, register success, add info to
				// local storage
				usernames.add(user.toString());
				secrets.add(secret.toString());
				sendMsg(
					this.register
						.get(this.registerUser.indexOf(user.toString())),
					"REGISTER_SUCCESS",
					"register success for " + user.toString());
				// delete user from register list
				this.register
					.remove(this.registerUser.indexOf(user.toString()));
				this.registerFlag
					.remove(this.registerUser.indexOf(user.toString()));
				this.registerUser
					.remove(this.registerUser.indexOf(user.toString()));
			}
		} else {
			// if not in the register list, broadcast the lock allowed
			for (int i = 0; i < this.servers.size(); i++)
				if (this.servers.get(i) != con)
					servers.get(i).writeMsg(obj.toString());
		}
		return false;
	}

	/*
	 * Called when a lock denied message arrive
	 */
	private boolean lockDenied(Connection con, JSONObject obj) {
		if (this.servers.indexOf(con) == -1) {
			// unauthenticated server
			sendMsg(con, "INVALID_MESSAGE", "unauthenticated server");
			return true;
		}
		// check username and secret
		Object user = obj.get("username");
		Object secret = obj.get("secret");
		if (user == null || secret == null) {
			// missing username or secret
			sendMsg(con, "INVALID_MESSAGE", "Missing username or secret");
			return true;
		}

		/// check the register list
		if (this.registerUser.indexOf(user.toString()) != -1) {
			// username in the register list
			sendMsg(
				this.register
					.get(this.registerUser.indexOf(user.toString())),
				"REGISTER_FAILED", user.toString()
					+ " is already registered with the system");
			// close the connection for register fail
			this.register.get(this.registerUser.indexOf(user.toString()))
				.closeCon();
			// delete user from register list
			this.register
				.remove(this.registerUser.indexOf(user.toString()));
			this.registerFlag
				.remove(this.registerUser.indexOf(user.toString()));
			this.registerUser
				.remove(this.registerUser.indexOf(user.toString()));
		}

		/// if not in the register list
		else {
			/// check local storage, delete the same data
			int key = usernames.indexOf(user.toString());
			if (key != -1)
				if (secrets.get(key).equals(secret.toString())) {
					secrets.remove(key);
					usernames.remove(key);
				}
			// broadcast lock denied
			for (int i = 0; i < this.servers.size(); i++)
				if (this.servers.get(i) != con)
					servers.get(i).writeMsg(obj.toString());
		}
		return false;
	}

	/*
	 * Called when a redirection may need to be done
	 */
	private boolean redirect(Connection con, Object user, Object secret) {
		int target = this.clients.size();
		// search the server with minimum load
		int minIndex = load.indexOf(Collections.min(load));
		// check if the minimum load is at least 2 less than the current server
		if (load.get(minIndex) <= target - 2) {
			// if so, send redirect message
			JSONObject obj = new JSONObject();
			obj.put("command", "REDIRECT");
			obj.put("hostname", hostName.get(minIndex));
			obj.put("port", hostPort.get(minIndex));
			con.writeMsg(obj.toString());
			return true;
		} else {
			// if not, the user log in the current server
			clients.add(con);
			loggedUser.add(user.toString());
			loggedSecret.add(secret.toString());
			return false;
		}
	}

	/*
	 * Called when a message is needed to be sent
	 */
	private void sendMsg(Connection con, String command, String info) {
		JSONObject obj = new JSONObject();
		obj.put("command", command);
		obj.put("info", info);
		con.writeMsg(obj.toString());
	}

}
