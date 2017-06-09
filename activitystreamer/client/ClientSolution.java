package activitystreamer.client;

import java.awt.event.WindowAdapter;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.sun.glass.events.WindowEvent;

import activitystreamer.server.Connection;
import activitystreamer.server.Control;
import activitystreamer.util.Settings;

public class ClientSolution extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ClientSolution clientSolution;
	private TextFrame textFrame;
	
	/*
	 * additional variables
	 */
	
	//info for client
	JSONParser parser = new JSONParser();
	private String username;
	private String secret;
	private Socket socket;
	private BufferedReader inreader;
	private OutputStreamWriter outwriter;
	private boolean term = false;
	private boolean isLog = false;
	
	// this is a singleton object
	public static ClientSolution getInstance() {
		if (clientSolution == null) {
			clientSolution = new ClientSolution();
		}
		return clientSolution;
	}

	public ClientSolution() {
		/*
		 * some additional initialization
		 */
		////////////////////////////////////
		username = Settings.getUsername();
		secret = Settings.getSecret();
		
		try {
			//establish the connection
			socket = new Socket(new String(), Settings.getRemotePort());
			this.outwriter = new OutputStreamWriter(
				socket.getOutputStream(), "UTF-8");
			this.inreader = new BufferedReader(
				new InputStreamReader(socket.getInputStream(), "UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		JSONObject obj = new JSONObject();
		//check username
		if(!this.username.equals("anonymous") && this.secret == null){
			//if it is not anonymous, and no secret is given in arguement
			//client should be set a secret first, then send register message
			this.secret = Settings.nextSecret();
			obj.put("command", "REGISTER");
			obj.put("username", this.username);
			obj.put("secret", this.secret);
			this.sendActivityObject(obj);
			String msg;
			try {
				//get reply
				while ((msg = inreader.readLine()) == null);
				JSONObject o;
				o = (JSONObject) parser.parse(msg);
				Object command = o.get("command");
				if (command == null) {
					//missing command
					log.info("INVALID_MESSAGE");
					sendMsg("INVALID_MESSAGE",
						"the received message did not contain a command");
					//close connection
					this.closeConnection(false);
				} else {
					switch(command.toString()){
					//register fails,
					case "REGISTER_FAILED":
						log.info(o.get("info").toString());
						//close the connection
						this.closeConnection(false);
						break;
					//register success
					case "REGISTER_SUCCESS":
						log.debug("opening the gui");
						log.info(o.get("info").toString());
						log.info("new login secret: " + this.secret);
						//send login command
						obj.replace("command", "LOGIN");
						this.sendActivityObject(obj);
						//show the GUI
						textFrame = new TextFrame();
						start();
						break;
						default:
							//receive invalid command
							log.info("receiving invalid command");
							this.closeConnection(false);
					}
				}
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				sendMsg("INVALID_MESSAGE",
					"JSON parse error while parsing message");
				e.printStackTrace();
			}
			
		}
		else{
			//send login command
			obj.put("command", "LOGIN");
			obj.put("username", this.username);
			obj.put("secret", (this.secret == null)? "":this.secret);
			this.sendActivityObject(obj);
			String msg;
			try {
				//get reply
				while ((msg = inreader.readLine()) == null);
				JSONObject o;
				o = (JSONObject) parser.parse(msg);
				Object command = o.get("command");
				if (command == null) {
					//missing command
					log.info("INVALID_MESSAGE");
					sendMsg("INVALID_MESSAGE",
						"the received message did not contain a command");
					//close connection
					this.closeConnection(false);
				} else {
					switch(command.toString()){
					case "LOGIN_SUCCESS":
						//login success
						log.debug("opening the gui");
						log.info(o.get("info").toString());
						//show the GUI
						textFrame = new TextFrame();
						start();
						break;
					case "LOGIN_FAILED":
						//login fails
						log.info(o.get("info").toString());
						//close connection
						this.closeConnection(false);
					}
				}
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				sendMsg("INVALID_MESSAGE",
					"JSON parse error while parsing message");
				e.printStackTrace();
			}
			
		}
		////////////////////////////////////
//		log.debug("opening the gui");
//		textFrame = new TextFrame();
//		// start the client's thread
//		start();
	}

	// called by the gui when the user clicks "send"
	public void sendActivityObject(JSONObject activityObj) {
		if (!term) {
			try {
				//send the JSON string to the server immediately
				this.outwriter.write(activityObj.toString() + "\n");
				this.outwriter.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/*
	 * Called by the gui when the user clicks disconnect
	 */
	public void disconnect() {
		textFrame.setVisible(false);
		/*
		 * other things to do
		 */
		sendMsg("LOGOUT", "");
		this.closeConnection(true);
	}
	
	// the client's run method, to receive messages
	@Override
	public void run() {
		try {
			String msg;
			while (!term && ((msg = inreader.readLine()) != null)) {
				//System.out.println(msg);
				JSONObject obj;
				obj = (JSONObject) parser.parse(msg);
				//check the command
				Object command = obj.get("command");
				if (command == null) {
					//missing command
					sendMsg("INVALID_MESSAGE",
						"the received message did not contain a command");
					//close connection
					disconnect();
				} else {
					switch (command.toString()) {
					case "CLOSE CONNECTION":
						log.info("Connect closed");
						this.closeConnection(true);
						break;
					case "INVALID_MESSAGE":
						log.info(obj.get("info").toString());
						this.closeConnection(true);
						break;
					case "AUTHENTICATION_FAIL":
						log.info(obj.get("info").toString());
						this.closeConnection(true);
						break;
					case "LOGIN_SUCCESS":
						if(!isLog){
							log.info(obj.get("info").toString());
							isLog = true;
						}
						break;
					case "REDIRECT":
						this.redirect(obj);
						break;
					case "ACTIVITY_BROADCAST":
						this.activityBroadcast(obj);
						break;
					default:
						sendMsg("INVALID_MESSAGE",
							"invalid command");
					}
				}
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
			sendMsg("INVALID_MESSAGE",
				"JSON parse error while parsing message");
		}
		
	}

	/*
	 * additional methods
	 */
	
	/*
	 * Called when a redirect message is received
	 */
	private void redirect(JSONObject obj) {
		Object name = obj.get("hostname");
		Object port = obj.get("port");
		//check the hostname and port
		if(name == null || port == null){
			//missing hostname or port
			log.info("missing hostname or port in redirect message");
			this.closeConnection(true);
			return;
		}
		this.closeConnection(false);
		term = false;
		try {
			//connect to new server by using the provided hostname and port
			socket = new Socket(name.toString(),
				Integer.parseInt(port.toString()));
			this.outwriter = new OutputStreamWriter(
				socket.getOutputStream(), "UTF-8");
			this.inreader = new BufferedReader(
				new InputStreamReader(socket.getInputStream(), "UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		//resend the login message
		JSONObject o = new JSONObject();
		o.put("command", "LOGIN");
		o.put("username", this.username);
		o.put("secret", this.secret);
		this.sendActivityObject(o);
	}
	
	/*
	 * Called when a activity broadcast message is received
	 */
	private void activityBroadcast(JSONObject obj) {
		//check the activity object
		Object activity = obj.get("activity");
		if(activity == null){
			//missing activity object
			log.info("missing activity object in activity broadcast");
			sendMsg("INVALID_MESSAGE",
				"missing activity object");
			this.closeConnection(true);
		}
		try {
			textFrame.setOutputText(
				(JSONObject) parser.parse(activity.toString()));
		} catch (ParseException e) {
			sendMsg("INVALID_MESSAGE",
				"JSON parse error while parsing message");
			e.printStackTrace();
		}

	}
	
	/*
	 * Called when a message is needed to be sent
	 */
	private void sendMsg(String command, String info) {
		//send message
		JSONObject obj = new JSONObject();
		obj.put("command", command);
		obj.put("info", info);
		this.sendActivityObject(obj);
	}
	
	/*
	 * Called when a connection is needed to be closed
	 */
	private void closeConnection(boolean disposeFrame) {
		//close the connection
		term = true;
		try {
			this.inreader.close();
			this.outwriter.close();
			this.socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if(disposeFrame == true)
			//dispose the frame
			this.textFrame.dispose();
	}
}
