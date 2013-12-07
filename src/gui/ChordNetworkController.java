package gui;

import gui.util.JAutoSubscribeActionButton;

import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Random;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JSeparator;
import javax.swing.JTextField;
import javax.swing.SwingConstants;

import chord.RMINode;
import chord.RMINodeServer;

/**
 * GUI component for manipulating a chord network. Contains the ability to
 * initiate a network, add/remove nodes, and add/remove/get values from a
 * network. This class really quick 'n dirty, with absolutely no manual or error
 * checking. Use at your own risk.
 * 
 * @author dmac
 * 
 */
public class ChordNetworkController extends JPanel {
	private String host;
	private int port;

	// id of node that should create a new network
	private JTextField txtStartNodeId = new JTextField(10);
	// the 'm' value of the network, which determines the number of fingers each
	// node will have and, by extension, what the keyspace is
	private JTextField txtStartNodeM = new JTextField(3);

	// id of a node that should be added to the network
	private JTextField txtAddNodeId = new JTextField(10);
	// if of a node that should be removed from the network
	private JTextField txtRemoveNodeId = new JTextField(10);

	// id of a node that should be called for adding/removing/getting values from
	// the network
	private JTextField txtValueNodeId = new JTextField(10);
	// the key of where the value should be stored for looking up/adding/deleting
	// values
	private JTextField txtValueId = new JTextField(10);

	public ChordNetworkController(final String host, final int port) {
		super(new FlowLayout());
		this.host = host;
		this.port = port;

		add(new JLabel("node id:"));
		add(txtStartNodeId);
		add(new JLabel("m:"));
		add(txtStartNodeM);
		add(new JAutoSubscribeActionButton("seed network", new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				seedNetwork();
			}
		}));

		add(new JSeparator(SwingConstants.VERTICAL));

		add(txtAddNodeId);
		txtAddNodeId.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				addNode();

			}
		});
		add(new JAutoSubscribeActionButton("add node", new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				addNode();
			}
		}));

		add(new JSeparator());

		add(txtRemoveNodeId);
		txtRemoveNodeId.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				removeNode();
			}
		});
		add(new JAutoSubscribeActionButton("remove node", new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				removeNode();
			}
		}));

		add(new JSeparator());

		add(new JLabel("node:"));
		add(txtValueNodeId);

		add(new JLabel("value key:"));
		add(txtValueId);

		add(new JAutoSubscribeActionButton("put", new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent arg0) {
				putValue();
			}
		}));
		add(new JAutoSubscribeActionButton("delete", new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				deleteValue();
			}
		}));
		add(new JAutoSubscribeActionButton("get", new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				getValue();
			}
		}));
	}

	/**
	 * get a random node from the RMI network
	 * 
	 * @return
	 */
	private RMINodeServer getRandomNode() {
		try {
			String[] nodeList = LocateRegistry.getRegistry(host, port).list();
			return getNode("" + nodeList[new Random().nextInt(nodeList.length)]);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * get the node with the specified nodeId from the RMI network
	 * 
	 * @param nodeId
	 * @return
	 */
	private RMINodeServer getNode(String nodeId) {
		try {
			return (RMINodeServer) LocateRegistry.getRegistry(host, port).lookup(nodeId);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * create a new Chord network
	 */

	private void seedNetwork() {
		seedNetwork(Integer.parseInt(txtStartNodeM.getText()), Long.parseLong(txtStartNodeId.getText()));
	}
	
	public void seedNetwork(int m, long nodeId){
		try {
			RMINode node = new RMINode(m, nodeId);
			LocateRegistry.createRegistry(port).bind("" + node.getNodeKey(), UnicastRemoteObject.exportObject(node, 0));
			node.join(null);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/**
	 * add a new node to the network
	 */
	private void addNode() {
		addNode(Long.parseLong(txtAddNodeId.getText()));
	}
	
	public void addNode(long nodeId) {
		try {
			RMINodeServer fromNetwork = getRandomNode();
			if (fromNetwork == null)
				return;

			RMINode node = new RMINode(fromNetwork.getHashLength(), nodeId);
			LocateRegistry.getRegistry(host, port).bind("" + node.getNodeKey(), UnicastRemoteObject.exportObject(node, 0));
			node.join(fromNetwork);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		txtAddNodeId.grabFocus();
		txtAddNodeId.selectAll();
	}

	/**
	 * remove a node from the network
	 */
	private void removeNode() {
		try {
			((RMINodeServer) LocateRegistry.getRegistry(host, port).lookup(txtRemoveNodeId.getText())).leave();
		} catch (Exception e) {
			e.printStackTrace();
		}

		txtRemoveNodeId.grabFocus();
		txtRemoveNodeId.selectAll();
	}
	
	/**
	 * place a value in the network. The value will just be the string
	 * representation of the key that the value should go in.
	 */
	private void putValue() {
		try {
			getNode(txtValueNodeId.getText()).put(Long.parseLong(txtValueId.getText()), txtValueId.getText());
		} catch (NumberFormatException | RemoteException e) {
			e.printStackTrace();
		}
	}

	/**
	 * delete the value provided at specified key
	 */
	private void deleteValue() {
		try {
			getNode(txtValueNodeId.getText()).delete(Long.parseLong(txtValueId.getText()));
		} catch (NumberFormatException | RemoteException e) {
			e.printStackTrace();
		}
	}

	/**
	 * get the value from the network with the specified key, and set the text of
	 * the txtValueId field with it
	 */
	private void getValue() {
		try {
			txtValueId.setText((String) getNode(txtValueNodeId.getText()).get(Long.parseLong(txtValueId.getText())));
		} catch (NumberFormatException | RemoteException e) {
			e.printStackTrace();
		}
	}
}
