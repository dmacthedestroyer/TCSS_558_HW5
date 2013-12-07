package gui;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.Timer;

import util.Log;
import chord.NodeState;
import chord.RMINodeState;

/**
 * A really crude, fragile GUI to enable you to poke around inside a Chord
 * network. There is absolutely no error handling anywhere. If you're reading
 * this, you know what code is, so just read the stack trace or the code to
 * figure out what's going on or how to use this.
 * 
 * @author dmac
 * 
 */
public class Main extends JPanel {

	/**
	 * Eclipse whined about this. I don't know what it is. STFU, Eclipse.
	 */
	private static final long serialVersionUID = 967889003587846649L;

	/**
	 * Create the GUI and show it. For thread safety, this method should be
	 * invoked from the event-dispatching thread.
	 */
	private static void createAndShowGUI(String[] args) {
		if (args.length < 2)
			Log.err("usage: java Main <registryAddress> <registryPort> [initialHashLength] [joinNodeKey] ... [joinNodeKey]");

		String host = args[0];
		int port = Integer.parseInt(args[1]);

		// Create and set up the window.
		JFrame frame = new JFrame("Chord node visualization");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		// Create and set up the content pane.
		Main newContentPane = new Main(host, port);
		newContentPane.setOpaque(true); // content panes must be opaque
		frame.setContentPane(newContentPane);

		// Display the window.
		frame.pack();
		frame.setVisible(true);

		if (args.length > 3) {
			newContentPane.controller.seedNetwork(Integer.parseInt(args[2]), Long.parseLong(args[3]));
			for (int i = 4; i < args.length; i++) {
				newContentPane.controller.addNode(Long.parseLong(args[i]));
			}
		}
	}

	public static void main(final String[] args) {
		// Schedule a job for the event-dispatching thread:
		// creating and showing this application's GUI.
		javax.swing.SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				createAndShowGUI(args);
			}
		});
	}

	private String host;
	private int port;
	private ChordNetworkTable chordNetworkTable = new ChordNetworkTable();
	private final ChordNetworkController controller;

	private Timer poller = new Timer(1000, new ActionListener() {

		@Override
		public void actionPerformed(ActionEvent arg0) {
			chordNetworkTable.update(getNetworkState());
		}
	});

	public Main(String host, int port) {
		super(new BorderLayout(15, 15));
		this.host = host;
		this.port = port;

		add(new JScrollPane(chordNetworkTable), BorderLayout.CENTER);
		add((controller = new ChordNetworkController(host, port)), BorderLayout.SOUTH);
		poller.start();
	}

	/**
	 * look up all items in the registry and get their node state. This is a
	 * really cheap way to find the state of a network, but this whole thing is
	 * meant to quickly get info out, so I'm not worried about cheating
	 * 
	 * @return
	 */
	private List<NodeState> getNetworkState() {
		Registry registry;
		try {
			registry = LocateRegistry.getRegistry(host, port);
		} catch (RemoteException e) {
			return new ArrayList<NodeState>();
		}

		try {
			ArrayList<NodeState> states = new ArrayList<>();
			try {
				String[] names = registry.list();
				for (String name : names) {
					try {
						RMINodeState state = (RMINodeState) registry.lookup(name);
						states.add(state.getState());
					} catch (RemoteException re) {
						registry.unbind(name);
					}
				}
			} catch (ConnectException ce) {
				return new ArrayList<NodeState>();
			} catch (java.rmi.ConnectIOException cioe) {
				cioe.printStackTrace();
				poller.stop();
			}

			java.util.Collections.sort(states, new Comparator<NodeState>() {
				@Override
				public int compare(NodeState o1, NodeState o2) {
					return (int) (o1.getKey() - o2.getKey());
				}
			});

			return states;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
