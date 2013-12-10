Instructions for running the GUI and viewing backups:
1. Run the gui.Main.java class.
- usage: java Main <registryAddress> <registryPort> [initialHashLength] [joinNodeKey] ... [joinNodeKey]
2. Use the text boxes to seed the network.
- Add a node id, which adds a node to the network
- Add an m value for the size of the hashtable
3. Add additional nodes using the 'add node' text box.
4. Remove a node by providing the node id to remove in the 'remove node' text box.
5. Put, get, or delete values by adding a number in the last text box.
6. To see backups, add a value to a node; the node will back the value up to its predecessor (shown in the GUI).