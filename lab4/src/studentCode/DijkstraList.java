package studentCode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.wisc.cs.sdn.apps.util.Host;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.routing.Link;

/**
 * The DijkstraList class is used to produce a Dijkstra table for each switch, so that routing
 * is done efficiently. Given a list of hosts, switches, and links, it will determine the shortest path
 * to all other switches from each switch. 
 * 
 * After constructing a DijkstraList object, computePaths must be called to run the algorithm.
 * 
 * @author cworm
 *
 */
public class DijkstraList {
	
	private Map<IOFSwitch, Map<Long, DijkstraNode>> hostPaths;
	private Collection<Host> hosts;
	private Collection<IOFSwitch> switches;
	private Collection<Link> links;
	
	/**
	 * Constructor for a DijkstraList.
	 * 
	 * @param hostList - The list of hosts in the network.
	 * @param switchList - The list of switches in the network.
	 * @param linkList - The list of links in the network.
	 */
	public DijkstraList(Collection<Host> hostList, Collection<IOFSwitch> switchList, Collection<Link> linkList) {
		this.hostPaths = new HashMap<IOFSwitch, Map<Long, DijkstraNode>>();
		
		this.hosts = hostList;
		this.switches = switchList;
		this.links = linkList;
		
		System.out.println("DEBUG: Init DijkstraList object with " + hosts.size() + " hosts, " + switches.size() + " switches, " +
				links.size() + " links.");
	}
	
	/**
	 * Sets the list of hosts for this DijkstraList object.
	 * 
	 * @param hostList - The new list of hosts to set.
	 */
	public void setHosts(Collection<Host> hostList) {
		System.out.println("DEBUG: Updated DijkstraList with " + hostList.size() + " hosts.");
		this.hosts = hostList;
	}
	
	/**
	 * Sets the list of switches for this DijkstraList object.
	 * 
	 * @param switchList - The new list of switches to set.
	 */
	public void setSwitches(Collection<IOFSwitch> switchList) {
		System.out.println("DEBUG: Updated DijkstraList with " + switchList.size() + " switches.");
		this.switches = switchList;
	}
	
	/**
	 * Sets the list of links for this DijkstraList object.
	 * 
	 * @param linkList - The new list of links to set.
	 */
	public void setLinks(Collection<Link> linkList) {
		System.out.println("DEBUG: Updated DijkstraList with " + linkList.size() + " links.");
		this.links = linkList;
	}
	
	/**
	 * Returns the Map containing each switch and the paths to all other switches.
	 * Can return an empty Map if DijkstraList is passed bad parameters or computePaths() has
	 * not been run.
	 * @return - The Map of switches and paths.
	 */
	public Map<IOFSwitch, Map<Long, DijkstraNode>> getPaths() {
		return this.hostPaths;
	}
	
	/**
	 * Performs Dijkstra's Shortest Path algorithm to determine the most efficient way to route
	 * packets in the network. Returns the Map of switches and paths after it finishes.
	 * 
	 * @return - The Map of switches and paths.
	 */
	public Map<IOFSwitch, Map<Long, DijkstraNode>> computePaths() {
		
		// Must compute a Dijkstra table for each switch, so iterate through each switch.
		for (IOFSwitch sw : switches) {
			// Init the Map which we will use for a Dijkstra table and the list of visited nodes.
			Map<Long, DijkstraNode> mapForSw = new HashMap<Long, DijkstraNode>();
			ArrayList<IOFSwitch> visitedSwitches = new ArrayList<IOFSwitch>();
			
			// Iterate through all switches to add them to the Dijkstra table.
			for (IOFSwitch sw1 : switches) {
				mapForSw.put(sw1.getId(), new DijkstraNode(sw1, Integer.MAX_VALUE, null));
				
				// If the switch we are on is the same as the one we are calculating the table for,
				// we set the distance to 0.
				if (sw1.equals(sw)) {
					mapForSw.get(sw1.getId()).setDist(0);
				}
			}
			
//			System.out.println("DEBUG: Dump DijkstraList for Sw: " + sw.getId());
//			for (DijkstraNode o : mapForSw.values()) {
//				String prevStr = "";
//				if (o.getPrevSwitch() == null)
//					prevStr = "NULL";
//				else
//					prevStr = "" + o.getPrevSwitch().getId();
//				System.out.println("DEBUG: Node: " + o.getDest().getStringId() + "\t" + o.getDist() + "\t" + prevStr);
//			}
			
			// Declares and sets the current switch we are visiting in Dijkstra's algorithm.
			IOFSwitch currentSw = sw;
	
			// Continues the algorithm while all switches have not been visited.
			while (!visitedSwitches.containsAll(switches)) {
				
				// Iterates through all links to find the switches which currentSw is connected to.
				Iterator<Link> e = links.iterator();
				while (e.hasNext()) {
					Link currentLink = e.next();
					
					// Find a link which is connected to currentSw and is not visited yet. If the link shows a
					// shorter path than the one currently recorded, update its distance and prevSwitch fields.
					if (sw == null) {
						System.out.println("WARNING: sw NULL in Dijkstra");
						return null;
					} else if (currentSw == null) {
						System.out.println("WARNING: currentSw NULL in Dijkstra");
						return null;
					} else if (currentLink == null) {
						System.out.println("WARNING: pathMap NULL!");
						return null;
					}
					
					if (currentLink.getSrc() == currentSw.getId() 
							&& !visitedSwitches.contains(mapForSw.get(currentLink.getDst()).getDest()) &&
							mapForSw.get(currentSw.getId()).getDist() + 1 < mapForSw.get(currentLink.getDst()).getDist()) {
						mapForSw.get(currentLink.getDst()).setPrevSwitch(currentSw);
						mapForSw.get(currentLink.getDst()).setDist(mapForSw.get(currentSw.getId()).getDist() + 1);
					}
				}
				
				// Now that we have checked all paths from currentSw, we add it to visitedSwitches.
				visitedSwitches.add(currentSw);
				
				// Determine the next switch to move to.
				// Iterate through all DijkstraNodes in this switch's table and choose the unvisited one
				// with the minimum distance from the starting switch.
				IOFSwitch nextSw = null;
				int minDist = Integer.MAX_VALUE;
				for (DijkstraNode tempNode : mapForSw.values()) {
					if (!visitedSwitches.contains(tempNode.getDest()) && tempNode.getDist() < minDist) {
						minDist = tempNode.getDist();
						nextSw = tempNode.getDest();
					}
				}
				
				// Check if we have actually found another switch to travel to.
				if (nextSw != null) {
					// In this case, the algorithm has not yet finished and we must check paths from another
					// switch.
					currentSw = nextSw;
				} else if (visitedSwitches.containsAll(switches)) {
					// Nothing wrong here, all switches visited, so no next switch. Outer while loop will end.
				} else {
					// Something wrong, we should have an unvisited switch to visit.
					System.out.println("WARNING: Incomplete DijkstraList encountered.");
					return null;
				}
				
//				System.out.println("DEBUG: Dump DijkstraList for Sw: " + sw.getId());
//				for (DijkstraNode o : mapForSw.values()) {
//					String prevStr = "";
//					if (o.getPrevSwitch() == null)
//						prevStr = "NULL";
//					else
//						prevStr = "" + o.getPrevSwitch().getId();
//					System.out.println("DEBUG: Node: " + o.getDest().getStringId() + "\t" + o.getDist() + "\t" + prevStr);
//				}
			}
			
			// Finally put the Dijkstra path Map for this particular switch into the hostPaths Map with the 
			// starting switch as a key.
			System.out.println("DEBUG: Computed DijkstraList for switchID: " + sw.getId());
			hostPaths.put(sw, mapForSw);
		}
		
		// After the algorithm finishes, return all Dijkstra tables created.
		return this.hostPaths;
	}
	
	/**
	 * A helper class for DijkstraList. It provides a data structure which is easy to use and 
	 * manipulate when performing Dijkstra's Shortest Path algorithm.
	 * 
	 * @author cworm
	 *
	 */
	class DijkstraNode {
		private IOFSwitch destination;
		private IOFSwitch prevSwitch;
		private int distance;
		private boolean init;
		
		/**
		 * Default constructor for a DijkstraNode. Inits all fields to null, false, or -1;
		 */
		DijkstraNode() {
			this.destination = null;
			this.prevSwitch = null;
			this.distance = -1;
			this.init = false;
		}
		
		/**
		 * Constructor for a DijkstraNode which initializes fields to the parameters given.
		 * 
		 * @param dest - The IOFSwitch which is the destination of this entry.
		 * @param dist - The distance from the starting IOFSwitch to the destination IOFSwitch.
		 * @param from - The previous IOFSwitch that is traversed through to reach the destination IOFSwitch.
		 */
		DijkstraNode(IOFSwitch dest, int dist, IOFSwitch from) {
			this.destination = dest;
			this.distance = dist;
			this.prevSwitch = from;
			
			if (this.destination != null && this.distance != -1) {
				this.init = true;
			} else {
				this.init = false;
			}
		}
		
		/**
		 * Returns the destination IOFSwitch in this DijkstraNode.
		 * 
		 * @return - The destination switch.
		 */
		IOFSwitch getDest() {
			return this.destination;
		}
		
		/**
		 * Returns the distance from the starting IOFSwitch to the destination IOFSwitch.
		 * @return
		 */
		int getDist() {
			return this.distance;
		}
		
		/**
		 * Returns the previous IOFSwitch traversed between the destination IOFSwitch and the starting IOFSwitch.
		 * 
		 * @return - The previous switch.
		 */
		IOFSwitch getPrevSwitch() {
			return this.prevSwitch;
		}
		
		/**
		 * Checks if the needed fields of this DijkstraNode have been properly set to a value.
		 * 
		 * @return - true if fields have valid values, false otherwise.
		 */
		boolean isInit() {
			return this.init;
		}
		
		/**
		 * Sets the destination field of this DijkstraNode.
		 * 
		 * @param dest - The new destination IOFSwitch to set.
		 */
		void setDest(IOFSwitch dest) {
			this.destination = dest;
			
			if (this.destination != null && this.distance != -1) {
				this.init = true;
			} else {
				this.init = false;
			}
		}
		
		/**
		 * Sets the distance field of this DijkstraNode.
		 * 
		 * @param dist - The new distance to set.
		 */
		void setDist(int dist) {
			this.distance = dist;
			
			if (this.destination != null && this.distance != -1) {
				this.init = true;
			} else {
				this.init = false;
			}
		}
		
		/**
		 * Sets the previous IOFSwitch of this DijkstraNode.
		 * 
		 * @param from - The new previous IOFSwitch to set.
		 */
		void setPrevSwitch(IOFSwitch from) {
			this.prevSwitch = from;
			
			if (this.destination != null && this.distance != -1) {
				this.init = true;
			} else {
				this.init = false;
			}
		}
		
		/**
		 * Tests equality between the fields of a DijkstraNode.
		 * 
		 * @param node - The DijkstraNode to test against.
		 * @return - true if the fields of the two nodes are the same, false otherwise.
		 */
		boolean equals(DijkstraNode node) {
			return this.destination.equals(node.destination) && this.prevSwitch.equals(node.prevSwitch) &&
						this.distance == node.distance;
		}
	}
}
