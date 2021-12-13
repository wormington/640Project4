package studentCode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.openflow.protocol.OFMatch;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.instruction.OFInstruction;
import org.openflow.protocol.instruction.OFInstructionApplyActions;

import edu.wisc.cs.sdn.apps.l3routing.L3Routing;
import edu.wisc.cs.sdn.apps.util.Host;
import edu.wisc.cs.sdn.apps.util.SwitchCommands;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.routing.Link;
import studentCode.DijkstraList.DijkstraNode;

public class RuleGenerator {
	
	private IOFSwitch sw;
	private DijkstraList paths;
	private Collection<Link> links;
	
	private Collection<OFMatch> l3Rules;
	
	
	public RuleGenerator(IOFSwitch sw, DijkstraList pathList, Collection<Link> linkList) {
		this.sw = sw;
		this.paths = pathList;
		this.links = linkList;
		l3Rules = new ArrayList<OFMatch>();
	}
	
	public void updateLinks(Collection<Link> newLinks) {
		this.links = newLinks;
	}
	
	public void addRuleRouteIP(Host h) {
		if (!h.isAttachedToSwitch()) {
			// host is not attached to switch, do not make a rule
			return;
		}
		
		// Create an OFMatch object which matches IPv4 packets with a Host's IP
		OFMatch temp = new OFMatch();
		temp.setDataLayerType(OFMatch.ETH_TYPE_IPV4);
		temp.setNetworkDestination(h.getIPv4Address());
		
		// Track the rules we add
		l3Rules.add(temp);
		
		int targetPort = -1;
		IOFSwitch targetSw = h.getSwitch();
		
		if (targetSw.equals(sw)) {
			// the host is connected to this switch. add a rule to route to host port.
			targetPort = h.getPort();
		} else {
			// the host is connected to another switch. Use the given DijkstraList to determine
			// routing for this host.
			Map<Long, DijkstraNode> pathMap = paths.getPaths().get(sw);
			long currentSwID = targetSw.getId();
			if (pathMap == null) {
				System.out.println("WARNING: pathMap NULL!");
				return;
			} else if (pathMap.get(currentSwID) == null) {
				System.out.println("WARNING: pathMap.get() NULL!");
				return;
			} else if (pathMap.get(currentSwID).getPrevSwitch() == null) {
				System.out.println("WARNING: pathMap.get().getPrevSwitch() NULL!");
				return;
			} else if (sw == null) {
				System.out.println("WARNING: sw NULL!");
				return;
			}
			while (!pathMap.get(currentSwID).getPrevSwitch().equals(sw)) {
				currentSwID = pathMap.get(currentSwID).getPrevSwitch().getId();
			}
			// after this loop, currentSwID contains the ID of the next switch in the path to this host.
			
			// Find the link between our switch and the target switch to determine the right port to send the 
			// packet out of.
			System.out.println("DEBUG: Searching for link between Sw: " + sw.getId() + " and Sw: " + currentSwID);
			for (Link l : links) {
				if (l.getSrc() == sw.getId() && l.getDst() == currentSwID) {
					targetPort = l.getSrcPort();
					break;
				} else if (l.getDst() == sw.getId() && l.getSrc() == currentSwID) {
					targetPort = l.getDstPort();
					break;
				}
			}
		}
		
		// After we find our port, construct our action to execute.
		OFActionOutput outPort = new OFActionOutput(targetPort);
		ArrayList<OFAction> actList = new ArrayList<OFAction>();
		actList.add(outPort);
		OFInstructionApplyActions instruction = new OFInstructionApplyActions(actList);
		ArrayList<OFInstruction> instrList = new ArrayList<OFInstruction>();
		instrList.add(instruction);
		
		System.out.println("DEBUG: Adding rule to Switch: " + sw.getId() + " for Host: " + h.getIPv4Address() + " at Port: " + outPort);
		SwitchCommands.installRule(sw, L3Routing.table, SwitchCommands.DEFAULT_PRIORITY, temp, instrList);
	}
	
	public void removeRuleRouteIP(Host h) {
		// Create the rule that we need to remove.
		OFMatch temp = new OFMatch();
		temp.setDataLayerType(OFMatch.ETH_TYPE_IPV4);
		temp.setNetworkDestination(h.getIPv4Address());
		
		// Remove the rule from our tracked list.
		l3Rules.remove(temp);
		
		// Use the SwitchCommands class to remove the rule from the table.
		
		if(SwitchCommands.removeRules(sw, L3Routing.table, temp)) {
			System.out.println("DEBUG: Removing rule from Switch: " + sw.getId() + " for Host: " + h.getIPv4Address());
		}
	}
	
	public void reset() {
		if (l3Rules.isEmpty()) {
			return;
		}
		
		int count = 0;
		for (OFMatch m : l3Rules) {
			SwitchCommands.removeRules(sw, L3Routing.table, m);
			count += 1;
		}
		
		l3Rules.clear();
		
		System.out.println("DEBUG: Removed " + count + " rules from Switch: " + sw.getId());
	}
}
