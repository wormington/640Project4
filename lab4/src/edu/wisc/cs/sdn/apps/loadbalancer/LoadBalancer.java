package edu.wisc.cs.sdn.apps.loadbalancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFOXMFieldType;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFPort;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.action.OFActionSetField;
import org.openflow.protocol.instruction.OFInstruction;
import org.openflow.protocol.instruction.OFInstructionApplyActions;
import org.openflow.protocol.instruction.OFInstructionGotoTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.wisc.cs.sdn.apps.l3routing.L3Routing;
import edu.wisc.cs.sdn.apps.util.ArpServer;
import edu.wisc.cs.sdn.apps.util.SwitchCommands;
import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitch.PortChangeType;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.ImmutablePort;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.internal.DeviceManagerImpl;
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.util.MACAddress;

public class LoadBalancer implements IFloodlightModule, IOFSwitchListener,
		IOFMessageListener
{
	public static final String MODULE_NAME = LoadBalancer.class.getSimpleName();
	
	private static final byte TCP_FLAG_SYN = 0x02;
	
	private static final short IDLE_TIMEOUT = 20;
	
	// Interface to the logging system
    private static Logger log = LoggerFactory.getLogger(MODULE_NAME);
    
    // Interface to Floodlight core for interacting with connected switches
    private IFloodlightProviderService floodlightProv;
    
    // Interface to device manager service
    private IDeviceService deviceProv;
    
    // Switch table in which rules should be installed
    private byte table;
    
    // Set of virtual IPs and the load balancer instances they correspond with
    private Map<Integer,LoadBalancerInstance> instances;
    
    /**
     * Loads dependencies and initializes data structures.
     */
	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Initializing %s...", MODULE_NAME));
		
		// Obtain table number from config
		Map<String,String> config = context.getConfigParams(this);
        this.table = Byte.parseByte(config.get("table"));
        
        // Create instances from config
        this.instances = new HashMap<Integer,LoadBalancerInstance>();
        String[] instanceConfigs = config.get("instances").split(";");
        for (String instanceConfig : instanceConfigs)
        {
        	String[] configItems = instanceConfig.split(" ");
        	if (configItems.length != 3)
        	{ 
        		log.error("Ignoring bad instance config: " + instanceConfig);
        		continue;
        	}
        	LoadBalancerInstance instance = new LoadBalancerInstance(
        			configItems[0], configItems[1], configItems[2].split(","));
            this.instances.put(instance.getVirtualIP(), instance);
            log.info("Added load balancer instance: " + instance);
        }
        
		this.floodlightProv = context.getServiceImpl(
				IFloodlightProviderService.class);
        this.deviceProv = context.getServiceImpl(IDeviceService.class);
        
        /*********************************************************************/
        /* TODO: Initialize other class variables, if necessary              */
        /*********************************************************************/
	}

	/**
     * Subscribes to events and performs other startup tasks.
     */
	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Starting %s...", MODULE_NAME));
		this.floodlightProv.addOFSwitchListener(this);
		this.floodlightProv.addOFMessageListener(OFType.PACKET_IN, this);
		
		/*********************************************************************/
		/* TODO: Perform other tasks, if necessary                           */
		
		/*********************************************************************/
	}
	
	/**
     * Event handler called when a switch joins the network.
     * @param DPID for the switch
     */
	@Override
	public void switchAdded(long switchId) 
	{
		IOFSwitch sw = this.floodlightProv.getSwitch(switchId);
		log.info(String.format("Switch s%d added", switchId));
		
		
		
		/*********************************************************************/
		/* TODO: Install rules to send:                                      */
		/*       (1) packets from new connections to each virtual load       */
		/*       balancer IP to the controller                               */
		
		// Install rule, init connection with virtual IP //FIXME Priority
		//Data layer is ethernet. Network layer is ip
		
		// Set filter to all packets sent over TCP
		OFMatch tcp = new OFMatch();
		tcp.setDataLayerType(OFMatch.ETH_TYPE_IPV4); 
		tcp.setNetworkProtocol(OFMatch.IP_PROTO_TCP);
		
		OFMatch arp = new OFMatch();
		arp.setDataLayerType(OFMatch.ETH_TYPE_ARP); 
		
		// Iterate over all ips in map
		ArrayList<OFAction> actList = new ArrayList<OFAction>();
		actList.add(new OFActionOutput(OFPort.OFPP_CONTROLLER));
		OFInstructionApplyActions instruction = new OFInstructionApplyActions(actList);
		
		ArrayList<OFInstruction> instrList = new ArrayList<OFInstruction>();
		instrList.add(instruction);
		
		for (Integer ip : instances.keySet()) {
			tcp.setNetworkDestination(ip);
			arp.setNetworkDestination(ip);
			SwitchCommands.installRule(sw, this.table, (short)(SwitchCommands.DEFAULT_PRIORITY+1), tcp, instrList);
			/*       (2) ARP packets to the controller, and                      */
			SwitchCommands.installRule(sw, this.table, (short)(SwitchCommands.DEFAULT_PRIORITY+1), arp, instrList);

		}
		
		
		/*       (3) all other packets to the next rule table in the switch  */
		OFMatch general = new OFMatch();
		ArrayList<OFInstruction> genInstruction = new ArrayList<OFInstruction>();
		genInstruction.add(new OFInstructionGotoTable(L3Routing.table));
		SwitchCommands.installRule(sw, this.table, SwitchCommands.DEFAULT_PRIORITY, general, genInstruction);

		/*********************************************************************/
	}
	
	/**
	 * Handle incoming packets sent from switches.
	 * @param sw switch on which the packet was received
	 * @param msg message from the switch
	 * @param cntx the Floodlight context in which the message should be handled
	 * @return indication whether another module should also process the packet
	 */
	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) 
	{
		// We're only interested in packet-in messages
		if (msg.getType() != OFType.PACKET_IN)
		{ return Command.CONTINUE; }
		OFPacketIn pktIn = (OFPacketIn)msg;
		
		// Handle the packet
		Ethernet ethPkt = new Ethernet();
		ethPkt.deserialize(pktIn.getPacketData(), 0,
				pktIn.getPacketData().length);
		
		/*********************************************************************/
		/* TODO: Send an ARP reply for ARP requests for virtual IPs; for TCP */
		/*       SYNs sent to a virtual IP, select a host and install        */
		/*       connection-specific rules to rewrite IP and MAC addresses;  */
		/*       ignore all other packets                                    */
		
		/*********************************************************************/
		
		
		System.out.println("WARNING: Here1\t" + ethPkt.getEtherType());
		if (ethPkt.getEtherType() == Ethernet.TYPE_ARP) {
			ARP arpPacket = (ARP) ethPkt.getPayload();
			System.out.println("WARNING: Here2");

			
			// Test if ARP request is associated with virtual IP
			for (LoadBalancerInstance i : instances.values()) {
				System.out.println("WARNING: virIp: " + i.getVirtualIP() + " otherIp: " + IPv4.toIPv4Address(arpPacket.getSenderProtocolAddress()));
				if (i.getVirtualIP() == IPv4.toIPv4Address(arpPacket.getTargetProtocolAddress())) {
					// construct ethernet packet
					arpPacket.setOpCode(ARP.OP_REPLY);
					arpPacket.setTargetHardwareAddress(arpPacket.getSenderHardwareAddress());
					arpPacket.setTargetProtocolAddress(arpPacket.getSenderProtocolAddress());
					arpPacket.setSenderHardwareAddress(i.getVirtualMAC());
					arpPacket.setSenderProtocolAddress(IPv4.toIPv4AddressBytes(i.getVirtualIP()));
					ethPkt.setDestinationMACAddress(ethPkt.getSourceMACAddress());
					ethPkt.setSourceMACAddress(i.getVirtualMAC());
					// send packet
					System.out.println("DEBUG: Sending ARP Packet from Sw: " + sw.getStringId() + " on port: " + pktIn.getInPort());
					SwitchCommands.sendPacket(sw, (short)pktIn.getInPort(), ethPkt);

					return Command.STOP;
				}
			}
			
			
		} else if (ethPkt.getEtherType() == Ethernet.TYPE_IPv4 && ((IPv4)ethPkt.getPayload()).getProtocol() == IPv4.PROTOCOL_TCP) {
			// tcp packet
			
			// Get client and server connection info
			IPv4 ipPacket = (IPv4) ethPkt.getPayload();
			TCP tcpPkt = (TCP)ipPacket.getPayload();
			int clientIp = ipPacket.getSourceAddress();
			short clientPort = tcpPkt.getSourcePort();
			
			// Look for the load balancer that matches our virtual IP
			LoadBalancerInstance serverInst = null;
			for (LoadBalancerInstance i : instances.values()) {
				if (i.getVirtualIP() == ipPacket.getDestinationAddress()) {
					// found our matching LoadBalancerInstance
					serverInst = i;
					break;
				}
			}
			
			// get our next Host in queue
			int serverIp = serverInst.getNextHostIP();
			
			// set serverPort to same as packet's
			short serverPort = tcpPkt.getDestinationPort();
			
			// Set up client to server Match criteria
			OFMatch clientToServer = new OFMatch();
			clientToServer.setDataLayerType(OFMatch.ETH_TYPE_IPV4);
			clientToServer.setNetworkProtocol(OFMatch.IP_PROTO_TCP);
			clientToServer.setNetworkDestination(serverInst.getVirtualIP());
			clientToServer.setNetworkSource(clientIp);
			clientToServer.setTransportSource(OFMatch.IP_PROTO_TCP, clientPort);
			clientToServer.setTransportDestination(OFMatch.IP_PROTO_TCP, serverPort);
			
			// Set up client to server instructions
			ArrayList<OFInstruction> clientToServInst = new ArrayList<OFInstruction>();
			ArrayList<OFAction> clientToServActions = new ArrayList<OFAction>();
			
			// First rewrite Ethernet and IP destinations, then send the packet to the next table
			// TODO first action might need MAC addr in long form, not byte[]
			clientToServActions.add(new OFActionSetField(OFOXMFieldType.IPV4_DST, serverIp));
			clientToServActions.add(new OFActionSetField(OFOXMFieldType.ETH_DST, this.getHostMACAddress(serverIp)));
			clientToServInst.add(new OFInstructionApplyActions(clientToServActions));
			clientToServInst.add(new OFInstructionGotoTable(L3Routing.table));
			
			// Install the rule in the switch
			SwitchCommands.installRule(sw, this.table, SwitchCommands.MAX_PRIORITY,
					clientToServer, clientToServInst, SwitchCommands.NO_TIMEOUT, IDLE_TIMEOUT);
			
			// Setup server to client match criteria
			OFMatch serverToClient = new OFMatch();
			serverToClient.setDataLayerType(OFMatch.ETH_TYPE_IPV4);
			serverToClient.setNetworkProtocol(OFMatch.IP_PROTO_TCP);
			serverToClient.setNetworkDestination(clientIp);
			serverToClient.setNetworkSource(serverIp);
			serverToClient.setTransportSource(OFMatch.IP_PROTO_TCP, serverPort);
			serverToClient.setTransportDestination(OFMatch.IP_PROTO_TCP, clientPort);
			
			// Set up server to client instructions
			ArrayList<OFInstruction> servToClientInst = new ArrayList<OFInstruction>();
			ArrayList<OFAction> servToClientActions = new ArrayList<OFAction>();
			
			// First rewrite Ethernet and IP destinations, then send the packet to the next table
			// TODO first action might need MAC addr in long form, not byte[]
			servToClientActions.add(new OFActionSetField(OFOXMFieldType.IPV4_SRC, serverInst.getVirtualIP()));
			servToClientActions.add(new OFActionSetField(OFOXMFieldType.ETH_SRC, serverInst.getVirtualMAC()));
			servToClientInst.add(new OFInstructionApplyActions(servToClientActions));
			servToClientInst.add(new OFInstructionGotoTable(L3Routing.table));
			
			// install the rule
			SwitchCommands.installRule(sw, this.table, SwitchCommands.MAX_PRIORITY, serverToClient, servToClientInst, SwitchCommands.NO_TIMEOUT, IDLE_TIMEOUT);
			
			System.out.println("DEBUG: Installed TCP rule in Sw: " + sw.getId() + " for Client: " + clientIp + " and Serv: " + serverIp);
			return Command.STOP;
		}
		
		
		
		
		
		// We don't care about other packets
		return Command.CONTINUE;
	}
	
	
	/**
	 * Returns the MAC address for a host, given the host's IP address.
	 * @param hostIPAddress the host's IP address
	 * @return the hosts's MAC address, null if unknown
	 */
	private byte[] getHostMACAddress(int hostIPAddress)
	{
		Iterator<? extends IDevice> iterator = this.deviceProv.queryDevices(
				null, null, hostIPAddress, null, null);
		if (!iterator.hasNext())
		{ return null; }
		IDevice device = iterator.next();
		return MACAddress.valueOf(device.getMACAddress()).toBytes();
	}

	/**
	 * Event handler called when a switch leaves the network.
	 * @param DPID for the switch
	 */
	@Override
	public void switchRemoved(long switchId) 
	{ /* Nothing we need to do, since the switch is no longer active */ }

	/**
	 * Event handler called when the controller becomes the master for a switch.
	 * @param DPID for the switch
	 */
	@Override
	public void switchActivated(long switchId)
	{ /* Nothing we need to do, since we're not switching controller roles */ }

	/**
	 * Event handler called when a port on a switch goes up or down, or is
	 * added or removed.
	 * @param DPID for the switch
	 * @param port the port on the switch whose status changed
	 * @param type the type of status change (up, down, add, remove)
	 */
	@Override
	public void switchPortChanged(long switchId, ImmutablePort port,
			PortChangeType type) 
	{ /* Nothing we need to do, since load balancer rules are port-agnostic */}

	/**
	 * Event handler called when some attribute of a switch changes.
	 * @param DPID for the switch
	 */
	@Override
	public void switchChanged(long switchId) 
	{ /* Nothing we need to do */ }
	
    /**
     * Tell the module system which services we provide.
     */
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() 
	{ return null; }

	/**
     * Tell the module system which services we implement.
     */
	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> 
			getServiceImpls() 
	{ return null; }

	/**
     * Tell the module system which modules we depend on.
     */
	@Override
	public Collection<Class<? extends IFloodlightService>> 
			getModuleDependencies() 
	{
		Collection<Class<? extends IFloodlightService >> floodlightService =
	            new ArrayList<Class<? extends IFloodlightService>>();
        floodlightService.add(IFloodlightProviderService.class);
        floodlightService.add(IDeviceService.class);
        return floodlightService;
	}

	/**
	 * Gets a name for this module.
	 * @return name for this module
	 */
	@Override
	public String getName() 
	{ return MODULE_NAME; }

	/**
	 * Check if events must be passed to another module before this module is
	 * notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) 
	{
		return (OFType.PACKET_IN == type 
				&& (name.equals(ArpServer.MODULE_NAME) 
					|| name.equals(DeviceManagerImpl.MODULE_NAME))); 
	}

	/**
	 * Check if events must be passed to another module after this module has
	 * been notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) 
	{ return false; }
	
}
