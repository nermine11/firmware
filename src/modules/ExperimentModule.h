#pragma once
#include "SinglePortModule.h"
#include "concurrency/OSThread.h"
#include <map>
#include <set>
using namespace std;
#define LEN(a) (sizeof(a) / sizeof(*a))

/**
 * A simple example module that just replies with "Message received" to any message it receives.
 */
class ExperimentModule : public SinglePortModule,  private concurrency::OSThread
{
  public:
    /** Constructor
     * name is for debugging output
     */
    ExperimentModule() : SinglePortModule("ExperimentModule", meshtastic_PortNum_TEXT_MESSAGE_APP),  
    concurrency::OSThread("ExperimentModule")
    //intialize the sentCount map
    {
        // give network time to set up
        setIntervalFromNow(40000);

    }
    // number of our nodes
    //NodeNum nodes[11] = {1391039350, 1227105360, 3214103652, 1833769890, 
    //2458335390, 871882989, 2446794159, 2057312131, 1507035365, 834716913, NODENUM_BROADCAST};
    NodeNum nodes[3] = {834716913,NODENUM_BROADCAST,1833769890  };
    /*
    map to keep track of how many unique packets are sent to each node
    We do not count retransmissions, only unique packets
    <NodeNum, set of unique packets sent to that NodeNum>*/ 
    std::map<NodeNum, std::set<uint32_t>> sentPackets;
    /*
    map to keep track of how many unique packets are received from each node
    We do not count duplicate packets, or what we received as relays
    We only count the packets sent to us as DM or rebroadcasts
    <NodeNum, set of unique packets received that NodeNum>*/ 
    std::map<NodeNum, std::set<uint32_t>> receivedPackets;
    // Total number of packets sent by our node 
    uint32_t globalCounter = 0;

  protected:
    unsigned int my_interval = 40000; // interval in millisconds
    virtual int32_t runOnce() override;
    /**
     * Send a packet nb i to specific destination dest
     * @i : counter of the packets sent to the destination dest
     * @ dest: the destination of the packet
     */
    uint32_t sendPacket(int i, NodeNum dest);
    /**
     * Called when we receive a packet
     * We save the packet in receivedPackets map
     */
    ProcessMessage handleReceived(const meshtastic_MeshPacket &mp) override;
};

extern ExperimentModule *experimentModule;
