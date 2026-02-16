#pragma once
#include "SinglePortModule.h"
#include "concurrency/OSThread.h"
#include "pb_encode.h"
#include "mesh/generated/meshtastic/experiment.pb.h"
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
    NodeNum nodes[2] = {1833769890, NODENUM_BROADCAST};
    //NodeNum nodes[2] = {1227105360,NODENUM_BROADCAST };
    /*
    map to keep track of how many unique packets are sent to each node
    We do not count retransmissions, only unique packets
    <NodeNum,  <unique packets received that NodeNum, when they were sent>*/ 
    std::map<NodeNum, std::map<uint32_t, uint32_t>> sentPackets;
    /*
    map to keep track of how many unique packets are received from each node
    We do not count duplicate packets, or what we received as relays
    We only count the packets sent to us as DM or rebroadcasts
    <NodeNum,  <unique packets received that NodeNum, when they were received>*/ 
    std::map<NodeNum, std::map<uint32_t, uint32_t>> receivedPackets;
    // Total number of packets sent by our node 
    uint32_t globalCounter = 0;

  protected:
    unsigned int my_interval = 45000; // interval in millisconds
    uint32_t lastStatsSent = 0;       // last time stats were sent to collector node
    virtual int32_t runOnce() override;
    /**
     * Send a packet nb i to specific destination dest
     * @i : counter of the packets sent to the destination dest
     * @ dest: the destination of the packet
     */
    uint32_t sendPacket(int i, NodeNum dest);
    /**
     * Called when we receive a packet, We save the packet in receivedPackets map
     */
    ProcessMessage handleReceived(const meshtastic_MeshPacket &mp) override;
    uint32_t ExperimentModule::sendToCollector();
  private:
    static void ExperimentModule ::deleteElements(int numElementsToRemove, std::map<NodeNum, std::map<uint32_t, uint32_t>> packets);
};

extern ExperimentModule *experimentModule;
