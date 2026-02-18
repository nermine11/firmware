#pragma once
#include "SinglePortModule.h"
#include "concurrency/OSThread.h"
#include "pb_encode.h"
#include "mesh/generated/meshtastic/experiment.pb.h"
#include <map>
using namespace std;
#define LEN(a) (sizeof(a) / sizeof(*a))
#define NB_NODES 10
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
    NodeNum nodes[1] = {NODENUM_BROADCAST};
    //NodeNum nodes[2] = {1227105360,NODENUM_BROADCAST };
    // Total number of packets sent by our node 
    uint32_t globalCounter = 0;

    struct Packet {
      NodeNum node; // source or destination
      uint32_t packetId;
      char text[64];
      uint32_t timestamp;
    };
    struct Node {
      Packet sentPackets[100];
      uint32_t sentCount = 0;
      uint32_t receievedCount = 0;
      Packet receivedPackets[100];
      uint32_t sentToCollector = 0;
      uint32_t receivedToCollector = 0;
    };
    std::map<NodeNum, Node> nodesMap;
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
    //ProcessMessage handleReceived(const meshtastic_MeshPacket &mp) override;
    uint32_t sendToCollector();
    void saveSentPacket(uint32_t id, NodeNum dest, uint32_t timestamp, char text[64]);
};

extern ExperimentModule *experimentModule;
