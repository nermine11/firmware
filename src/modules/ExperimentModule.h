#pragma once
#include "SinglePortModule.h"
#include "concurrency/OSThread.h"
#include "pb_encode.h"
#include "mesh/generated/meshtastic/experiment.pb.h"
#include <map>
using namespace std;
#define LEN(a) (sizeof(a) / sizeof(*a))
#define NB_NODES 11
#define COLLECTOR_NODE 2458335390  
/**
 * A module that sends packets periodically every 40s and listens to packets 
 * and sends its data to collector node every 2 minutes
 * 
 */
class ExperimentModule : public SinglePortModule,  private concurrency::OSThread
{
  public:
    /** Constructor
     * name is for debugging output
     */
    ExperimentModule() : SinglePortModule("ExperimentModule", meshtastic_PortNum_TEXT_MESSAGE_APP),  
    concurrency::OSThread("ExperimentModule")
    {
        // give network time to set up
        // interval between 40000ms and 100000ms 
        unsigned int startInterval = rand()%(100000-40000 + 1) + 40000;
        setIntervalFromNow(startInterval);
    }
    // Our nodes
    NodeNum nodes[NB_NODES] = {1391039350, 1227105360, 3214103652, 1833769890, 
    2458335390, 871882989, 2446794159, 2057312131, 1507035365, 834716913, NODENUM_BROADCAST};
    uint32_t currentDestIndex = 0;
    uint32_t globalSentCounter = 0;
    //NodeNum nodes[2] = {NODENUM_BROADCAST, 2446794159};
    struct Packet {
      NodeNum node; // source or destination depending on whether we are sending or receiving
      uint32_t packetId;
      char text[64];
      uint32_t timestamp;
    };
    struct NodeInfo {
      Packet sentPackets[100];
      uint32_t sentPacketsCount = 0;
      uint32_t receivedPacketsCount = 0;
      Packet receivedPackets[100];
      uint32_t sentPacketsToCollector = 0;      // number of sent packets sent to the collector
      uint32_t receivedPacketsToCollector = 0; // number of received packets sent to the collector
    };
    std::map<NodeNum, NodeInfo> nodesMap;
  protected:
    /**
    * Send periodically a packet to a destination
    * Each my_interval ms, change the destination
    */
    virtual int32_t runOnce() override;
    /**
     * Send a packet nb i to specific destination dest
     * @ dest: the destination of the packet
     */
    uint32_t sendPacket( NodeNum dest);
    /**
     * Save sent packet in our nodesMap
     */
    void saveSentPacket(uint32_t id, NodeNum dest, 
                        uint32_t timestamp,char text[64]);
    /**
     * Called when we receive a packet, We save the packet in receivedPackets map
     */
    ProcessMessage handleReceived(const meshtastic_MeshPacket &mp) override;
    void savereceivedPacket(const meshtastic_MeshPacket &mp, uint32_t timestamp);
    /**
     * Send our data to collector node 
     */
    uint32_t sendToCollector();
    private:
      unsigned int my_interval = 90000; // interval in millisconds to run the module again
      uint32_t lastStatsSent = 0;       // last time stats were sent to collector node
      // interval between 1000000ms and 180000ms (3mins)
      unsigned int collectorInterval = rand()%(120000-100000 + 1) + 100000;
};

extern ExperimentModule *experimentModule;
