#pragma once
#include "SinglePortModule.h"
#include "concurrency/OSThread.h"
#include "pb_encode.h"
#include "mesh/generated/meshtastic/experiment.pb.h"
#include <map>
using namespace std;
#define LEN(a) (sizeof(a) / sizeof(*a))
#define NB_NODES 11
#define MAX_TRACKED_BROADCASTS 10
#define COLLECTOR_NODE 834716913
#define HIKING_INTERVAL 900000    // send packet every 15 mins
#define DISASTER_INTERVAL 120000  // send packet every 2 mins

/**
 * A module that sends packets periodically and listens to packets 
 * and sends its data to collector node 
 * 
 */
class ExperimentModule : public SinglePortModule,  private concurrency::OSThread
{
  public:
    /** Constructor
     * name is for debugging output
     */
    ExperimentModule() : 
    SinglePortModule("ExperimentModule", meshtastic_PortNum_TEXT_MESSAGE_APP),  
    concurrency::OSThread("ExperimentModule")
    {
        // give network time to set up
        // interval between 40000ms and 100000ms 
        unsigned int startInterval = random(40000,100000);
        setIntervalFromNow(startInterval);
    }
    // Our nodes
    NodeNum nodes[NB_NODES] = {
        1391039350, 1227105360, 3214103652, 1833769890, 
        2458335390, 871882989, 2446794159, 2057312131, 
        1507035365, 834716913, NODENUM_BROADCAST
    };
    struct NodeStats {
        uint32_t nodeId = 0;
        uint32_t dmSent = 0;
        uint32_t dmReceived = 0;
        uint32_t broadcastSent = 0;
        uint32_t broadcastReceived = 0;
        uint32_t rttSum = 0;      // not used yet
        uint32_t rttCount = 0;    // not used yet
    };
    std::map<NodeNum, NodeStats> statsMap;

    struct BroadcastRecord{
        uint32_t sender;
        uint32_t packetId;
    };
    /*BroadcastRecord recentReceivedBroadcasts[MAX_TRACKED_BROADCASTS];
    uint32_t broadcastRecordCount = 0;
    uint32_t sentBroadcastRecords = 0;*/


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
     * Called when we receive a packet, We save the packet in receivedPackets map
     */
    ProcessMessage handleReceived(const meshtastic_MeshPacket &mp) override;
    /**
     * Send our data to collector node 
     */
    uint32_t sendToCollector();
    private:
        unsigned int my_interval = DISASTER_INTERVAL; // interval in millisconds to run the module again
        uint32_t lastStatsSent = 0;       // last time stats were sent to collector node
        // interval between 1000000ms and 180000ms (3mins)
        unsigned int collectorInterval = random(100000,180000);
        uint32_t currentDestIndex = 0;
        uint32_t globalSentCounter = 0;
};

extern ExperimentModule *experimentModule;
