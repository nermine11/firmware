#pragma once
#include "mesh/generated/meshtastic/experiment.pb.h"
#include "SinglePortModule.h"
#include "concurrency/OSThread.h"
#include <map>
using namespace std;


/**
 * A simple example module that just replies with "Message received" to any message it receives.
 */
class CollectorExperimentModule : public SinglePortModule
{
  public:
    /** Constructor
     * name is for debugging output
     */
    CollectorExperimentModule() : SinglePortModule("CollectorExperimentModule", meshtastic_PortNum_PRIVATE_APP){}
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
    };
    std::map<NodeNum, Node> nodesMap;
    protected:
      ProcessMessage handleReceived(const meshtastic_MeshPacket &mp);
      void printExperimentStats();

};


extern CollectorExperimentModule  *collectorModule;
