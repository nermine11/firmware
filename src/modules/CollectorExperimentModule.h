#pragma once
#include "mesh/generated/meshtastic/experiment.pb.h"
#include "SinglePortModule.h"
#include "MeshModule.h"
#include "concurrency/OSThread.h"
#include <map>
using namespace std;
/**
 * A module to collect data from other nodes and sends it to the computer
 */
class CollectorExperimentModule : public MeshModule
{
  public:
    /** Constructor
     * name is for debugging output
     */
    CollectorExperimentModule() : MeshModule("CollectorExperimentModule")
    {}
    struct Packet {
      NodeNum node; // source or destination depending on whether we are sending or receiving
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
      unsigned int my_interval = 10000; // interval in millisconds to run the module again
      bool wantPacket(const meshtastic_MeshPacket *p) override;

    };
extern CollectorExperimentModule  *collectorModule;
