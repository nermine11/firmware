#pragma once
#include "mesh/generated/meshtastic/experiment.pb.h"
#include "SinglePortModule.h"
#include "MeshModule.h"
#include "concurrency/OSThread.h"
#include <map>
#include <set>
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
      Packet sentPackets[200];
      uint32_t sentCount = 0;
      uint32_t receivedCount = 0;
      Packet receivedPackets[200];
    };
    std::map<NodeNum, Node> nodesMap;
    std::set<uint32_t> sentProcessed;
    std::set<uint32_t> receivedProcessed;
    protected:
      ProcessMessage handleReceived(const meshtastic_MeshPacket &mp);
      void printExperimentStats();
      bool wantPacket(const meshtastic_MeshPacket *p) override;

    };
extern CollectorExperimentModule  *collectorModule;
