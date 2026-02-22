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
    CollectorExperimentModule() :
        MeshModule("CollectorExperimentModule"){}

    struct LinkStats{
        uint32_t dmSent      = 0;
        uint32_t dmReceived  = 0;
        uint32_t broadcastSent      = 0;
        uint32_t broadcastReceived  = 0;
        uint32_t rtt_sum   = 0;
        uint32_t rtt_count = 0;
    };
    // networkStats[A][B] = stats reported by A about B
    std::map<NodeNum, std::map<NodeNum, LinkStats>> networkStats;
    protected:
      ProcessMessage handleReceived(const meshtastic_MeshPacket &mp);
      void printExperimentStats();
      bool wantPacket(const meshtastic_MeshPacket *p) override;
      void sendTestUSB();

    };
extern CollectorExperimentModule  *collectorModule;
