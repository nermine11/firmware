#pragma once
#include "mesh/generated/meshtastic/experiment.pb.h"
#include "SinglePortModule.h"
#include "MeshModule.h"
#include "concurrency/OSThread.h"
#define NB_NODES 9
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
        MeshModule("CollectorExperimentModule")
        {
          for(auto i = 0; i< NB_NODES; i++){
            for(auto j = 0; j< NB_NODES; j++){
              networkStats[i][j] = {};
            }
            sentBroadcasts[i] = 0;
          }
        }
    // Our nodes
    NodeNum nodes[NB_NODES + 1] = {
        1391039350, 1227105360, 3214103652, 1833769890, 
        2458335390, 871882989, 2446794159, 2057312131, 
        1507035365, NODENUM_BROADCAST
    };
    struct LinkStats{
        uint32_t dmSent      = 0;
        uint32_t dmReceived  = 0;
        uint32_t broadcastReceived  = 0;
        uint32_t rtt_sum   = 0;
        uint32_t rtt_count = 0;
    };
    // networkStats[A][B] = stats reported by A about B
    LinkStats networkStats[NB_NODES][NB_NODES];
    // Sent broadcasts IDS per node
    // Node A sent how many broadcasts
    uint32_t sentBroadcasts[NB_NODES];

    protected:
      ProcessMessage handleReceived(const meshtastic_MeshPacket &mp);
      bool wantPacket(const meshtastic_MeshPacket *p) override;
      int  getNodeIndex(NodeNum node);
      void sendTestUSB();
      float GlobalBroadcastsPDR(int A);
    };
extern CollectorExperimentModule  *collectorModule;
