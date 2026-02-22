#include "CollectorExperimentModule.h"
#include "MeshService.h"
#include "configuration.h"
#include "Router.h"
#include "main.h"
#include <sys/stat.h>

CollectorExperimentModule  *collectorModule;

bool CollectorExperimentModule::wantPacket(const meshtastic_MeshPacket *p){
    return p->decoded.portnum == meshtastic_PortNum_PRIVATE_APP ||
    p->decoded.portnum == meshtastic_PortNum_TEXT_MESSAGE_APP;
}

ProcessMessage CollectorExperimentModule::handleReceived(const meshtastic_MeshPacket &mp)
{
    /*
    If we receive text dump from another node, 
    send all data through serial
    */ 
    if (mp.decoded.portnum == meshtastic_PortNum_TEXT_MESSAGE_APP){
        const char *msg = (const char *) mp.decoded.payload.bytes;
        LOG_INFO("messagee %s", msg);
        if (strncmp(msg, "dump", 4) == 0){
            sendTestUSB();             
        }
        return ProcessMessage::STOP;
    }
    // ---- Stats received from the experiment nodes------

    // Only handle packets sent to us as DM 
    if (!isToUs(&mp)){
        return ProcessMessage::STOP;
    }
    static ExperimentStats stats = ExperimentStats_init_zero;
    memset(&stats, 0, sizeof(stats));
    if (!pb_decode_from_bytes(
            mp.decoded.payload.bytes,
            mp.decoded.payload.size,
            ExperimentStats_fields,
            &stats)) {
        LOG_ERROR("Decode failed");
        return ProcessMessage::STOP;
    }
    NodeNum sender = stats.sender_node;
    auto &ns = networkStats[sender];
    /* ------------------ Sent packets ------------------ */
    for (uint32_t i = 0; i < stats.stats_count; i++) {
        _NodeStats *nodeState = &stats.stats[i];
        LinkStats& linkStats = networkStats[sender][nodeState->node_id];
        linkStats.dmSent = nodeState -> dm_sent;
        linkStats.dmReceived = nodeState -> dm_received;
        linkStats.broadcastSent = nodeState -> broadcast_sent;
        linkStats.broadcastReceived = nodeState -> broadcast_received;
        linkStats.rtt_sum = nodeState ->rtt_sum;
        linkStats.rtt_count = nodeState ->rtt_count;
    }
    return ProcessMessage::STOP; // Don't Let others look at this message 
}

void CollectorExperimentModule::printExperimentStats()
{
    LOG_INFO (" ========= Network Stats ========");
    for(auto &sendPair: networkStats){
        NodeNum A = sendPair.first;
        for(auto& destPair : networkStats[A]){
            NodeNum B = destPair.first;
            LinkStats& ls = destPair.second;
            // PDR of DMs sent by A for each dest node B
            // what B received from A
            uint32_t dmReceived = 0;
            uint32_t broadcastReceived = 0;
            if(networkStats.count(B) && networkStats.count(A)){
                dmReceived = networkStats[B][A].dmReceived;
                broadcastReceived = networkStats[B][A].broadcastReceived;
            }
            // dm PDR
            float dmPdr = 0.0f;
            if(networkStats[A][B].dmSent> 0){
                // node to node pdr
                dmPdr = (float) dmReceived/networkStats[A][B].dmSent;
            }
            // broadcast PDR
            float broadcastPdr = 0.0f;
            if(networkStats[A][B].broadcastSent> 0){
                // node to node pdr
                broadcastPdr = (float) broadcastReceived/networkStats[A][B].broadcastSent;
            }
            LOG_INFO(" Link %u -> %u", A, B);
            LOG_INFO(" dm PDR: %.3f", dmPdr);
            LOG_INFO(" broadcast PDR %.3f", broadcastPdr );     
            // global PDR of broadcast sent by A nb 
        }


        LOG_INFO (" =================");

    }
    LOG_INFO (" ========End =========");

}

void CollectorExperimentModule::sendTestUSB()
{
    Serial.println("DUMP_BEGIN");
    for(auto &sendPair: networkStats){
        NodeNum A = sendPair.first;
        for(auto& destPair : networkStats[A]){
            NodeNum B = destPair.first;
            LinkStats& ls = destPair.second;
            // PDR of DMs sent by A for each dest node B
            // what B received from A
            uint32_t dmReceived = 0;
            uint32_t broadcastReceived = 0;
            if(networkStats.count(B) && networkStats.count(A)){
                dmReceived = networkStats[B][A].dmReceived;
                broadcastReceived = networkStats[B][A].broadcastReceived;
            }
            // dm PDR
            float dmPdr = 0.0f;
            if(networkStats[A][B].dmSent> 0){
                // node to node pdr
                dmPdr = (float) dmReceived/networkStats[A][B].dmSent;
            }
            // broadcast PDR
            float broadcastPdr = 0.0f;
            if(networkStats[A][B].broadcastSent> 0){
                // node to node pdr
                broadcastPdr = (float) broadcastReceived/networkStats[A][B].broadcastSent;
            }
            Serial.print(A);
            Serial.print(",");
            Serial.print(B);
            Serial.print(",");
            Serial.print(dmPdr, 6);
            Serial.print(",");
            Serial.println(broadcastPdr, 6);
        }
    }
    Serial.println("DUMP_END");
    Serial.flush();
}
