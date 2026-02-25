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

int CollectorExperimentModule::getNodeIndex(NodeNum node){
    for(auto i = 0; i< NB_NODES; i++){
        if(nodes[i] == node){
            return i;
        }
    }
    return -1;
}

ProcessMessage CollectorExperimentModule::handleReceived(const meshtastic_MeshPacket &mp)
{
    LOG_INFO(" in handle received");
    /*
    If we receive text dump from another node, 
    send all data through serial
    */ 
    if (mp.decoded.portnum == meshtastic_PortNum_TEXT_MESSAGE_APP){
        const char *msg = (const char *) mp.decoded.payload.bytes;
        if (strncmp(msg, "dump", 4) == 0){
            sendTestUSB();             
        }
        return ProcessMessage::CONTINUE;
    }
    // ---- Stats received from the experiment nodes------

    // Only handle packets sent to us as DM 
    if (!isToUs(&mp) || mp.from == nodeDB->getNodeNum()){
        LOG_INFO("message not to us or from us");
        return ProcessMessage::CONTINUE;
    }
    static ExperimentStats stats = ExperimentStats_init_zero;
    memset(&stats, 0, sizeof(stats));
    if (!pb_decode_from_bytes(
            mp.decoded.payload.bytes,
            mp.decoded.payload.size,
            ExperimentStats_fields,
            &stats)) {
        LOG_ERROR("Decode failed");
        return ProcessMessage::CONTINUE;
    }
    NodeNum sender = stats.sender_node;
    int senderIndex = getNodeIndex(sender);
    LOG_INFO("sender Index: %u" ,senderIndex);
    if(senderIndex == -1){
        return ProcessMessage::CONTINUE;
    }
    /* ------------------ received stats------------------ */
    if(stats.stats_count >= 10){
        LOG_INFO("STATS COUNT big");
        return ProcessMessage::CONTINUE;
    }
    for (uint32_t i = 0; i < stats.stats_count; i++) {
        _NodeStats *nodeState = &stats.stats[i];
        int otherNodeIndex = getNodeIndex(nodeState->node_id);
        if(otherNodeIndex == -1){
            continue;
        }
        LinkStats& linkStats = networkStats[senderIndex][otherNodeIndex];
        linkStats.dmSent = nodeState -> dm_sent;
        linkStats.dmReceived = nodeState -> dm_received;
        linkStats.broadcastReceived = nodeState -> broadcasts_received;
        linkStats.rtt_sum = nodeState ->rtt_sum;
        linkStats.rtt_count = nodeState ->rtt_count;
    }
    sentBroadcasts[senderIndex] = stats.sentBroadcasts;

    return ProcessMessage::CONTINUE; // Don't Let others look at this message 
}

float CollectorExperimentModule::GlobalBroadcastsPDR(int A){
    uint32_t broadcastsReceived = 0;
    for(auto j = 0; j< NB_NODES; j++){
        if(j == A){
            continue;
        }
        broadcastsReceived += networkStats[j][A].broadcastReceived;
    }
    uint32_t sent = sentBroadcasts[A];
    uint32_t maxPossibleCoverage = sent *(NB_NODES - 1); // how many broadcasts should be received by all other nodes
    return maxPossibleCoverage> 0? float(broadcastsReceived)/ float(maxPossibleCoverage): 0.0f;
}

void CollectorExperimentModule::sendTestUSB()
{
    Serial.println("DUMP_BEGIN");
    for(auto i = 0; i< NB_NODES; i++){
        NodeNum A = nodes[i];
        int indexA = getNodeIndex(A);
        if(indexA == -1){
            continue;
        }
        // calculate global broadcast PDR
        float GlobalBroadcastsPdr = GlobalBroadcastsPDR(indexA);
        for(auto j = 0; j< NB_NODES; j++){
            if(nodes[j] == A){
                continue;
            }
            NodeNum B = nodes[j];
            int indexB = getNodeIndex(B);
            if(indexB == -1){
                continue;
            }
            // PDR of DMs sent by A for each dest node B
            // what B received from A
            uint32_t dmReceived = 0;
            uint32_t broadcastReceived = 0;
            dmReceived = networkStats[indexB][indexA].dmReceived;
            broadcastReceived = networkStats[indexB][indexA].broadcastReceived;
            // dm PDR
            float dmPdr = 0.0f;
            if(networkStats[indexA][indexB].dmSent> 0){
                // node to node pdr
                dmPdr = (float) dmReceived/networkStats[indexA][indexB].dmSent;
            }
            // broadcast PDR
            float broadcastPdr = 0.0f;
            if(sentBroadcasts[indexA]> 0){
                // node to node pdr
                broadcastPdr = (float) broadcastReceived/sentBroadcasts[indexA];
            }
            // rtt
            float rtt = 0.0f;
            if(networkStats[indexA][indexB].rtt_count > 0){
                rtt = (float) networkStats[indexA][indexB].rtt_sum / networkStats[indexA][indexB].rtt_count;
            }
            Serial.print(A);
            Serial.print(",");
            Serial.print(GlobalBroadcastsPdr);
            Serial.print(",");
            Serial.print(B);
            Serial.print(",");
            Serial.print(dmPdr, 6);
            Serial.print(",");
            Serial.print(broadcastPdr, 6);
            Serial.print(",");
            Serial.println(rtt, 6);
        }
    }
    Serial.println("DUMP_END");
    Serial.flush();
}
