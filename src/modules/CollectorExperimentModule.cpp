#include "CollectorExperimentModule.h"
#include "MeshService.h"
#include "configuration.h"
#include "Router.h"
#include "main.h"
#include <sys/stat.h>

CollectorExperimentModule  *collectorModule;


ProcessMessage CollectorExperimentModule::handleReceived(const meshtastic_MeshPacket &mp)
{
    LOG_INFO("ExperimentModule handleReceived called");
    if (!isToUs(&mp)){
        return ProcessMessage::STOP;
    }
    static ExperimentStats stats = ExperimentStats_init_zero;
    if (!pb_decode_from_bytes(
            mp.decoded.payload.bytes,
            mp.decoded.payload.size,
            ExperimentStats_fields,
            &stats)) {

        LOG_ERROR("Decode failed");
        return ProcessMessage::STOP;
    }
    NodeNum sender = stats.sender_node;
    Node &nodeEntry = nodesMap[sender];
    /* ------------------ SENT PACKETS ------------------ */
    for (uint32_t i = 0; i < stats.sent_count; i++) {
        NodeStats *nodeState = &stats.sent[i];
        for (uint32_t j = 0; j < nodeState->packets_count; j++) {
            PacketEntry *pe = &nodeState->packets[j];
            Packet &p = nodeEntry.sentPackets[nodeEntry.sentCount];
            p.node = nodeState->node_id;              // destination node
            p.packetId = pe->packet_id;
            p.timestamp = pe->timestamp;
            snprintf(p.text,
                    sizeof(p.text),
                    "%s",
                    pe->text);            
            nodeEntry.sentCount++;
        }
    }
    printExperimentStats();
    return ProcessMessage::STOP; // Let others look at this message also if they want
}

void CollectorExperimentModule::printExperimentStats()
{
    LOG_INFO("{");
    for (auto &pair : nodesMap)
    {
        NodeNum nodeId = pair.first;
        Node &node = pair.second;
        LOG_INFO("  \"%u\": {", nodeId);
        /* -------- SENT -------- */
        LOG_INFO("    \"sent\": [");
        for (uint32_t i = 0; i < node.sentCount; i++)
        {
            Packet &pkt = node.sentPackets[i];
            LOG_INFO(
                "      {\"to\":%u,\"packetId\":%u,\"timestamp\":%u,\"text\":\"%s\"}",
                pkt.node,
                pkt.packetId,
                pkt.timestamp,
                pkt.text
            );
        }
        LOG_INFO("    ],");
    }
}

