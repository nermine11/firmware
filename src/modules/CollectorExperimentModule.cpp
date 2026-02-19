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
    if (mp.decoded.portnum == meshtastic_PortNum_TEXT_MESSAGE_APP)
    {
        const char *msg = (const char *) mp.decoded.payload.bytes;
        LOG_INFO("messageeeeeee %s", msg);
        if (strncmp(msg, "dump", 4) == 0){
            printExperimentStats();             
        }
        return ProcessMessage::STOP;
    }
    
    // only handle packets sent to us as DM (data reports)
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
    return ProcessMessage::STOP; // Don't Let others look at this message 
}

void CollectorExperimentModule::printExperimentStats()
{
    LOG_INFO("{");
    for (auto &pair : nodesMap)
    {
        NodeNum senderId = pair.first;
        Node &node = pair.second;
        LOG_INFO("  Node: %u", senderId);
        LOG_INFO("  Sent {");
        // Group packets by destination node
        std::map<NodeNum, uint32_t> packetCount;
        for (uint32_t i = 0; i < node.sentCount; i++)
        {
            packetCount[node.sentPackets[i].node]++;
        }
        // For each destination node
        for (auto &destPair : packetCount)
        {
            NodeNum destNode = destPair.first;
            uint32_t count = destPair.second;
            LOG_INFO("    Node %u : Number of packets sent: %u",
                     destNode, count);
            LOG_INFO("    {");
            for (uint32_t i = 0; i < node.sentCount; i++)
            {
                Packet &pkt = node.sentPackets[i];

                if (pkt.node == destNode)
                {
                    LOG_INFO("      PacketId: %u, timestamp: %u, text: %s",
                             pkt.packetId,
                             pkt.timestamp,
                             pkt.text);
                }
            }
            LOG_INFO("    }");
        }
        LOG_INFO("  }");
    }
    LOG_INFO("}");
}

