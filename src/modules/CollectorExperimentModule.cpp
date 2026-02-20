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
    // if we receive text dump from another node, send all data through serial
    if (mp.decoded.portnum == meshtastic_PortNum_TEXT_MESSAGE_APP)
    {
        const char *msg = (const char *) mp.decoded.payload.bytes;
        LOG_INFO("messageeeeeee %s", msg);
        if (strncmp(msg, "dump", 4) == 0){
            printExperimentStats();             
        }
        return ProcessMessage::STOP;
    }
    // Stats received from the experiment nodes
    // only handle packets sent to us as DM (data reports)
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
    Node &nodeEntry = nodesMap[sender];
    /* ------------------ Sent packets ------------------ */
    for (uint32_t i = 0; i < stats.sent_count; i++) {
        NodeStats *nodeState = &stats.sent[i];
        for (uint32_t j = 0; j < nodeState->packets_count; j++) {
            PacketEntry *pe = &nodeState->packets[j];
            /* We have seen this packet before so do not 
            save it */
            if(sentProcessed.find(pe->packet_id) != sentProcessed.end()){
                continue;
            }
            else{
                sentProcessed.insert(pe->packet_id);
            }
            Packet &p = nodeEntry.sentPackets[nodeEntry.sentCount];
            p.node = nodeState->node_id;              // destination node
            p.packetId = pe->packet_id;
            p.timestamp = pe->timestamp;
            snprintf(p.text,
                    sizeof(p.text),
                    "%s",
                    pe->text);          
            LOG_INFO("text %s",p.text);  
            nodeEntry.sentCount++;
            LOG_INFO("sent count of node %u: %u",p.node, nodeEntry.sentCount);
        }
    }
    /* ------------------ Received packets ------------------ */
    for (uint32_t i = 0; i < stats.received_count; i++) {
        NodeStats *nodeState = &stats.received[i];
        for (uint32_t j = 0; j < nodeState->packets_count; j++) {
            PacketEntry *pe = &nodeState->packets[j];
            /* We have seen this packet before so do not 
            save it */
            if(receivedProcessed.find(pe->packet_id) != receivedProcessed.end()){
                continue;
            }
            else{
                receivedProcessed.insert(pe->packet_id);
            }
            Packet &p = nodeEntry.receivedPackets[nodeEntry.receivedCount];
            p.node = nodeState->node_id;              // Source node
            p.packetId = pe->packet_id;
            p.timestamp = pe->timestamp;
            snprintf(p.text,
                    sizeof(p.text),
                    "%s",
                    pe->text);            
            nodeEntry.receivedCount++;
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
        /* -------------------- SENT -------------------- */
        LOG_INFO("  Sent {");
        std::map<NodeNum, uint32_t> sentCountMap;
        for (uint32_t i = 0; i < node.sentCount; i++){
            sentCountMap[node.sentPackets[i].node]++;
        }
        for (auto &destPair : sentCountMap){
            NodeNum destNode = destPair.first;
            uint32_t count = destPair.second;
            LOG_INFO("    Node %u : Number of packets sent: %u",
                     destNode, count);
            LOG_INFO("    {");
            for (uint32_t i = 0; i < node.sentCount; i++){
                Packet &pkt = node.sentPackets[i];
                if (pkt.node == destNode){
                    LOG_INFO("      PacketId: %u, timestamp: %u, text: %s",
                             pkt.packetId,
                             pkt.timestamp,
                             pkt.text);
                }
            }
            LOG_INFO("    }");
        }
        LOG_INFO("  }");
        /* -------------------- RECEIVED -------------------- */
        LOG_INFO("  Received {");
        std::map<NodeNum, uint32_t> recvCountMap;
        for (uint32_t i = 0; i < node.receivedCount; i++){
            recvCountMap[node.receivedPackets[i].node]++;
        }
        for (auto &srcPair : recvCountMap){
            NodeNum srcNode = srcPair.first;
            uint32_t count = srcPair.second;
            LOG_INFO("    Node %u : Number of packets received: %u",
                     srcNode, count);
            LOG_INFO("    {");
            for (uint32_t i = 0; i < node.receivedCount; i++){
                Packet &pkt = node.receivedPackets[i];
                if (pkt.node == srcNode){
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

