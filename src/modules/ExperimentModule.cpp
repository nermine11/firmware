#include "ExperimentModule.h"
#include "MeshService.h"
#include "configuration.h"
#include "Router.h"
#include "main.h"
#include <sys/stat.h>

ExperimentModule *experimentModule;

uint32_t ExperimentModule::sendPacket(int i, NodeNum dest)
{
    meshtastic_MeshPacket *p = router->allocForSending();
    if (p){
        p->to = dest;
        p->decoded.portnum = meshtastic_PortNum_TEXT_MESSAGE_APP;
        p->want_ack = false;
        // payload test: i: destination
        char msg[40];
        snprintf(msg, sizeof(msg), "test: %d: %d", i, dest);
        size_t len = strlen(msg);
        // Safety check
        if (len > sizeof(p->decoded.payload.bytes)){
            len = sizeof(p->decoded.payload.bytes);
        }
        memcpy(p->decoded.payload.bytes, msg, len);
        p->decoded.payload.size = len;
        // send the packet
        service->sendToMesh(p);
        return p->id;
    }
    return 0;
}

ProcessMessage ExperimentModule::handleReceived(const meshtastic_MeshPacket &mp)
{
    // if packet is broadcast or is to us, increment the counter
    if ((isBroadcast(mp.to) || isToUs(&mp))) {
        auto& senderMap = receivedPackets[mp.from];
        // if the id of the packet is not already received, add when it was received
        if (senderMap.find(mp.id) == senderMap.end()) {
            senderMap[mp.id] = millis();
            LOG_INFO("Received test packet from %u", mp.from);

        }
    }
    return ProcessMessage::CONTINUE; // Let others look at this message also if they want
}


uint32_t ExperimentModule::sendToCollector()
{
    meshtastic_MeshPacket *p = router->allocForSending();
    if (p){
        p->to = 1391039350; // collector node
        p->decoded.portnum = meshtastic_PortNum_TEXT_MESSAGE_APP;
        p->want_ack = false;
        // Create protobuf struct
        ExperimentStats stats = ExperimentStats_init_zero;
        stats.sender_node = nodeDB->getNodeNum();
        // Sent packets
        stats.sent_count = 0; //intialize to 0
        for (auto &destPair : sentPackets){
            NodeStats *nodeStats = &stats.sent[stats.sent_count];
            stats.sent_count ++;
            nodeStats->node_id = destPair.first;
            nodeStats->packets_count = 0;
            for (auto &packetPair : destPair.second)
            {
                PacketEntry *entry = &nodeStats->packets[nodeStats->packets_count];
                nodeStats->packets_count++;
                entry->packet_id = packetPair.first;
                entry->timestamp = packetPair.second;
            }
        }
        sentPackets.clear(); // clear the map
        // Recieved packets
        stats.received_count = 0;
        for (auto &srcPair : receivedPackets)
        {
            NodeStats *nodeStats = &stats.received[stats.received_count];
            stats.received_count++;
            nodeStats->node_id = srcPair.first;
            nodeStats->packets_count = 0;
            for (auto &packetPair : srcPair.second)
            {
                PacketEntry *entry = &nodeStats->packets[nodeStats->packets_count];
                nodeStats->packets_count++;
                entry->packet_id = packetPair.first;
                entry->timestamp = packetPair.second;
            }
        }
        receivedPackets.clear();
        // Encode into payload
        pb_ostream_t stream = pb_ostream_from_buffer(
            p->decoded.payload.bytes,
            sizeof(p->decoded.payload.bytes)
        );
        bool success = pb_encode(&stream, ExperimentStats_fields, &stats);
        if (!success){
            LOG_ERROR("Protobuf encode failed");
            return 0;
        }
        p->decoded.payload.size = stream.bytes_written;
        service->sendToMesh(p);
        return p->id;
    } 
    return 0;
}

/**
 * send periodically a packet to a destination
 * each my_interval ms, change the destination
 */
int32_t ExperimentModule::runOnce()
{
    NodeNum dest = nodes[globalCounter % LEN(nodes)];
    LOG_INFO(" dest: %u, globalCounter:%u, LEN(nodes): %u ", dest, globalCounter, LEN(nodes));
    uint32_t i = sentPackets[dest].size();
    uint32_t id = sendPacket(i, dest);
    if(id){
        LOG_INFO("Sent test packet %u to %u", i, dest);
        sentPackets[dest][id] = millis();
        globalCounter ++;
        LOG_INFO("nb global packets: %u", globalCounter);
    }
    // send data to collector every 5 mins
    uint32_t now  = millis();
    // if it has been 5 mins since we last sent, send again
    if(now - lastStatsSent > 300000 ){
        sendToCollector();
        lastStatsSent = now;
    }
    // run again after my_interval ms
    return(my_interval);
}
