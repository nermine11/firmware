#include "ExperimentModule.h"
#include "MeshService.h"
#include "configuration.h"
#include "Router.h"
#include "main.h"
#include <sys/stat.h>

ExperimentModule *experimentModule;

void ExperimentModule::saveSentPacket(uint32_t packetId, NodeNum dest, 
                                     uint32_t timestamp,char text[64]){
    NodeInfo &nodeInfo = nodesMap[dest];
    // create packet to save
    Packet &p = nodeInfo.sentPackets[nodeInfo.sentPacketsCount];
    p.node = dest;
    p.packetId = packetId;
    p.timestamp = timestamp;
    snprintf(p.text,
            sizeof(p.text),
            "%s",
            text);
   nodeInfo.sentPacketsCount++;
   globalSentCounter++;
}

void ExperimentModule::savereceivedPacket(const meshtastic_MeshPacket &mp, uint32_t timestamp){
    NodeInfo &nodeInfo = nodesMap[mp.from];
    // create packet to save
    Packet &p = nodeInfo.receivedPackets[nodeInfo.receivedPacketsCount];
    p.node = mp.from;
    p.packetId = mp.id;
    p.timestamp = timestamp;
    // payload copy
    size_t len = mp.decoded.payload.size;
    if (len >= sizeof(p.text))
        len = sizeof(p.text) - 1;
    memcpy(p.text, mp.decoded.payload.bytes, len);
    p.text[len] = '\0';
   nodeInfo.receivedPacketsCount++;
   
}

uint32_t ExperimentModule::sendPacket( NodeNum dest)
{   
    // don't send packet to myself
    if(dest == nodeDB->getNodeNum()){
        return 0;
    }
    meshtastic_MeshPacket *p = router->allocForSending();
    if (p){
        p->to = dest;
        p->decoded.portnum = meshtastic_PortNum_TEXT_MESSAGE_APP;
        p->want_ack = false;
        // payload test: i: destination
        char msg[40];
        uint32_t i = globalSentCounter + 1;
        LOG_INFO("Sent test packet %u to %u", i, dest);
        if(p->to == NODENUM_BROADCAST){
            snprintf(msg, sizeof(msg), "broadcast test: %d", i);
        }
        else{
            snprintf(msg, sizeof(msg), "test: %d", i);
        }
        size_t len = strlen(msg);
        // Safety check
        if (len > sizeof(p->decoded.payload.bytes)){
            len = sizeof(p->decoded.payload.bytes);
        }
        memcpy(p->decoded.payload.bytes, msg, len);
        p->decoded.payload.size = len;
        // send the packet
        service->sendToMesh(p);
        saveSentPacket(p->id, p->to, millis(), msg);
        return p->id;
    }
    return 0;
}

ProcessMessage ExperimentModule::handleReceived(const meshtastic_MeshPacket &mp)
{
    uint32_t timestamp = millis();
    // if packet is broadcast or is to us, increment the counter
    if ((isBroadcast(mp.to) || isToUs(&mp))) {
        savereceivedPacket(mp, timestamp);
        LOG_INFO("Received test packet from %u", mp.from);
    }
    return ProcessMessage::CONTINUE; // Let others look at this message also if they want
}


uint32_t ExperimentModule::sendToCollector()
{
    meshtastic_MeshPacket *p = router->allocForSending();
    if (p){
        p->to = COLLECTOR_NODE;
        p->decoded.portnum =  meshtastic_PortNum_PRIVATE_APP;
        p->want_ack = false;
        // Create protobuf struct
        static ExperimentStats stats = ExperimentStats_init_zero; 
        memset(&stats, 0, sizeof(stats)); 
        stats.sender_node = nodeDB->getNodeNum();
        // ------------Sent packets---------------
        stats.sent_count = 0; //intialize to 0
        for (auto &pair : nodesMap) {
        if (stats.sent_count >= 10) {
            LOG_WARN("Too many nodes, skipping remaining sent stats");
            break;
        }
            NodeInfo &nodeInfo = pair.second;
            NodeStats *nodeStats = &stats.sent[stats.sent_count];
            nodeStats->packets_count = 0;
            stats.sent_count ++;
            for (uint32_t j = nodeInfo.sentPacketsToCollector; j < nodeInfo.sentPacketsCount; j++){
                Packet &packet = nodeInfo.sentPackets[j];
                nodeStats->node_id = packet.node;
                if(nodeStats->packets_count == 3){
                    LOG_INFO("Depassed max number of packets");
                    break;
                }
                PacketEntry *packetEntry = &nodeStats->packets[nodeStats->packets_count];
                nodeStats->packets_count++;
                snprintf(packetEntry->text,
                        sizeof(packetEntry->text),
                        "%s",
                        packet.text);
                packetEntry->packet_id = packet.packetId;
                packetEntry->timestamp = packet.timestamp;
                nodeInfo.sentPacketsToCollector ++;
            }
        }
        // ------------received packets---------------
        stats.received_count = 0; //intialize to 0
        for (auto &pair : nodesMap) {
            if (stats.received_count >= 10) {
                LOG_WARN("Too many nodes, skipping remaining sent stats");
                break;
            }
            NodeInfo &nodeInfo = pair.second;
            NodeStats *nodeStats = &stats.received[stats.received_count];
            nodeStats->packets_count = 0;
            stats.received_count ++;
            for (uint32_t j = nodeInfo.receivedPacketsToCollector; j < nodeInfo.receivedPacketsCount; j++){
                Packet &packet = nodeInfo.receivedPackets[j];
                nodeStats->node_id = packet.node;
                if(nodeStats->packets_count == 3){
                    LOG_INFO("Depassed max number of packets");
                    break;
                }
                PacketEntry *packetEntry = &nodeStats->packets[nodeStats->packets_count];
                nodeStats->packets_count++;
                snprintf(packetEntry->text,
                        sizeof(packetEntry->text),
                        "%s",
                        packet.text);
                packetEntry->packet_id = packet.packetId;
                packetEntry->timestamp = packet.timestamp;
                nodeInfo.receivedPacketsToCollector ++;
            }
        }
        // Encode into payload
        if((p->decoded.payload.size = pb_encode_to_bytes(
            p->decoded.payload.bytes,
            sizeof(p->decoded.payload.bytes),
            ExperimentStats_fields,
            &stats)) == 0)
        {
            LOG_ERROR("Protobuf encode failed");
            return 0;
        }
        LOG_INFO("Encoded size: %u bytes", p->decoded.payload.size);
        service->sendToMesh(p);
        LOG_INFO("Sent to mesh");
        return p->id;
    } 
    return 0;
}

int32_t ExperimentModule::runOnce()
{
    NodeNum dest = nodes[currentDestIndex];
    currentDestIndex++;
    if (currentDestIndex >= NB_NODES){
        currentDestIndex= 0;
    }
    uint32_t id = sendPacket(dest);
    // send data to collector every 2 mins
    uint32_t now  = millis();
    // if it has been 2 mins since we last sent and one of the maps is not empty, send again
    if(now - lastStatsSent > collectorInterval )
    {
        sendToCollector();
        LOG_INFO("Sent to collector");
        lastStatsSent = now;
    }
    // run again after my_interval ms
    return(my_interval);
}
