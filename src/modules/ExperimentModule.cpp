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
    Packet &p = nodeInfo.sentPackets[nodeInfo.sentCount];
    p.node = dest;
    p.packetId = packetId;
    p.timestamp = timestamp;
    snprintf(p.text,
            sizeof(p.text),
            "%s",
            text);
   nodeInfo.sentCount++;
}

uint32_t ExperimentModule::sendPacket(int i, NodeNum dest)
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
        snprintf(msg, sizeof(msg), "test: %d", i);
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

/*ProcessMessage ExperimentModule::handleReceived(const meshtastic_MeshPacket &mp)
{
    LOG_INFO("ExperimentModule handleReceived called");
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
}*/


uint32_t ExperimentModule::sendToCollector()
{
    meshtastic_MeshPacket *p = router->allocForSending();
    if (p){
        p->to = COLLECTOR_NODE;
        p->decoded.portnum =  meshtastic_PortNum_PRIVATE_APP;
        p->want_ack = false;
        // Create protobuf struct
        static ExperimentStats stats = ExperimentStats_init_zero;  
        stats.sender_node = nodeDB->getNodeNum();
        // Sent packets
        stats.sent_count = 0; //intialize to 0
        for (auto &pair : nodesMap) {
            LOG_INFO("HERE");
            NodeInfo &nodeInfo = pair.second;
            NodeStats *nodeStats = &stats.sent[stats.sent_count];
            nodeStats->packets_count = 0;
            stats.sent_count ++;
            for (uint32_t j = nodeInfo.sentToCollector; j < nodeInfo.sentCount; j++){
                LOG_INFO("sentCount: %u", j);
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
                nodeInfo.sentToCollector ++;
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
    NodeNum dest = nodes[globalCounter % LEN(nodes)];
    LOG_INFO(" dest: %u, globalCounter:%u, LEN(nodes): %u ", dest, globalCounter, LEN(nodes));
    uint32_t i = nodesMap[dest].sentCount + 1;
    uint32_t id = sendPacket(i, dest);
    if(id){
        LOG_INFO("Sent test packet %u to %u", i, dest);
        globalCounter ++;
        LOG_INFO("nb global packets: %u", globalCounter);
    }
    // send data to collector every 2 mins
    uint32_t now  = millis();
    // if it has been 2 mins since we last sent and one of the maps is not empty, send again
    if(now - lastStatsSent > 80000 )
    {
        sendToCollector();
        LOG_INFO("Sent to collector");
        lastStatsSent = now;
    }
    // run again after my_interval ms
    return(my_interval);
}
