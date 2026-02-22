#include "ExperimentModule.h"
#include "MeshService.h"
#include "configuration.h"
#include "Router.h"
#include "main.h"
#include <sys/stat.h>

ExperimentModule *experimentModule;


uint32_t ExperimentModule::sendPacket(NodeNum dest)
{   
    // don't send packet to myself
    if(dest == nodeDB->getNodeNum()){
        return 0;
    }
    meshtastic_MeshPacket *p = router->allocForSending();
    if(!p){
        return 0;
    }
    else{
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
        //----save the data----
        NodeStats &stats = statsMap[dest];
        stats.nodeId = dest;
        if(p->to == NODENUM_BROADCAST){
            stats.broadcastSent ++;
        }
        else{
            stats.dmSent++;
        }
        globalSentCounter++;
        return p->id;
    }
}

ProcessMessage ExperimentModule::handleReceived(const meshtastic_MeshPacket &mp)
{
    // if packet is broadcast or is to us, increment the counter
    if ((isBroadcast(mp.to) || isToUs(&mp))) {
        NodeStats &stats = statsMap[mp.from];
        stats.nodeId = mp.from;
        if(mp.to == NODENUM_BROADCAST){
            stats.broadcastReceived ++;
        }
        else{
            stats.dmReceived++;
        }
        LOG_INFO("Received test packet from %u", mp.from);
    }
    return ProcessMessage::CONTINUE; // Let others look at this message also if they want
}

uint32_t ExperimentModule::sendToCollector()
{
    meshtastic_MeshPacket *p = router->allocForSending();
    if(!p){
        return 0;
    }
    else{
        p->to = COLLECTOR_NODE;
        p->decoded.portnum =  meshtastic_PortNum_PRIVATE_APP;
        p->want_ack = false;
        // Create protobuf struct
        static ExperimentStats experimentStats = ExperimentStats_init_zero; 
        memset(&experimentStats, 0, sizeof(experimentStats)); 
        experimentStats.sender_node = nodeDB->getNodeNum();
        // ------------stats to send---------------
        /*
            Each interval, we send our stats for each node in the experiment
            Each interval, the data in the collector is overwritten with the new updates
        */
        experimentStats.stats_count = 0; //intialize to 0
        for (auto &pair : statsMap) {
            if(experimentStats.stats_count >=10){
                break;
            }
            NodeStats &ns = pair.second;
            _NodeStats* nodeStats = &experimentStats.stats[experimentStats.stats_count];
            nodeStats->node_id = ns.nodeId;
            nodeStats->dm_sent = ns.dmSent;
            nodeStats->dm_received = ns.dmReceived;
            nodeStats->broadcast_sent = ns.broadcastSent;
            nodeStats->broadcast_received = ns.broadcastReceived;
            nodeStats ->rtt_sum = ns.rttSum;
            nodeStats->rtt_count = ns.rttCount;
            experimentStats.stats_count++;
        }
        /*experimentStats.broadcasts_received_count = 0;
        for(auto i = sentBroadcastRecords; i< broadcastRecordCount; i++){
            _broadcastsReceived* broadcastsReceived = &experimentStats.broadcasts_received[experimentStats.broadcasts_received_count];
            BroadcastRecord& broadcastRecord = recentReceivedBroadcasts[i];
            broadcastsReceived -> sender = broadcastRecord.sender;
            broadcastsReceived -> packet_id = broadcastRecord.packetId;
            experimentStats.broadcasts_received_count++;
            sentBroadcastRecords++;
        }*/
        // Encode into payload
        if((p->decoded.payload.size = pb_encode_to_bytes(
            p->decoded.payload.bytes,
            sizeof(p->decoded.payload.bytes),
            ExperimentStats_fields,
            &experimentStats)) == 0){
            LOG_ERROR("Protobuf encode failed");
            return 0;
        }
        LOG_INFO("Encoded size: %u bytes", p->decoded.payload.size);
        service->sendToMesh(p);
        return p->id;
    }
}

int32_t ExperimentModule::runOnce()
{
    /*
    send to each destination in order per interval
    and circle back to the beginning of the nodes list after you
    sent to each destination
    */ 
    NodeNum dest = nodes[currentDestIndex];
    currentDestIndex++;
    if (currentDestIndex >= NB_NODES){
        currentDestIndex= 0;
    }
    uint32_t id = sendPacket(dest);
    // send data to collector every interval
    uint32_t now  = millis();;
    if(now - lastStatsSent > collectorInterval){
        sendToCollector();
        LOG_INFO("Sent to collector");
        lastStatsSent = now;
        collectorInterval = random(100000,180000);
    }
    // run again after my_interval ms + random delay
    return(my_interval + random(-20000, 20000));
}
