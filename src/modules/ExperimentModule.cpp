#include "ExperimentModule.h"
#include "MeshService.h"
#include "configuration.h"
#include "Router.h"
#include "main.h"
#include <sys/stat.h>

ExperimentModule *experimentModule;

ExperimentModule::NodeStats* ExperimentModule::getStats(NodeNum node){
    for(auto i = 0; i< NB_NODES; i++){
        if(statsArray[i].nodeId == node){
            return &statsArray[i];
        }
    }
    return nullptr;
}

uint32_t ExperimentModule::sendPacket(NodeNum dest)
{   
    // don't send packet to myself
    if(dest == nodeDB->getNodeNum()){
        return 0;
    }
    meshtastic_MeshPacket *p = allocDataPacket();
    if(!p){
        return 0;
    }
    else{
        p->to = dest;
        p->want_ack = false;
        p->channel = 0;
        if(!isBroadcast(p->to)){
            /* Ask for response back to get rtt
             in case of DM only to avoid congestions */
            p->decoded.want_response = true;
        }
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
        NodeStats* stats = nullptr;
        if(!isBroadcast(p->to)){
            /* initiate stats so if it fails we return before 0 sending */
            stats = getStats(dest);
            if(!stats){
                return 0;
            }
        }
        // send the packet
        service->sendToMesh(p);
        uint32_t sentTime = millis();
        //----save the data----
        if(isBroadcast(p->to)){
            sentBroadcasts++;
        }
        else{
            stats->dmSent++;
            // when was this packet sent
            if(timestampsCount >= MAX_TRACKED_TIMESTAMPS){
                timestampsCount = 0;
            }
            timestamps[timestampsCount].packetId = p->id;
            timestamps[timestampsCount].timestamp = sentTime;
            LOG_INFO("id: %u, time:%u", timestamps[timestampsCount].packetId, timestamps[timestampsCount].timestamp);
            timestampsCount++;
        }
        globalSentCounter++;
        return p->id;
        }
}

uint32_t ExperimentModule::getTimestamp(uint32_t id){
    for(auto i = 0; i< MAX_TRACKED_TIMESTAMPS; i++){
        if (timestamps[i].packetId == id){
            uint32_t time = timestamps[i].timestamp;
            timestamps[i].packetId = 0;
            timestamps[i].timestamp = 0;
            return time;
        }
    }
    return 0;
}

ProcessMessage ExperimentModule::handleReceived(const meshtastic_MeshPacket &mp)
{
    // if packet is broadcast or is to us, increment the counter
    uint32_t receivedTimestamp = millis();
    if ((isBroadcast(mp.to) || isToUs(&mp))) {
        NodeStats *stats = getStats(mp.from);
        if(!stats){
            return ProcessMessage::CONTINUE;
        }
        if(isBroadcast(mp.to)){
            stats->broadcastsReceived ++;
        }
        // dont count replies as Received DMs
        else if(isToUs(&mp) && !(mp.decoded.reply_id)){
            stats->dmReceived++;
        }
        LOG_INFO("Received test packet from %u", mp.from);
        // if want response, then send the packet back
        if(mp.decoded.want_response == true){
            LOG_INFO("replyyy");
            const char *replyStr = "Message Received";
            auto reply = allocDataPacket();                 // Allocate a packet for sending
            reply->to = mp.from;
            reply->want_ack = false;
            reply->channel = 0;
            reply->decoded.reply_id = mp.id;                // indicate its a reply
            reply->decoded.payload.size = strlen(replyStr); // You must specify how many bytes are in the reply
            memcpy(reply->decoded.payload.bytes, replyStr, reply->decoded.payload.size);
            service->sendToMesh(reply);
        }
        // if packet received is a response, calculate and save the rtt
        uint32_t sentTime = getTimestamp(mp.decoded.reply_id);
        if(mp.decoded.reply_id && sentTime){
            uint32_t rtt = receivedTimestamp - sentTime ; // received - sent
            stats->rttSum += rtt;
            stats->rttCount ++;
            LOG_INFO("rttsum: %u, rttCount: %u", stats->rttSum, stats->rttCount );
        }
    }
    return ProcessMessage::CONTINUE; // Let others look at this message also if they want
}

void ExperimentModule::sendToCollector()
{
    meshtastic_MeshPacket *p = router->allocForSending();
    if(!p){
        return;
    }
    else{
        p->to = COLLECTOR_NODE;
        p->decoded.portnum =  meshtastic_PortNum_PRIVATE_APP;
        p->want_ack = false;
        p->channel = 0;
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
        for (auto i = 0; i< NB_NODES ; i++) {
            if(nodes[i] == nodeDB->getNodeNum()){
                continue;
            }
            if(experimentStats.stats_count >=10){
                break;
            }
            NodeStats *ns = getStats(nodes[i]);
            if(!ns){
                continue;
            }
            _NodeStats* nodeStats = &experimentStats.stats[experimentStats.stats_count];
            nodeStats->node_id = ns->nodeId;
            nodeStats->dm_sent = ns->dmSent;
            nodeStats->dm_received = ns->dmReceived;
            nodeStats->broadcasts_received = ns->broadcastsReceived;
            nodeStats ->rtt_sum = ns->rttSum;
            nodeStats->rtt_count = ns->rttCount;
            experimentStats.stats_count++;
        }
        experimentStats.sentBroadcasts = sentBroadcasts;
        // Encode into payload
        if((p->decoded.payload.size = pb_encode_to_bytes(
            p->decoded.payload.bytes,
            sizeof(p->decoded.payload.bytes),
            ExperimentStats_fields,
            &experimentStats)) == 0){
            LOG_ERROR("Protobuf encode failed");
            return;
        }
        LOG_INFO("Encoded size: %u bytes", p->decoded.payload.size);
        service->sendToMesh(p);
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
    if (currentDestIndex >= NB_NODES + 1){
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
