#include "ExperimentModule.h"
#include "MeshService.h"
#include "configuration.h"
#include "Router.h"
#include "main.h"
#include <sys/stat.h>
ExperimentModule *experimentModule;

/* ------------------PRIVATE HELPER METHODS------------------*/
ExperimentModule::NodeStats* ExperimentModule::getStats(NodeNum node){
    for(auto i = 0; i< NB_NODES; i++){
        if(statsArray[i].nodeId == node){
            return &statsArray[i];
        }
    }
    return nullptr;
}

uint32_t ExperimentModule::getTimestamp(uint32_t id){
    for(auto i = 0; i< timestampsCount; i++){
        if (timestamps[i].packetId == id){
            uint32_t time = timestamps[i].timestamp;
            timestamps[i].packetId = 0;
            timestamps[i].timestamp = 0;
            return time;
        }
    }
    return 0;
}

/* ---------------------CLEAR_STATS---------------------------*/
void ExperimentModule::clearStats(){
    // initialize statsArray
    for(auto i = 0; i< NB_NODES; i++){
        statsArray[i].num_pkgen_data_sent = 0;
        statsArray[i].num_pkgen_reply_received = 0;
        statsArray[i].num_pkgen_data_received_dm = 0;
        statsArray[i].num_pkgen_data_received_broadcasts = 0;
        statsArray[i].rtt = 0;
    }
    for(auto i = 0; i< MAX_TRACKED_TIMESTAMPS; i++){
        timestamps[i].packetId = 0;
        timestamps[i].timestamp = 0;
    }
    num_sent_broadcasts = 0;
    timestampsCount  = 0;
}

/* ----------------------SEND---------------------------------*/
uint32_t ExperimentModule::sendClearStatsResponseToCollector(){
    uint32_t packetTimestamp = millis();
    meshtastic_MeshPacket *p = allocDataPacket();
    if(!p){
        return 0;
    }
    else{
        p->to = COLLECTOR_NODE;
        p->want_ack = false;
        p->channel = 0;
        p->decoded.payload.bytes[0] = static_cast<uint8_t> (PacketType::STATS_CLEAR_RESP);
        p->decoded.payload.size = 1;
        // send the packet
        service->sendToMesh(p);
        LOG_INFO("Sent STATS_CLEAR_REQ to collector");
        return p->id;
    }
}

uint32_t ExperimentModule::sendPkgenData(NodeNum dest, uint8_t pkgenDoReply, uint16_t seqnum)
{   
    uint32_t packetTimestamp = millis();
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
        // Check if we should send back a response
        if(pkgenDoReply){
            /* Ask for response back to get rtt */
            p->decoded.want_response = true;
        }
        NodeStats* stats = nullptr;
        if(!isBroadcast(p->to)){
            /* initiate stats so if it fails we return before sending */
            stats = getStats(dest);
            if(!stats){
                return 0;
            }
        }
        // Payload
        uint8_t* buf = p->decoded.payload.bytes;
        uint8_t offset = 0;
        // pkktype 1 byte
        buf[offset] = static_cast<uint8_t> (PacketType::PKGEN_DATA);
        offset ++;
        // seqnum 2 bytes
        memcpy(buf + offset, &seqnum, 2);
        offset += 2;
        p->decoded.payload.size = offset;
        LOG_INFO("Encoded size of the reply: %u bytes", p->decoded.payload.size);
        // send the packet
        service->sendToMesh(p);
        //----Save the stats----      
        uint16_t sentTime = millis();
        if(isBroadcast(p->to)){
            num_sent_broadcasts++;
        }
        else{
            stats->num_pkgen_data_sent++;
            // when was this packet sent
            if(timestampsCount >= MAX_TRACKED_TIMESTAMPS){
                LOG_ERROR(" We depassed the maximum number of timestamps, should not happen!");
            }
            else{
                timestamps[timestampsCount].packetId = p->id;
                timestamps[timestampsCount].timestamp = sentTime;
                LOG_INFO("id: %u, time:%u", timestamps[timestampsCount].packetId, timestamps[timestampsCount].timestamp);
                timestampsCount++;
            }

        }
        return p->id;
    }
}

uint32_t ExperimentModule::sendPkgenResponseToCollector(uint8_t cmdid){
    meshtastic_MeshPacket *p = allocDataPacket();
    if(!p){
        return 0;
    }
    else{
        p->to = COLLECTOR_NODE;
        p->want_ack = false;
        p->channel = 0;
        // Payload
        uint8_t* buf = p->decoded.payload.bytes;
        uint8_t offset = 0;
        // pkktype 1 byte
        buf[offset] = static_cast<uint8_t> (PacketType::PKGEN_CONFIG_RESP);
        offset ++;
        // cmdid 1 byte
        memcpy(buf + offset, &cmdid, 1);
        offset += 1;
        p->decoded.payload.size = offset;
        LOG_INFO("Encoded size of the reply: %u bytes", p->decoded.payload.size);
        // send the packet
        service->sendToMesh(p);
        LOG_INFO("Sent PKGEN_CONFIG_RESP to collector");
        return p->id;
    }
}

uint32_t ExperimentModule::sendReply(const meshtastic_MeshPacket &mp){
    const auto& pl = mp.decoded.payload;
    // Get the seqnum of the packet that want a reply back
    uint16_t seqnum = 0;                               
    memcpy(&seqnum, &pl.bytes[2], 2);
    auto reply = allocDataPacket();                 // Allocate a packet for sending
    reply->to = mp.from;
    reply->want_ack = false;
    reply->channel = 0;
    reply->decoded.reply_id = mp.id;                // indicate its a reply
    uint8_t* buf = reply->decoded.payload.bytes;
    uint8_t offset = 0;
    // pkktype 1 byte
    buf[offset] = static_cast<uint8_t> (PacketType::PKGEN_REPLY);
    offset ++;
    // seqnum 2 bytes
    memcpy(buf + offset, &seqnum, 2);
    offset += 2;
    reply->decoded.payload.size = offset;
    LOG_INFO("Encoded size of the reply: %u bytes", reply->decoded.payload.size);
    service->sendToMesh(reply);
    return reply->id;
}

void ExperimentModule::sendStatsToCollector(){
    uint32_t packetTimestamp = millis();
    meshtastic_MeshPacket *p = allocDataPacket();
    if(!p){
        return;
    }
    else{
        p->to = COLLECTOR_NODE;
        p->want_ack = false;
        p->channel = 0; 
        uint8_t* buf = p->decoded.payload.bytes;
        uint32_t offset = 0;
        // 1. pkttype 1 byte
        buf[offset] = static_cast<uint8_t> (PacketType::STATS_GET_RESP);
        offset ++;
        //2. number of sent broadcasts 2 bytes
        memcpy(buf + offset, &num_sent_broadcasts, 2);
        offset += 2;
        //3. Node stats blocks 14 bytes per node
        for (auto i = 0; i< NB_NODES ; i++) {
            if(nodes[i] == nodeDB->getNodeNum()){
                continue;
            }
            NodeStats *ns = getStats(nodes[i]);
            if(!ns){
                continue;
            }
            memcpy(buf + offset, &ns->nodeId , 4);
            offset += 4;
            memcpy(buf + offset, &ns->num_pkgen_data_sent , 2);
            offset += 2;
            memcpy(buf + offset, &ns->num_pkgen_reply_received , 2);
            offset += 2;
            memcpy(buf + offset, &ns->num_pkgen_data_received_dm , 2);
            offset += 2;
            memcpy(buf + offset, &ns->num_pkgen_data_received_broadcasts , 2);
            offset += 2;
            uint16_t rtt = 0;
            if(ns->num_pkgen_reply_received){
                rtt = (ns->rtt/ ns->num_pkgen_reply_received) / 1000; // to see if i should count it or send rtt directly
            }
            memcpy(buf + offset, &rtt , 2);
            offset += 2;   
        }
        p->decoded.payload.size = offset;
        LOG_INFO("Encoded size: %u bytes", p->decoded.payload.size);
        // send the packet
        service->sendToMesh(p);
        LOG_INFO("Sent stats");
    }
}

/* ----------------------------RECEIVE-------------------------------*/
ExperimentModule::PacketType ExperimentModule::parseCommand(const meshtastic_MeshPacket &mp){
    const auto& pl = mp.decoded.payload;
    if (pl.size < 1){
        return PacketType::NONE;
    }
    uint8_t cmd = pl.bytes[0];
    switch(cmd){
        case static_cast<uint8_t> (PacketType::STATS_CLEAR_REQ) :
            return PacketType::STATS_CLEAR_REQ;
        case static_cast<uint8_t>( PacketType:: PKGEN_CONFIG_REQ):
            return PacketType:: PKGEN_CONFIG_REQ;
        case static_cast<uint8_t>(PacketType:: STATS_GET_REQ):
            return PacketType:: STATS_GET_REQ;
        default:
            return PacketType:: NONE;
    }
}

bool ExperimentModule::parsePkgenCommand(
    const meshtastic_MeshPacket &mp,
    uint8_t&cmdid, 
    NodeNum& dest, 
    uint8_t& pkgenDoReply,
    uint16_t& pkgenPeriod, 
    uint16_t& pkgenNumpkt
){
    const auto& pl = mp.decoded.payload;
    if(pl.size < 10){
        return false;
    }
    memcpy(&cmdid, &pl.bytes[1], 1);
    memcpy(&dest, &pl.bytes[2], 4);
    memcpy(&pkgenDoReply, &pl.bytes[6], 1);
    memcpy(&pkgenPeriod, &pl.bytes[7], 2 );
    memcpy(&pkgenNumpkt, &pl.bytes[9], 2);
    return true;
}

ProcessMessage ExperimentModule::handleReceived(const meshtastic_MeshPacket &mp)
{
    // If packet is broadcast or is to us, increment the counter
    uint32_t receivedTimestamp = millis();
    // don't count packets that we act as a relay for
    if(!isBroadcast(mp.to) && !isToUs(&mp)){
        return ProcessMessage::CONTINUE;
    }
    else{
        NodeStats *stats = getStats(mp.from);
        if(!stats){
            return ProcessMessage::CONTINUE;
        }
        LOG_INFO("Received test packet from %u", mp.from);
        /* We received a DM, 
           Don't cound replies or requests from Collector node as received DMs
        */
        if(isToUs(&mp) && !(mp.decoded.reply_id) && mp.from != COLLECTOR_NODE){
            stats->num_pkgen_data_received_dm ++;
        }
        // We received a reply
        else if(isToUs(&mp) && mp.decoded.reply_id){
            stats->num_pkgen_reply_received ++;
        }
        // don't count what we receive as broadcasts from the collector
        else if(isBroadcast(mp.to) && mp.from != COLLECTOR_NODE){
            stats->num_pkgen_data_received_broadcasts ++;
        }
        // Requests we receive from the collector node
        else if(mp.from == COLLECTOR_NODE ){
            // check what command we recieved
            auto command = parseCommand(mp);
            if(command == PacketType::PKGEN_CONFIG_REQ){
                if(parsePkgenCommand(mp, cmdid, pkgenDestination, pkgenDoReply,
                pkgenPeriod, pkgenNumpkt)){
                    LOG_INFO("Received pkgen request");
                    pkgenState = PkgenState::RUNNING;
                };
            }
            else if(command == PacketType::STATS_GET_REQ){
                LOG_INFO("Received stats request");
                sendStatsToCollector();
            }
            else if(command == PacketType::STATS_CLEAR_REQ){
                LOG_INFO("Received Clear stats request");
                clearStats();
                uint32_t id = sendClearStatsResponseToCollector();
                if(!id){
                    LOG_ERROR(" Response ID is 0");
                }
            }
        }
        // if want response, then send the reply back
        if(mp.decoded.want_response == true){
            LOG_INFO("Received PKGEN_DATA that wants a reply");
            uint32_t id = sendReply(mp);
            if(!id){
                LOG_ERROR("REPLY ID IS 0");
            }
        }
        // if packet received is a response, calculate and save the rtt
        uint32_t sentTime = getTimestamp(mp.decoded.reply_id);
        if(mp.decoded.reply_id && sentTime){
            uint16_t rtt = receivedTimestamp - sentTime ; // received - sent
            stats->rtt += rtt;
            LOG_INFO("rttsum: %u, rttCount: %u", stats->rtt, stats->num_pkgen_reply_received);
        }
    }
    return ProcessMessage::CONTINUE; // Let others look at this message also if they want
}

/* ----------------------------RUNS PERIODICALLY-------------------------------*/
int32_t ExperimentModule::runOnce()
{
    static uint16_t num_pkgen_data_sent = 0;
    if(pkgenState == PkgenState::RUNNING){
        // If we sent all the PKGEN_DATA we have to or if numpkt = 0 we stop sending
        if(num_pkgen_data_sent >= pkgenNumpkt){
            pkgenState = PkgenState::IDLE;
            sendPkgenResponseToCollector(cmdid);
            return my_interval;   // go back to normal scheduling
        }
        // Send 1 PKGEN_DATA packet every pkgenPeriod
        else{
            while(num_pkgen_data_sent < pkgenNumpkt){
                sendPkgenData(pkgenDestination, pkgenDoReply, num_pkgen_data_sent);
                num_pkgen_data_sent ++;
                return pkgenPeriod * 1000;  // run again after pkgenPeriod seconds
            }
        }
    }
    else{
        return(my_interval);
    }   
}
