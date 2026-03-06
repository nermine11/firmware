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
        timestampsBySeqnum[i] = 0;
    }
    num_sent_broadcasts = 0;
    timestampsCount  = 0;
    pkgen_sent_count = 0;
    LOG_INFO("Cleared stats");
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
        LOG_INFO("Sent STATS_CLEAR_RESP to collector");
        return p->id;
    }
}

uint32_t ExperimentModule::sendPkgenData(NodeNum dest, uint8_t pkgenDoReply, uint16_t seqnum)
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
        //do_reply 1 byte
        buf[offset] = pkgenDoReply;
        offset ++;   
        // seqnum 2 bytes
        memcpy(buf + offset, &seqnum, 2);
        offset += 2;
        p->decoded.payload.size = offset;
        LOG_INFO("Encoded size of PKGEN_DATA: %u bytes", p->decoded.payload.size);
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
                timestampsBySeqnum[seqnum] = sentTime;
            }
        }
        LOG_INFO("Sent PKGEN data to %u", dest);
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
        LOG_INFO("Encoded size of pkgen response: %u bytes", p->decoded.payload.size);
        // send the packet
        service->sendToMesh(p);
        LOG_INFO("Sent PKGEN_CONFIG_RESP to collector");
        return p->id;
    }
}

uint32_t ExperimentModule::sendReply(const meshtastic_MeshPacket &mp){
    meshtastic_MeshPacket *p = allocDataPacket();
    if(!p){
        return 0;
    }
    else{
        p->to = mp.from;
        p->want_ack = false;
        p->channel = 0;
        // Payload
        uint8_t* buf = p->decoded.payload.bytes;
        uint8_t offset = 0;
        // pkktype 1 byte
        buf[offset] = static_cast<uint8_t> (PacketType::PKGEN_REPLY);
        offset ++;
        // seqnum 2 bytes
        const auto& pl = mp.decoded.payload;
        uint16_t seqnum = 0;
        memcpy(&seqnum, &pl.bytes[2], 2); 
        memcpy(buf + offset, &seqnum, 2);
        offset += 2;
        p->decoded.payload.size = offset;
        LOG_INFO("Encoded size of the reply: %u bytes", p->decoded.payload.size);
        // Send the data
        service->sendToMesh(p);
        LOG_INFO("Sent a reply to %u", p->to);
        return p->id;
    }
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
            LOG_INFO("Node 0x%x: sent=%u, replies=%u, dmRx=%u, bcRx=%u, rttAvg=%u",
                ns->nodeId,
                ns->num_pkgen_data_sent,
                ns->num_pkgen_reply_received,
                ns->num_pkgen_data_received_dm,
                ns->num_pkgen_data_received_broadcasts,
                rtt);
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
        // Requests we receive from the collector node
        if(mp.from == COLLECTOR_NODE ){
            // check what command we recieved
            auto command = parseCommand(mp);
            if(command == PacketType::PKGEN_CONFIG_REQ){
                if(parsePkgenCommand(mp, cmdid, pkgenDestination, pkgenDoReply,
                pkgenPeriod, pkgenNumpkt)){
                    LOG_INFO("Received pkgen request");
                    pkgenState = PkgenState::RUNNING;
                    sendPkgenResponseToCollector(cmdid);
                    setInterval(PKGEN_INTERVAL); // call run_once() after 10 mins
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
        // we received a broadcast 
        else if(isBroadcast(mp.to)){
            stats->num_pkgen_data_received_broadcasts ++;
        }
        /* We received a DM */
        else if(isToUs(&mp) && mp.from != COLLECTOR_NODE){
            // check if it is a dm or reply
            const auto& pl = mp.decoded.payload;
            auto size      = pl.size;
            // we received pkgen_DATA not reply
            if(size == 4){
                stats->num_pkgen_data_received_dm ++;
                // Check if want a reply
                uint8_t doReply = 0;
                memcpy(&doReply, &pl.bytes[1], 1); 
                if(doReply){
                    LOG_INFO("Received PKGEN_DATA that wants a reply");
                    uint32_t id = sendReply(mp);
                    if(!id){
                        LOG_ERROR("REPLY ID IS 0");
                    }
                }
            }
            // we received a PKGEN_REPLY so calculate the rtt
            else if (size == 3){
                stats->num_pkgen_reply_received ++;
                uint16_t seqnumReply = 0;
                memcpy(&seqnumReply, &pl.bytes[1], 2); 
                uint16_t sentTime = timestampsBySeqnum[seqnumReply];
                if(sentTime > 0){
                    uint16_t rtt = receivedTimestamp - sentTime ; // received - sent
                    stats->rtt += rtt;
                    LOG_INFO("rttsum: %u, rttCount: %u", stats->rtt, stats->num_pkgen_reply_received);
                }
            }
        }
    }
    return ProcessMessage::CONTINUE; // Let others look at this message also if they want
}

/* ----------------------------RUNS PERIODICALLY-------------------------------*/
int32_t ExperimentModule::runOnce()
{
    if(pkgenState == PkgenState::RUNNING){
        // If we sent all the PKGEN_DATA we have to or if numpkt = 0 we stop sending
        if(pkgen_sent_count >= pkgenNumpkt){
            pkgenState = PkgenState::IDLE;
            return my_interval;   // go back to normal scheduling
        }
        // Send 1 PKGEN_DATA packet every pkgenPeriod
        else{
            sendPkgenData(pkgenDestination, pkgenDoReply, pkgen_sent_count);
            pkgen_sent_count ++;
            return pkgenPeriod * 1000;  // run again after pkgenPeriod seconds
        }
    }
    else{
        return(my_interval);
    }   
}
