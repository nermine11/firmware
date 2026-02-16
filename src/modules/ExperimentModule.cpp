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
        receivedPackets[mp.from].insert(mp.id);
        LOG_INFO("Received test packet from %u", mp.from);

    }
    return ProcessMessage::CONTINUE; // Let others look at this message also if they want
}

/**
 * send periodically a packet to a destination
 * each my_interval ms, change the destination
 */
int32_t ExperimentModule::runOnce()
{

    NodeNum dest = nodes[globalCounter % LEN(nodes)];
    uint32_t i = sentPackets[dest].size();
    uint32_t id = sendPacket(i, dest);
    if(id){
        LOG_INFO("Sent test packet %u to %u", i, dest);
        sentPackets[dest].insert(id);
        globalCounter ++;
        LOG_INFO("nb global packets: %u", globalCounter);
    }
    // run again after my_interval ms
    return(my_interval);
}
