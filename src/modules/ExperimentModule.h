#pragma once
#include "SinglePortModule.h"
#include "concurrency/OSThread.h"
#include "pb_encode.h"
#define NB_NODES 10
#define MAX_TRACKED_TIMESTAMPS 100
#define COLLECTOR_NODE 0x31c0c4f1
#define START_INTERVAL 300000 
#define PKGEN_INTERVAL 600000 // do 10 mins

/**
 * A module that sends packets periodically and listens to packets 
 * and sends its data to collector node 
 * 
 */
class ExperimentModule : public SinglePortModule,  private concurrency::OSThread
{
  public:
    /** Constructor
     * name is for debugging output
     */
    ExperimentModule() : 
    SinglePortModule("ExperimentModule", meshtastic_PortNum_PRIVATE_APP),  
    concurrency::OSThread("ExperimentModule")
    {
        // Give network time to set up
        setIntervalFromNow(START_INTERVAL);
        // Initialize statsArray
        for(auto i = 0; i< NB_NODES; i++){
            statsArray[i].nodeId = nodes[i];
            statsArray[i].num_pkgen_data_sent = 0;
            statsArray[i].num_pkgen_reply_received = 0;
            statsArray[i].num_pkgen_data_received_dm = 0;
            statsArray[i].num_pkgen_data_received_broadcasts = 0;
            statsArray[i].rtt = 0;
        }
    }
    // Our nodes
    NodeNum nodes[NB_NODES] = {
        0x52e99376, 0x49242450, 0xbf935464,
        0x6d4d1ba2, 0x9287389e, 0x33f7e0ed, 
        0x91d71daf, 0x7aa01783, 0x59d388e5,
        0x31c0c4f1
    };
    enum class PacketType: uint8_t{
        NONE,
        PKGEN_CONFIG_REQ        = 0X01,
        PKGEN_CONFIG_RESP       = 0X02,
        PKGEN_DATA              = 0X03,
        PKGEN_REPLY             = 0X04,
        STATS_GET_REQ           = 0X05,
        STATS_GET_RESP          = 0X06,
        STATS_CLEAR_REQ         = 0X07,
        STATS_CLEAR_RESP        = 0X08
    };
    enum class PkgenState{
        IDLE,
        RUNNING
    };
    struct NodeStats {
        uint32_t nodeId = 0;
        uint16_t num_pkgen_data_sent = 0;
        uint16_t num_pkgen_reply_received = 0;
        uint16_t num_pkgen_data_received_dm = 0;
        uint16_t num_pkgen_data_received_broadcasts = 0;
        uint16_t rtt = 0;      
    };
    NodeStats statsArray[NB_NODES]; //stats of the other nodes
    uint16_t num_sent_broadcasts = 0;
    uint16_t pkgen_sent_count = 0;
    // Sent Packets timestamps tracking
    uint16_t timestampsBySeqnum[MAX_TRACKED_TIMESTAMPS] = {0};
    uint16_t timestampsCount = 0;

  protected:
    /**
     * clear our local data
     */
    void clearStats();
    /**
     * Send STATS_CLEAR_RESP to the collector
     */
    uint32_t sendClearStatsResponseToCollector();
    /**
     * Send a packet to specific destination dest
     */
    uint32_t sendPkgenData(NodeNum dest, uint8_t pkgenDoReply, uint16_t seqnum);
    /**
     * SENDS PKGEN_CONFIG_RESP to the collector
     */
    uint32_t sendPkgenResponseToCollector(uint8_t cmdid);
    /**
     * Sends reply backs
     */
    uint32_t sendReply(const meshtastic_MeshPacket &mp);
    /**
     * Send our stats to collector node 
     */
    void sendStatsToCollector();
    /**
     * Parse the requests we receive from the collector node
     */
    PacketType parseCommand(const meshtastic_MeshPacket &mp);
    /**
     * Parse  PKGEN_CONFIG_REQ we receive from the collector node
     */
    bool parsePkgenCommand(const meshtastic_MeshPacket &mp,
    uint8_t&cmdid, NodeNum& dest, uint8_t& pkgenDoReply, 
    uint16_t& pkgenPeriod, uint16_t& pkgenNumpkt);
    /**
     * Called when we receive a packet
     */
    ProcessMessage handleReceived(const meshtastic_MeshPacket &mp) override;
    /**
    * Runs every my_interval ms
    */
    virtual int32_t runOnce() override;

    private:
        unsigned int my_interval = 60000; // interval in millisconds to run runOnce again
        NodeStats* getStats(NodeNum node);
        PkgenState pkgenState      = PkgenState::IDLE;
        uint8_t  cmdid             = 0;
        NodeNum pkgenDestination   = 0;
        uint8_t  pkgenDoReply      = 0;
        uint16_t pkgenPeriod       = 0;
        uint16_t pkgenNumpkt       = 0;
};

extern ExperimentModule *experimentModule;
