#pragma once

#include <assert.h>
#include <condition_variable>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <queue>
#include <set>
#include <tuple>
#include <vector>

#include "conf/conf.hpp"
#include "connection_manager.h"
#include "derecho_internal.h"
#include "derecho_modes.h"
#include "derecho_sst.h"
#include "mutils-serialization/SerializationMacros.hpp"
#include "mutils-serialization/SerializationSupport.hpp"
#include "rdmc/rdmc.h"
#include "spdlog/spdlog.h"
#include "sst/multicast.h"
#include "sst/sst.h"
#include "subgroup_info.h"

namespace derecho {

/**
 * Bundles together a set of callback functions for message delivery events.
 * These will be invoked by MulticastGroup or ViewManager to hand control back
 * to the client if it wants to implement custom logic to respond to each
 * message's arrival. (Note, this is a client-facing constructor argument,
 * not an internal data structure).
 */
struct CallbackSet {
    message_callback_t global_stability_callback;
    persistence_callback_t local_persistence_callback = nullptr;
    persistence_callback_t global_persistence_callback = nullptr;
};

/**
 * Bundles together a set of low-level parameters for configuring Derecho groups.
 * All of the parameters except max payload size and block size have sensible
 * defaults, but the correct block size to set depends on the user's desired max
 * payload size.
 */
struct DerechoParams : public mutils::ByteRepresentable {
    long long unsigned int max_payload_size;
    long long unsigned int max_smc_payload_size;
    long long unsigned int block_size;
    unsigned int window_size;
    unsigned int timeout_ms;
    rdmc::send_algorithm rdmc_send_algorithm;
    uint32_t rpc_port;

    DerechoParams() {
        max_payload_size = derecho::getConfUInt64(CONF_DERECHO_MAX_PAYLOAD_SIZE);
        max_smc_payload_size = std::min((uint64_t)max_payload_size, derecho::getConfUInt64(CONF_DERECHO_MAX_SMC_PAYLOAD_SIZE));
        block_size = derecho::getConfUInt64(CONF_DERECHO_BLOCK_SIZE);
        window_size = derecho::getConfUInt32(CONF_DERECHO_WINDOW_SIZE);
        timeout_ms = derecho::getConfUInt32(CONF_DERECHO_TIMEOUT_MS);
        std::string rdmc_send_algorithm_string = derecho::getConfString(CONF_DERECHO_RDMC_SEND_ALGORITHM);
        if(rdmc_send_algorithm_string == "binomial_send") {
            rdmc_send_algorithm = rdmc::send_algorithm::BINOMIAL_SEND;
        } else if(rdmc_send_algorithm_string == "chain_send") {
            rdmc_send_algorithm = rdmc::send_algorithm::CHAIN_SEND;
        } else if(rdmc_send_algorithm_string == "sequential_send") {
            rdmc_send_algorithm = rdmc::send_algorithm::SEQUENTIAL_SEND;
        } else if(rdmc_send_algorithm_string == "tree_send") {
            rdmc_send_algorithm = rdmc::send_algorithm::TREE_SEND;
        } else {
            throw "wrong value for RDMC send algorithm: " + rdmc_send_algorithm_string + ". Check your config file.";
        }
        rpc_port = derecho::getConfUInt32(CONF_DERECHO_RPC_PORT);
    }

    DerechoParams(long long unsigned int max_payload_size,
                  long long unsigned int max_smc_payload_size,
                  long long unsigned int block_size,
                  unsigned int window_size,
                  unsigned int timeout_ms,
                  rdmc::send_algorithm rdmc_send_algorithm,
                  uint32_t rpc_port)
            : max_payload_size(max_payload_size),
              max_smc_payload_size(max_smc_payload_size),
              block_size(block_size),
              window_size(window_size),
              timeout_ms(timeout_ms),
              rdmc_send_algorithm(rdmc_send_algorithm),
              rpc_port(rpc_port) {
    }

    DEFAULT_SERIALIZATION_SUPPORT(DerechoParams, max_payload_size, max_smc_payload_size, block_size, window_size, timeout_ms, rdmc_send_algorithm, rpc_port);
};

/**
 * The header for an individual multicast message, which will always be the
 * first sizeof(header) bytes in the message's data buffer.
 */
struct __attribute__((__packed__)) header {
    uint32_t header_size;
    int32_t index;
    uint64_t timestamp;
    bool cooked_send;
};

/**
 * Represents a block of memory used to store a message. This object contains
 * both the array of bytes in which the message is stored and the corresponding
 * RDMA memory region (which has registered that array of bytes as its buffer).
 * This is a move-only type, since memory regions can't be copied.
 */
struct MessageBuffer {
    std::unique_ptr<char[]> buffer;
    std::shared_ptr<rdma::memory_region> mr;

    MessageBuffer() {}
    MessageBuffer(size_t size) {
        if(size != 0) {
            buffer = std::unique_ptr<char[]>(new char[size]);
            mr = std::make_shared<rdma::memory_region>(buffer.get(), size);
        }
    }
    MessageBuffer(const MessageBuffer&) = delete;
    MessageBuffer(MessageBuffer&&) = default;
    MessageBuffer& operator=(const MessageBuffer&) = delete;
    MessageBuffer& operator=(MessageBuffer&&) = default;
};

/**
 * A structure containing an RDMC message (which consists of some bytes in a
 * registered memory region) and some associated metadata. Note that the
 * metadata (sender_id, index, etc.) is only stored locally, not sent over the
 * network with the message.
 */
struct RDMCMessage {
    /** The unique node ID of the message's sender. */
    uint32_t sender_id;
    /** The message's index (relative to other messages sent by that sender). */
    //long long int index;
    message_id_t index;
    /** The message's size in bytes. */
    long long unsigned int size;
    /** The MessageBuffer that contains the message's body. */
    MessageBuffer message_buffer;
};

struct SSTMessage {
    /** The unique node ID of the message's sender. */
    uint32_t sender_id;
    /** The message's index (relative to other messages sent by that sender). */
    int32_t index;
    /** The message's size in bytes. */
    long long unsigned int size;
    /** Pointer to the message */
    volatile char* buf;
};

/**
 * A collection of settings for a single subgroup that this node is a member of.
 * Mostly extracted from SubView, but tailored specifically to what MulticastGroup
 * needs to know about subgroups and shards.
 */
struct SubgroupSettings {
    /** This node's shard number within the subgroup */
    uint32_t shard_num;
    /** This node's rank within its shard of the subgroup */
    uint32_t shard_rank;
    /** The members of the subgroup */
    std::vector<node_id_t> members;
    /** The "is_sender" flags for members of the subgroup */
    std::vector<int> senders;
    /** This node's sender rank within the subgroup (as defined by SubView::sender_rank_of) */
    int sender_rank;
    /** The offset of this node's num_received counter within the subgroup's SST section */
    uint32_t num_received_offset;
    /** The operation mode of the subgroup */
    Mode mode;
};

/** Implements the low-level mechanics of tracking multicasts in a Derecho group,
 * using RDMC to deliver messages and SST to track their arrival and stability.
 * This class should only be used as part of a Group, since it does not know how
 * to handle failures. */
class MulticastGroup {
    friend class ViewManager;

private:
    whenlog(std::shared_ptr<spdlog::logger> logger;)
            /** vector of member id's */
            std::vector<node_id_t> members;
    /** inverse map of node_ids to sst_row */
    std::map<node_id_t, uint32_t> node_id_to_sst_index;
    /**  number of members */
    const unsigned int num_members;
    /** index of the local node in the members vector, which should also be its row index in the SST */
    const int member_index;

public:
    /** Block size used for message transfer.
     * we keep it simple; one block size for messages from all senders */
    const long long unsigned int block_size;
    // maximum size of any message that can be sent
    const long long unsigned int max_msg_size;
    // maximum size of message that can be sent using SST multicast
    const long long unsigned int sst_max_msg_size;
    /** Send algorithm for constructing a multicast from point-to-point unicast.
     *  Binomial pipeline by default. */
    const rdmc::send_algorithm rdmc_send_algorithm;
    const unsigned int window_size;

private:
    /** Message-delivery event callbacks, supplied by the client, for "raw" sends */
    const CallbackSet callbacks;
    uint32_t total_num_subgroups;
    /** Maps subgroup IDs (for subgroups this node is a member of) to an immutable
     * set of configuration options for that subgroup. */
    const std::map<subgroup_id_t, SubgroupSettings> subgroup_settings;
    /** Used for synchronizing receives by RDMC and SST */
    std::vector<std::list<int32_t>> received_intervals;
    /** Maps subgroup IDs for which this node is a sender to the RDMC group it should use to send.
     * Constructed incrementally in create_rdmc_sst_groups(), so it can't be const.  */
    std::map<subgroup_id_t, uint32_t> subgroup_to_rdmc_group;
    /** These two callbacks are internal, not exposed to clients, so they're not in CallbackSet */
    rpc_handler_t rpc_callback;

    /** Offset to add to member ranks to form RDMC group numbers. */
    uint16_t rdmc_group_num_offset;
    /** false if RDMC groups haven't been created successfully */
    bool rdmc_sst_groups_created = false;
    /** Stores message buffers not currently in use. Protected by
     * msg_state_mtx */
    std::map<uint32_t, std::vector<MessageBuffer>> free_message_buffers;

    /** Index to be used the next time get_sendbuffer_ptr is called.
     * When next_message is not none, then next_message.index = future_message_index-1 */
    std::vector<message_id_t> future_message_indices;

    /** next_message is the message that will be sent when send is called the next time.
     * It is std::nullopt when there is no message to send. */
    std::vector<std::optional<RDMCMessage>> next_sends;
    std::map<uint32_t, bool> pending_sst_sends;
    /** Messages that are ready to be sent, but must wait until the current send finishes. */
    std::vector<std::queue<RDMCMessage>> pending_sends;
    /** Vector of messages that are currently being sent out using RDMC, or boost::none otherwise. */
    /** one per subgroup */
    std::vector<std::optional<RDMCMessage>> current_sends;

    /** Messages that are currently being received. */
    std::map<std::pair<subgroup_id_t, node_id_t>, RDMCMessage> current_receives;

    /** Messages that have finished sending/receiving but aren't yet globally stable.
     * Organized by [subgroup number] -> [sequence number] -> [message] */
    std::map<subgroup_id_t, std::map<message_id_t, RDMCMessage>> locally_stable_rdmc_messages;
    /** Same map as locally_stable_rdmc_messages, but for SST messages */
    std::map<subgroup_id_t, std::map<message_id_t, SSTMessage>> locally_stable_sst_messages;
    std::map<subgroup_id_t, std::set<uint64_t>> pending_message_timestamps;
    std::map<subgroup_id_t, std::map<message_id_t, uint64_t>> pending_persistence;
    /** Messages that are currently being written to persistent storage */
    std::map<subgroup_id_t, std::map<message_id_t, RDMCMessage>> non_persistent_messages;
    /** Messages that are currently being written to persistent storage */
    std::map<subgroup_id_t, std::map<message_id_t, SSTMessage>> non_persistent_sst_messages;

    std::vector<message_id_t> next_message_to_deliver;
    std::mutex msg_state_mtx;
    std::condition_variable sender_cv;

    /** The time, in milliseconds, that a sender can wait to send a message before it is considered failed. */
    unsigned int sender_timeout;

    /** Indicates that the group is being destroyed. */
    std::atomic<bool> thread_shutdown{false};
    /** The background thread that sends messages with RDMC. */
    std::thread sender_thread;

    std::thread timeout_thread;

    /** The SST, shared between this group and its GMS. */
    std::shared_ptr<DerechoSST> sst;

    /** The SSTs for multicasts **/
    std::vector<std::unique_ptr<sst::multicast_group<DerechoSST>>> sst_multicast_group_ptrs;

    using pred_handle = typename sst::Predicates<DerechoSST>::pred_handle;
    std::list<pred_handle> receiver_pred_handles;
    std::list<pred_handle> stability_pred_handles;
    std::list<pred_handle> delivery_pred_handles;
    std::list<pred_handle> persistence_pred_handles;
    std::list<pred_handle> sender_pred_handles;

    std::vector<bool> last_transfer_medium;

    /** post the next version to a subgroup just before deliver a message so
     * that the user code know the current version being handled. */
    subgroup_post_next_version_func_t post_next_version_callback;

    /** persistence manager callbacks */
    persistence_manager_callbacks_t persistence_manager_callbacks;

    /** Continuously waits for a new pending send, then sends it. This function
     * implements the sender thread. */
    void send_loop();

    uint64_t get_time();

    /** Checks for failures when a sender reaches its timeout. This function
     * implements the timeout thread. */
    void check_failures_loop();

    bool create_rdmc_sst_groups();
    void initialize_sst_row();
    void register_predicates();

    /**
     * Delivers a single message to the application layer, either by invoking
     * an RPC function or by calling a global stability callback.
     * @param msg A reference to the message
     * @param subgroup_num The ID of the subgroup this message is in
     * @param version The version assigned to the message
     */
    void deliver_message(RDMCMessage& msg, subgroup_id_t subgroup_num, persistent::version_t version);
    /**
     * Same as the other deliver_message, but for the SSTMessage type
     * @param msg A reference to the message to deliver
     * @param subgroup_num The ID of the subgroup this message is in
     * @param version The version assigned to the message
     */
    void deliver_message(SSTMessage& msg, subgroup_id_t subgroup_num, persistent::version_t version);

    /**
     * Enqueues a single message for persistence with the persistence manager.
     * Note that this does not actually wait for the message to be persisted;
     * you must still post a persistence request with the persistence manager.
     * @param msg The message that should cause a new version to be registered
     * with PersistenceManager
     * @param subgroup_num The ID of the subgroup this message is in
     * @param version The version assigned to the message
     * @return true if a new version was created
     * false if the message is a null message
     */
    bool version_message(RDMCMessage& msg, subgroup_id_t subgroup_num, persistent::version_t version, uint64_t msg_timestamp);
    /**
     * Same as the other version_message, but for the SSTMessage type.
     * @param msg The message that should cause a new version to be registered
     * with PersistenceManager
     * @param subgroup_num The ID of the subgroup this message is in
     * @param version The version assigned to the message
     * @return true if a new version was created
     * false if the message is a null message
     */
    bool version_message(SSTMessage& msg, subgroup_id_t subgroup_num, persistent::version_t version, uint64_t msg_timestamp);

    uint32_t get_num_senders(const std::vector<int>& shard_senders) {
        uint32_t num = 0;
        for(const auto i : shard_senders) {
            if(i) {
                num++;
            }
        }
        return num;
    };

    int32_t resolve_num_received(int32_t index, uint32_t num_received_entry);

    /* Predicate functions for receiving and delivering messages, parameterized by subgroup.
     * register_predicates will create and bind one of these for each subgroup. */

    void delivery_trigger(subgroup_id_t subgroup_num, const SubgroupSettings& curr_subgroup_settings,
                          const uint32_t num_shard_members, DerechoSST& sst);

    void sst_receive_handler(subgroup_id_t subgroup_num, const SubgroupSettings& curr_subgroup_settings,
                             const std::map<uint32_t, uint32_t>& shard_ranks_by_sender_rank,
                             uint32_t num_shard_senders, uint32_t sender_rank,
                             volatile char* data, uint64_t size);

    bool receiver_predicate(subgroup_id_t subgroup_num, const SubgroupSettings& curr_subgroup_settings,
                            const std::map<uint32_t, uint32_t>& shard_ranks_by_sender_rank,
                            uint32_t num_shard_senders, const DerechoSST& sst);

    void receiver_function(subgroup_id_t subgroup_num, const SubgroupSettings& curr_subgroup_settings,
                           const std::map<uint32_t, uint32_t>& shard_ranks_by_sender_rank,
                           uint32_t num_shard_senders, DerechoSST& sst, unsigned int batch_size,
                           const std::function<void(uint32_t, volatile char*, uint32_t)>& sst_receive_handler_lambda);

    // Internally used to automatically send a NULL message
    void get_buffer_and_send_auto_null(subgroup_id_t subgroup_num);
    /* Get a pointer into the current buffer, to write data into it before sending
     * Now this is a private function, called by send internally */
    char* get_sendbuffer_ptr(subgroup_id_t subgroup_num, long long unsigned int payload_size, bool cooked_send);

public:
    /**
     * Standard constructor for setting up a MulticastGroup for the first time.
     * @param _members A list of node IDs of members in this group
     * @param my_node_id The rank (ID) of this node in the group
     * @param _sst The SST this group will use; created by the GMS (membership
     * service) for this group.
     * @param _callbacks A set of functions to call when messages have reached
     * various levels of stability
     * @param total_num_subgroups The total number of subgroups in this Derecho
     * Group
     * @param subgroup_settings_by_id A list of SubgroupSettings, one for each
     * subgroup this node belongs to, indexed by subgroup ID
     * @param derecho_params The parameters for multicasts in this group
     * @param post_next_version_callback The callback for posting the upcoming
     *        version to be delivered in a subgroup.
     * @param persistence_manager_callbacks The callbacks to PersistenceManager
     * that will be used to persist received messages
     * @param already_failed (Optional) A Boolean vector indicating which
     * elements of _members are nodes that have already failed in this view
     */
    MulticastGroup(
            std::vector<node_id_t> members, node_id_t my_node_id,
            std::shared_ptr<DerechoSST> sst,
            CallbackSet callbacks,
            uint32_t total_num_subgroups,
            const std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings_by_id,
            const DerechoParams derecho_params,
	    const subgroup_post_next_version_func_t& post_next_version_callback,
            const persistence_manager_callbacks_t& persistence_manager_callbacks,
            std::vector<char> already_failed = {});
    /** Constructor to initialize a new MulticastGroup from an old one,
     * preserving the same settings but providing a new list of members. */
    MulticastGroup(
            std::vector<node_id_t> members, node_id_t my_node_id,
            std::shared_ptr<DerechoSST> sst,
            MulticastGroup&& old_group,
            uint32_t total_num_subgroups,
            const std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings_by_id,
	    const subgroup_post_next_version_func_t& post_next_version_callback,
            const persistence_manager_callbacks_t& persistence_manager_callbacks,
            std::vector<char> already_failed = {});

    ~MulticastGroup();

    /**
     * Registers a function to be called upon receipt of a multicast RPC message
     * @param handler A function that will handle RPC messages.
     */
    void register_rpc_callback(rpc_handler_t handler) { rpc_callback = std::move(handler); }

    void deliver_messages_upto(const std::vector<int32_t>& max_indices_for_senders, subgroup_id_t subgroup_num, uint32_t num_shard_senders);
    /** Send now internally calls get_sendbuffer_ptr.
	The user function that generates the message is supplied to send */
    bool send(subgroup_id_t subgroup_num, long long unsigned int payload_size,
              const std::function<void(char* buf)>& msg_generator, bool cooked_send);
    bool check_pending_sst_sends(subgroup_id_t subgroup_num);

    const uint64_t compute_global_stability_frontier(subgroup_id_t subgroup_num);

    /** Stops all sending and receiving in this group, in preparation for shutting it down. */
    void wedge();
    /** Debugging function; prints the current state of the SST to stdout. */
    void debug_print();
    static long long unsigned int compute_max_msg_size(
            const long long unsigned int max_payload_size,
            const long long unsigned int block_size,
            bool using_rdmc);

    /**
     * @return a map from subgroup ID to SubgroupSettings for only those subgroups
     * that this node belongs to.
     */
    const std::map<subgroup_id_t, SubgroupSettings>& get_subgroup_settings() {
        return subgroup_settings;
    }
    std::vector<uint32_t> get_shard_sst_indices(subgroup_id_t subgroup_num);
};
}  // namespace derecho
