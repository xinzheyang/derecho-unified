#include <algorithm>
#include <cassert>
#include <chrono>
#include <limits>
#include <thread>

#include "derecho_internal.h"
#include "multicast_group.h"
#include "persistent/Persistent.hpp"
#include "rdmc/util.h"
#include "utils/logger.hpp"

namespace derecho {

/**
 * Helper function to find the index of an element in a container.
 */
template <class T, class U>
size_t index_of(T container, U elem) {
    size_t n = 0;
    for(auto it = begin(container); it != end(container); ++it) {
        if(*it == elem) return n;
        n++;
    }
    return container.size();
}

MulticastGroup::MulticastGroup(
        std::vector<node_id_t> _members, node_id_t my_node_id,
        std::shared_ptr<DerechoSST> sst,
        CallbackSet callbacks,
        uint32_t total_num_subgroups,
        const std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings_by_id,
        const DerechoParams derecho_params,
	const subgroup_post_next_version_func_t& post_next_version_callback,
        const persistence_manager_callbacks_t& persistence_manager_callbacks,
        std::vector<char> already_failed)
        : whenlog(logger(LoggerFactory::getDefaultLogger()), )
                  members(_members),
          num_members(members.size()),
          member_index(index_of(members, my_node_id)),
          block_size(derecho_params.block_size),
          max_msg_size(compute_max_msg_size(derecho_params.max_payload_size,
                                            derecho_params.block_size,
                                            derecho_params.max_payload_size > derecho_params.max_smc_payload_size)),
          sst_max_msg_size(derecho_params.max_smc_payload_size + sizeof(header)),
          rdmc_send_algorithm(derecho_params.rdmc_send_algorithm),
          window_size(derecho_params.window_size),
          callbacks(callbacks),
          total_num_subgroups(total_num_subgroups),
          subgroup_settings(subgroup_settings_by_id),
          received_intervals(sst->num_received.size(), {-1, -1}),
          rdmc_group_num_offset(0),
          future_message_indices(total_num_subgroups, 0),
          next_sends(total_num_subgroups),
          pending_sends(total_num_subgroups),
          current_sends(total_num_subgroups),
          next_message_to_deliver(total_num_subgroups),
          sender_timeout(derecho_params.timeout_ms),
          sst(sst),
          sst_multicast_group_ptrs(total_num_subgroups),
          last_transfer_medium(total_num_subgroups),
          post_next_version_callback(post_next_version_callback),
          persistence_manager_callbacks(persistence_manager_callbacks) {
    assert(window_size >= 1);

    for(uint i = 0; i < num_members; ++i) {
        node_id_to_sst_index[members[i]] = i;
    }

    for(const auto p : subgroup_settings_by_id) {
        auto num_shard_members = p.second.members.size();
        while(free_message_buffers[p.first].size() < window_size * num_shard_members) {
            free_message_buffers[p.first].emplace_back(max_msg_size);
        }
    }

    initialize_sst_row();
    bool no_member_failed = true;
    if(already_failed.size()) {
        for(uint i = 0; i < num_members; ++i) {
            if(already_failed[i]) {
                no_member_failed = false;
                break;
            }
        }
    }
    if(!already_failed.size() || no_member_failed) {
        // if groups are created successfully, rdmc_sst_groups_created will be set to true
        rdmc_sst_groups_created = create_rdmc_sst_groups();
    }
    register_predicates();
    sender_thread = std::thread(&MulticastGroup::send_loop, this);
    timeout_thread = std::thread(&MulticastGroup::check_failures_loop, this);
}

MulticastGroup::MulticastGroup(
        std::vector<node_id_t> _members, node_id_t my_node_id,
        std::shared_ptr<DerechoSST> sst,
        MulticastGroup&& old_group,
        uint32_t total_num_subgroups,
        const std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings_by_id,
	const subgroup_post_next_version_func_t& post_next_version_callback,
        const persistence_manager_callbacks_t& persistence_manager_callbacks,
        std::vector<char> already_failed)
        : whenlog(logger(old_group.logger), )
                  members(_members),
          num_members(members.size()),
          member_index(index_of(members, my_node_id)),
          block_size(old_group.block_size),
          max_msg_size(old_group.max_msg_size),
          sst_max_msg_size(old_group.sst_max_msg_size),
          rdmc_send_algorithm(old_group.rdmc_send_algorithm),
          window_size(old_group.window_size),
          callbacks(old_group.callbacks),
          total_num_subgroups(total_num_subgroups),
          subgroup_settings(subgroup_settings_by_id),
          received_intervals(sst->num_received.size(), {-1, -1}),
          rpc_callback(old_group.rpc_callback),
          rdmc_group_num_offset(old_group.rdmc_group_num_offset + old_group.num_members),
          future_message_indices(total_num_subgroups, 0),
          next_sends(total_num_subgroups),
          pending_sends(total_num_subgroups),
          current_sends(total_num_subgroups),
          next_message_to_deliver(total_num_subgroups),
          sender_timeout(old_group.sender_timeout),
          sst(sst),
          sst_multicast_group_ptrs(total_num_subgroups),
          last_transfer_medium(total_num_subgroups),
          post_next_version_callback(post_next_version_callback),
          persistence_manager_callbacks(persistence_manager_callbacks) {
    // Make sure rdmc_group_num_offset didn't overflow.
    assert(old_group.rdmc_group_num_offset <= std::numeric_limits<uint16_t>::max() - old_group.num_members - num_members);

    // Just in case
    old_group.wedge();

    for(uint i = 0; i < num_members; ++i) {
        node_id_to_sst_index[members[i]] = i;
    }

    // Convience function that takes a msg from the old group and
    // produces one suitable for this group.
    auto convert_msg = [this](RDMCMessage& msg, subgroup_id_t subgroup_num) {
        msg.sender_id = members[member_index];
        msg.index = future_message_indices[subgroup_num]++;
        return std::move(msg);
    };

    // Convience function that takes a msg from the old group and
    // produces one suitable for this group.
    auto convert_sst_msg = [this](SSTMessage& msg, subgroup_id_t subgroup_num) {
        msg.sender_id = members[member_index];
        msg.index = future_message_indices[subgroup_num]++;
        return std::move(msg);
    };

    for(const auto p : subgroup_settings_by_id) {
        auto num_shard_members = p.second.members.size();
        while(free_message_buffers[p.first].size() < window_size * num_shard_members) {
            free_message_buffers[p.first].emplace_back(max_msg_size);
        }
    }

    // Reclaim RDMCMessageBuffers from the old group, and supplement them with
    // additional if the group has grown.
    std::lock_guard<std::mutex> lock(old_group.msg_state_mtx);
    for(const auto p : subgroup_settings_by_id) {
        const auto subgroup_num = p.first;
        auto num_shard_members = p.second.members.size();
        // for later: don't move extra message buffers
        free_message_buffers[subgroup_num].swap(old_group.free_message_buffers[subgroup_num]);
        while(free_message_buffers[subgroup_num].size() < old_group.window_size * num_shard_members) {
            free_message_buffers[subgroup_num].emplace_back(max_msg_size);
        }
    }

    for(auto& msg : old_group.current_receives) {
        free_message_buffers[msg.first.first].push_back(std::move(msg.second.message_buffer));
    }
    old_group.current_receives.clear();

    // Assume that any locally stable messages failed. If we were the sender
    // than re-attempt, otherwise discard. TODO: Presumably the ragged edge
    // cleanup will want the chance to deliver some of these.
    for(auto& p : old_group.locally_stable_rdmc_messages) {
        if(p.second.size() == 0) {
            continue;
        }

        for(auto& q : p.second) {
            if(q.second.sender_id == members[member_index]) {
                pending_sends[p.first].push(convert_msg(q.second, p.first));
            } else {
                free_message_buffers[p.first].push_back(std::move(q.second.message_buffer));
            }
        }
    }
    old_group.locally_stable_rdmc_messages.clear();

    for(auto& p : old_group.locally_stable_sst_messages) {
        if(p.second.size() == 0) {
            continue;
        }
    }
    old_group.locally_stable_sst_messages.clear();

    // Any messages that were being sent should be re-attempted.
    for(const auto& p : subgroup_settings_by_id) {
        auto subgroup_num = p.first;
        if(old_group.current_sends.size() > subgroup_num && old_group.current_sends[subgroup_num]) {
            pending_sends[subgroup_num].push(convert_msg(*old_group.current_sends[subgroup_num], subgroup_num));
        }

        if(old_group.pending_sends.size() > subgroup_num) {
            while(!old_group.pending_sends[subgroup_num].empty()) {
                pending_sends[subgroup_num].push(convert_msg(old_group.pending_sends[subgroup_num].front(), subgroup_num));
                old_group.pending_sends[subgroup_num].pop();
            }
        }

        if(old_group.next_sends.size() > subgroup_num && old_group.next_sends[subgroup_num]) {
            next_sends[subgroup_num] = convert_msg(*old_group.next_sends[subgroup_num], subgroup_num);
        }

        for(auto& entry : old_group.non_persistent_messages[subgroup_num]) {
            non_persistent_messages[subgroup_num].emplace(entry.first,
                                                          convert_msg(entry.second, subgroup_num));
        }
        old_group.non_persistent_messages.clear();
        for(auto& entry : old_group.non_persistent_sst_messages[subgroup_num]) {
            non_persistent_sst_messages[subgroup_num].emplace(entry.first,
                                                              convert_sst_msg(entry.second, subgroup_num));
        }
        old_group.non_persistent_sst_messages.clear();
    }

    initialize_sst_row();
    bool no_member_failed = true;
    if(already_failed.size()) {
        for(uint i = 0; i < num_members; ++i) {
            if(already_failed[i]) {
                no_member_failed = false;
                break;
            }
        }
    }
    if(!already_failed.size() || no_member_failed) {
        // if groups are created successfully, rdmc_sst_groups_created will be set to true
        rdmc_sst_groups_created = create_rdmc_sst_groups();
    }
    register_predicates();
    sender_thread = std::thread(&MulticastGroup::send_loop, this);
    timeout_thread = std::thread(&MulticastGroup::check_failures_loop, this);
}

bool MulticastGroup::create_rdmc_sst_groups() {
    for(const auto& p : subgroup_settings) {
        uint32_t subgroup_num = p.first;
        const SubgroupSettings& curr_subgroup_settings = p.second;
        const std::vector<node_id_t>& shard_members = curr_subgroup_settings.members;
        std::size_t num_shard_members = shard_members.size();
        std::vector<int> shard_senders = curr_subgroup_settings.senders;
        uint32_t num_shard_senders = get_num_senders(shard_senders);
        auto shard_sst_indices = get_shard_sst_indices(subgroup_num);
        sst_multicast_group_ptrs[subgroup_num] = std::make_unique<sst::multicast_group<DerechoSST>>(
                sst, shard_sst_indices, window_size, sst_max_msg_size, curr_subgroup_settings.senders,
                curr_subgroup_settings.num_received_offset, window_size * subgroup_num);
        for(uint shard_rank = 0, sender_rank = -1; shard_rank < num_shard_members; ++shard_rank) {
            // don't create RDMC group if the shard member is never going to send
            if(!shard_senders[shard_rank]) {
                continue;
            }
            sender_rank++;
            node_id_t node_id = shard_members[shard_rank];
            // When RDMC receives a message, it should store it in
            // locally_stable_rdmc_messages and update the received count
            rdmc::completion_callback_t rdmc_receive_handler;
            rdmc_receive_handler = [this, subgroup_num, shard_rank, sender_rank,
                                    curr_subgroup_settings, node_id,
                                    num_shard_members, num_shard_senders,
                                    shard_sst_indices](char* data, size_t size) {
                assert(this->sst);
                std::lock_guard<std::mutex> lock(msg_state_mtx);
                header* h = (header*)data;
                const int32_t index = h->index;
                message_id_t sequence_number = index * num_shard_senders + sender_rank;

                whenlog(logger->trace("Locally received message in subgroup {}, sender rank {}, index {}", subgroup_num, shard_rank, index););
                // Move message from current_receives to locally_stable_rdmc_messages.
                if(node_id == members[member_index]) {
                    assert(current_sends[subgroup_num]);
                    locally_stable_rdmc_messages[subgroup_num][sequence_number] = std::move(*current_sends[subgroup_num]);
                    current_sends[subgroup_num] = std::nullopt;
                } else {
                    auto it = current_receives.find({subgroup_num, node_id});
                    assert(it != current_receives.end());
                    auto& message = it->second;
                    message.index = index;
                    locally_stable_rdmc_messages[subgroup_num].emplace(sequence_number, std::move(message));
                    current_receives.erase(it);
                }

                auto new_num_received = resolve_num_received(index, curr_subgroup_settings.num_received_offset + sender_rank);
                /* NULL Send Scheme */
                // only if I am a sender in the subgroup and the subgroup is not in UNORDERED mode
                if(curr_subgroup_settings.sender_rank >= 0 && curr_subgroup_settings.mode != Mode::UNORDERED) {
                    if(curr_subgroup_settings.sender_rank < (int)sender_rank) {
                        while(future_message_indices[subgroup_num] <= new_num_received) {
                            get_buffer_and_send_auto_null(subgroup_num);
                        }
                    } else if(curr_subgroup_settings.sender_rank > (int)sender_rank) {
                        while(future_message_indices[subgroup_num] < new_num_received) {
                            get_buffer_and_send_auto_null(subgroup_num);
                        }
                    }
                }

                // deliver immediately if in UNORDERED mode
                if(curr_subgroup_settings.mode == Mode::UNORDERED) {
                    // issue stability upcalls for the recently sequenced messages
                    for(int i = sst->num_received[member_index][curr_subgroup_settings.num_received_offset + sender_rank] + 1;
                        i <= new_num_received; ++i) {
                        message_id_t seq_num = i * num_shard_senders + sender_rank;
                        if(!locally_stable_sst_messages[subgroup_num].empty()
                           && locally_stable_sst_messages[subgroup_num].begin()->first == seq_num) {
                            auto& msg = locally_stable_sst_messages[subgroup_num].begin()->second;
                            char* buf = const_cast<char*>(msg.buf);
                            header* h = (header*)(buf);
                            // no delivery callback for a NULL message
                            if(msg.size > h->header_size && callbacks.global_stability_callback) {
                                callbacks.global_stability_callback(subgroup_num, msg.sender_id,
                                                                    msg.index,
                                                                    {{buf + h->header_size, msg.size - h->header_size}},
                                                                    INVALID_VERSION);
                            }
                            if(node_id == members[member_index]) {
                                pending_message_timestamps[subgroup_num].erase(h->timestamp);
                            }
                            locally_stable_sst_messages[subgroup_num].erase(locally_stable_sst_messages[subgroup_num].begin());
                        } else {
                            assert(!locally_stable_rdmc_messages[subgroup_num].empty());
                            auto it2 = locally_stable_rdmc_messages[subgroup_num].begin();
                            assert(it2->first == seq_num);
                            auto& msg = it2->second;
                            char* buf = msg.message_buffer.buffer.get();
                            header* h = (header*)(buf);
                            // no delivery for a NULL message
                            if(msg.size > h->header_size && callbacks.global_stability_callback) {
                                callbacks.global_stability_callback(subgroup_num, msg.sender_id,
                                                                    msg.index,
                                                                    {{buf + h->header_size, msg.size - h->header_size}},
                                                                    INVALID_VERSION);
                            }
                            free_message_buffers[subgroup_num].push_back(std::move(msg.message_buffer));
                            if(node_id == members[member_index]) {
                                pending_message_timestamps[subgroup_num].erase(h->timestamp);
                            }
                            locally_stable_rdmc_messages[subgroup_num].erase(it2);
                        }
                    }
                }
                if(new_num_received > sst->num_received[member_index][curr_subgroup_settings.num_received_offset + sender_rank]) {
                    sst->num_received[member_index][curr_subgroup_settings.num_received_offset + sender_rank] = new_num_received;
                    // std::atomic_signal_fence(std::memory_order_acq_rel);
                    auto* min_ptr = std::min_element(&sst->num_received[member_index][curr_subgroup_settings.num_received_offset],
                                                     &sst->num_received[member_index][curr_subgroup_settings.num_received_offset + num_shard_senders]);
                    uint min_index = std::distance(&sst->num_received[member_index][curr_subgroup_settings.num_received_offset], min_ptr);
                    auto new_seq_num = (*min_ptr + 1) * num_shard_senders + min_index - 1;
                    if(static_cast<message_id_t>(new_seq_num) > sst->seq_num[member_index][subgroup_num]) {
                        whenlog(logger->trace("Updating seq_num for subgroup {} to {}", subgroup_num, new_seq_num););
                        sst->seq_num[member_index][subgroup_num] = new_seq_num;
                        sst->put(shard_sst_indices,
                                 (char*)std::addressof(sst->seq_num[0][subgroup_num]) - sst->getBaseAddress(),
                                 sizeof(decltype(sst->seq_num)::value_type));
                    }
                    sst->put(shard_sst_indices,
                             (char*)std::addressof(sst->num_received[0][curr_subgroup_settings.num_received_offset + sender_rank])
                                     - sst->getBaseAddress(),
                             sizeof(decltype(sst->num_received)::value_type));
                }
            };
            // Capture rdmc_receive_handler by copy! The reference to it won't be valid after this constructor ends!
            auto receive_handler_plus_notify =
                    [this, rdmc_receive_handler](char* data, size_t size) {
                        rdmc_receive_handler(data, size);
                        // signal background writer thread
                        sender_cv.notify_all();
                    };

            // Create a "rotated" vector of members in which the currently selected shard member (shard_rank) is first
            std::vector<uint32_t> rotated_shard_members(shard_members.size());
            for(uint k = 0; k < num_shard_members; ++k) {
                rotated_shard_members[k] = shard_members[(shard_rank + k) % num_shard_members];
            }

            // don't create rdmc group if there's only one member in the shard
            if(num_shard_members <= 1) {
                continue;
            }

            if(node_id == members[member_index]) {
                //Create a group in which this node is the sender, and only self-receives happen
                if(!rdmc::create_group(
                           rdmc_group_num_offset, rotated_shard_members, block_size, rdmc_send_algorithm,
                           [this](size_t length) -> rdmc::receive_destination {
                               assert_always(false);
                               return {nullptr, 0};
                           },
                           receive_handler_plus_notify,
                           [](std::optional<uint32_t>) {})) {
                    return false;
                }
                subgroup_to_rdmc_group[subgroup_num] = rdmc_group_num_offset;
                rdmc_group_num_offset++;
            } else {
                if(!rdmc::create_group(
                           rdmc_group_num_offset, rotated_shard_members, block_size, rdmc_send_algorithm,
                           [this, subgroup_num, node_id, sender_rank, num_shard_senders](size_t length) {
                               std::lock_guard<std::mutex> lock(msg_state_mtx);
                               assert(!free_message_buffers[subgroup_num].empty());
                               //Create a Message struct to receive the data into.
                               RDMCMessage msg;
                               msg.sender_id = node_id;
                               msg.size = length;
                               msg.message_buffer = std::move(free_message_buffers[subgroup_num].back());
                               free_message_buffers[subgroup_num].pop_back();

                               rdmc::receive_destination ret{msg.message_buffer.mr, 0};
                               current_receives[{subgroup_num, node_id}] = std::move(msg);

                               assert(ret.mr->buffer != nullptr);
                               return ret;
                           },
                           rdmc_receive_handler, [](std::optional<uint32_t>) {})) {
                    return false;
                }
                rdmc_group_num_offset++;
            }
        }
    }
    return true;
}

void MulticastGroup::initialize_sst_row() {
    auto num_received_size = sst->num_received.size();
    auto seq_num_size = sst->seq_num.size();
    for(uint i = 0; i < num_members; ++i) {
        for(uint j = 0; j < num_received_size; ++j) {
            sst->num_received[i][j] = -1;
        }
        for(uint j = 0; j < seq_num_size; ++j) {
            sst->seq_num[i][j] = -1;
            sst->delivered_num[i][j] = -1;
            sst->persisted_num[i][j] = -1;
        }
    }
    sst->put();
    sst->sync_with_members();
}

void MulticastGroup::deliver_message(RDMCMessage& msg, subgroup_id_t subgroup_num, persistent::version_t version) {
    char* buf = msg.message_buffer.buffer.get();
    header* h = (header*)(buf);
    // cooked send
    if(h->cooked_send) {
        buf += h->header_size;
        auto payload_size = msg.size - h->header_size;
        post_next_version_callback(subgroup_num, version);
        rpc_callback(subgroup_num, msg.sender_id, buf, payload_size);
        if(callbacks.global_stability_callback) {
            callbacks.global_stability_callback(subgroup_num, msg.sender_id, msg.index, {},
                                                version);
        }
    } else if(msg.size > h->header_size && callbacks.global_stability_callback) {
        callbacks.global_stability_callback(subgroup_num, msg.sender_id, msg.index,
                                            {{buf + h->header_size, msg.size - h->header_size}},
                                            version);
    }
}

void MulticastGroup::deliver_message(SSTMessage& msg, subgroup_id_t subgroup_num, persistent::version_t version) {
    char* buf = const_cast<char*>(msg.buf);
    header* h = (header*)(buf);
    // cooked send
    if(h->cooked_send) {
        buf += h->header_size;
        auto payload_size = msg.size - h->header_size;
        post_next_version_callback(subgroup_num, version);
        rpc_callback(subgroup_num, msg.sender_id, buf, payload_size);
        if(callbacks.global_stability_callback) {
            callbacks.global_stability_callback(subgroup_num, msg.sender_id, msg.index, {},
                                                version);
        }
    } else if(msg.size > h->header_size && callbacks.global_stability_callback) {
        callbacks.global_stability_callback(subgroup_num, msg.sender_id, msg.index,
                                            {{buf + h->header_size, msg.size - h->header_size}},
                                            version);
    }
}

bool MulticastGroup::version_message(RDMCMessage& msg, subgroup_id_t subgroup_num, persistent::version_t version, uint64_t msg_timestamp) {
    char* buf = msg.message_buffer.buffer.get();
    header* h = (header*)(buf);
    // null message filter
    if(msg.size == h->header_size) {
        return false;
    }
    if(msg.sender_id == members[member_index]) {
        pending_persistence[subgroup_num][locally_stable_rdmc_messages[subgroup_num].begin()->first] = msg_timestamp;
    }
    // make a version for persistent<t>/volatile<t>
    uint64_t msg_ts_us = msg_timestamp / 1e3;
    if(msg_ts_us == 0) {
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        msg_ts_us = (uint64_t)now.tv_sec * 1e6 + now.tv_nsec / 1e3;
    }
    std::get<0>(persistence_manager_callbacks)(subgroup_num, version, HLC{msg_ts_us, 0});
    return true;
}

bool MulticastGroup::version_message(SSTMessage& msg, subgroup_id_t subgroup_num, persistent::version_t version, uint64_t msg_timestamp) {
    char* buf = const_cast<char*>(msg.buf);
    header* h = (header*)(buf);
    // null message filter
    if(msg.size == h->header_size) {
        return false;
    }
    if(msg.sender_id == members[member_index]) {
        pending_persistence[subgroup_num][locally_stable_sst_messages[subgroup_num].begin()->first] = msg_timestamp;
    }
    // make a version for persistent<t>/volatile<t>
    uint64_t msg_ts_us = msg_timestamp / 1e3;
    if(msg_ts_us == 0) {
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        msg_ts_us = (uint64_t)now.tv_sec * 1e6 + now.tv_nsec / 1e3;
    }
    std::get<0>(persistence_manager_callbacks)(subgroup_num, version, HLC{msg_ts_us, 0});
    return true;
}

void MulticastGroup::deliver_messages_upto(
        const std::vector<int32_t>& max_indices_for_senders,
        subgroup_id_t subgroup_num, uint32_t num_shard_senders) {
    bool non_null_msgs_delivered = false;
    assert(max_indices_for_senders.size() == (size_t)num_shard_senders);
    std::lock_guard<std::mutex> lock(msg_state_mtx);
    int32_t curr_seq_num = sst->delivered_num[member_index][subgroup_num];
    int32_t max_seq_num = curr_seq_num;
    for(uint sender = 0; sender < num_shard_senders; sender++) {
        max_seq_num = std::max(max_seq_num,
                               static_cast<int32_t>(max_indices_for_senders[sender] * num_shard_senders + sender));
    }
    persistent::version_t assigned_version = INVALID_VERSION;
    for(int32_t seq_num = curr_seq_num + 1; seq_num <= max_seq_num; seq_num++) {
        //determine if this sequence number should actually be skipped
        int32_t index = seq_num / num_shard_senders;
        uint32_t sender_rank = seq_num % num_shard_senders;
        if(index > max_indices_for_senders[sender_rank]) {
            continue;
        }
        auto rdmc_msg_ptr = locally_stable_rdmc_messages[subgroup_num].find(seq_num);
        assigned_version = persistent::combine_int32s(sst->vid[member_index], seq_num);
        if(rdmc_msg_ptr != locally_stable_rdmc_messages[subgroup_num].end()) {
            auto& msg = rdmc_msg_ptr->second;
            char* buf = msg.message_buffer.buffer.get();
            uint64_t msg_ts = ((header*)buf)->timestamp;
            //Note: deliver_message frees the RDMC buffer in msg, which is why the timestamp must be saved before calling this
            deliver_message(msg, subgroup_num, assigned_version);
            non_null_msgs_delivered |= version_message(msg, subgroup_num, assigned_version, msg_ts);
            // free the message buffer only after it version_message has been called
            free_message_buffers[subgroup_num].push_back(std::move(msg.message_buffer));
            locally_stable_rdmc_messages[subgroup_num].erase(rdmc_msg_ptr);
        } else {
            auto& msg = locally_stable_sst_messages[subgroup_num].at(seq_num);
            char* buf = (char*)msg.buf;
            uint64_t msg_ts = ((header*)buf)->timestamp;
            deliver_message(msg, subgroup_num, assigned_version);
            non_null_msgs_delivered |= version_message(msg, subgroup_num, assigned_version, msg_ts);
            locally_stable_sst_messages[subgroup_num].erase(seq_num);
        }
    }
    gmssst::set(sst->delivered_num[member_index][subgroup_num], max_seq_num);
    sst->put(get_shard_sst_indices(subgroup_num),
             (char*)std::addressof(sst->delivered_num[0][subgroup_num]) - sst->getBaseAddress(),
             sizeof(decltype(sst->delivered_num)::value_type));
    if(non_null_msgs_delivered) {
        //Call the persistence_manager_post_persist_func
        std::get<1>(persistence_manager_callbacks)(subgroup_num, assigned_version);
    }
}

int32_t MulticastGroup::resolve_num_received(int32_t index, uint32_t num_received_entry) {
    auto it = received_intervals[num_received_entry].end();
    it--;
    while(*it > index) {
        it--;
    }
    if(std::next(it) == received_intervals[num_received_entry].end()) {
        if(*it == index - 1) {
            *it = index;
        } else {
            received_intervals[num_received_entry].push_back(index);
            received_intervals[num_received_entry].push_back(index);
        }
    } else {
        auto next_it = std::next(it);
        if(*it != index - 1) {
            received_intervals[num_received_entry].insert(next_it, index);
            if(*next_it != index + 1) {
                received_intervals[num_received_entry].insert(next_it, index);
            } else {
                received_intervals[num_received_entry].erase(next_it);
            }
        } else {
            if(*next_it != index + 1) {
                received_intervals[num_received_entry].insert(next_it, index);
            } else {
                received_intervals[num_received_entry].erase(next_it);
            }
            received_intervals[num_received_entry].erase(it);
        }
    }
    return *std::next(received_intervals[num_received_entry].begin());
}

bool MulticastGroup::receiver_predicate(subgroup_id_t subgroup_num, const SubgroupSettings& curr_subgroup_settings,
                                        const std::map<uint32_t, uint32_t>& shard_ranks_by_sender_rank,
                                        uint32_t num_shard_senders, const DerechoSST& sst) {
    for(uint sender_count = 0; sender_count < num_shard_senders; ++sender_count) {
        int32_t num_received = sst.num_received_sst[member_index][curr_subgroup_settings.num_received_offset + sender_count] + 1;
        uint32_t slot = num_received % window_size;
        if(static_cast<long long int>((uint64_t&)sst.slots[node_id_to_sst_index.at(curr_subgroup_settings.members[shard_ranks_by_sender_rank.at(sender_count)])]
                                                          [(sst_max_msg_size + 2 * sizeof(uint64_t)) * (subgroup_num * window_size + slot + 1) - sizeof(uint64_t)])
           == num_received / window_size + 1) {
            return true;
        }
    }
    return false;
}

void MulticastGroup::sst_receive_handler(subgroup_id_t subgroup_num, const SubgroupSettings& curr_subgroup_settings,
                                         const std::map<uint32_t, uint32_t>& shard_ranks_by_sender_rank,
                                         uint32_t num_shard_senders, uint32_t sender_rank,
                                         volatile char* data, uint64_t size) {
    header* h = (header*)data;
    const int32_t index = h->index;

    message_id_t sequence_number = index * num_shard_senders + sender_rank;
    node_id_t node_id = curr_subgroup_settings.members[shard_ranks_by_sender_rank.at(sender_rank)];

    locally_stable_sst_messages[subgroup_num][sequence_number] = {node_id, index, size, data};

    auto new_num_received = resolve_num_received(index, curr_subgroup_settings.num_received_offset + sender_rank);
    /* NULL Send Scheme */
    // only if I am a sender in the subgroup and the subgroup is not in UNORDERED mode
    if(curr_subgroup_settings.sender_rank >= 0 && curr_subgroup_settings.mode != Mode::UNORDERED) {
        if(curr_subgroup_settings.sender_rank < (int)sender_rank) {
            while(future_message_indices[subgroup_num] <= new_num_received) {
                get_buffer_and_send_auto_null(subgroup_num);
            }
        } else if(curr_subgroup_settings.sender_rank > (int)sender_rank) {
            while(future_message_indices[subgroup_num] < new_num_received) {
                get_buffer_and_send_auto_null(subgroup_num);
            }
        }
    }

    if(curr_subgroup_settings.mode == Mode::UNORDERED) {
        // issue stability upcalls for the recently sequenced messages
        for(int i = sst->num_received[member_index][curr_subgroup_settings.num_received_offset + sender_rank] + 1; i <= new_num_received; ++i) {
            message_id_t seq_num = i * num_shard_senders + sender_rank;
            if(!locally_stable_sst_messages[subgroup_num].empty()
               && locally_stable_sst_messages[subgroup_num].begin()->first == seq_num) {
                auto& msg = locally_stable_sst_messages[subgroup_num].begin()->second;
                char* buf = const_cast<char*>(msg.buf);
                header* h = (header*)(buf);
                if(msg.size > h->header_size && callbacks.global_stability_callback) {
                    callbacks.global_stability_callback(subgroup_num, msg.sender_id,
                                                        msg.index,
                                                        {{buf + h->header_size, msg.size - h->header_size}},
                                                        INVALID_VERSION);
                }
                if(node_id == members[member_index]) {
                    pending_message_timestamps[subgroup_num].erase(h->timestamp);
                }
                locally_stable_sst_messages[subgroup_num].erase(locally_stable_sst_messages[subgroup_num].begin());
            } else {
                assert(!locally_stable_rdmc_messages[subgroup_num].empty());
                auto it2 = locally_stable_rdmc_messages[subgroup_num].begin();
                assert(it2->first == seq_num);
                auto& msg = it2->second;
                char* buf = msg.message_buffer.buffer.get();
                header* h = (header*)(buf);
                if(msg.size > h->header_size && callbacks.global_stability_callback) {
                    callbacks.global_stability_callback(subgroup_num, msg.sender_id,
                                                        msg.index,
                                                        {{buf + h->header_size, msg.size - h->header_size}},
                                                        INVALID_VERSION);
                }
                free_message_buffers[subgroup_num].push_back(std::move(msg.message_buffer));
                if(node_id == members[member_index]) {
                    pending_message_timestamps[subgroup_num].erase(h->timestamp);
                }
                locally_stable_rdmc_messages[subgroup_num].erase(it2);
            }
        }
    }
    sst->num_received[member_index][curr_subgroup_settings.num_received_offset + sender_rank] = new_num_received;
}

void MulticastGroup::receiver_function(subgroup_id_t subgroup_num, const SubgroupSettings& curr_subgroup_settings,
                                       const std::map<uint32_t, uint32_t>& shard_ranks_by_sender_rank,
                                       uint32_t num_shard_senders, DerechoSST& sst, unsigned int batch_size,
                                       const std::function<void(uint32_t, volatile char*, uint32_t)>& sst_receive_handler_lambda) {
    std::lock_guard<std::mutex> lock(msg_state_mtx);
    for(uint i = 0; i < batch_size; ++i) {
        for(uint sender_count = 0; sender_count < num_shard_senders; ++sender_count) {
            auto num_received = sst.num_received_sst[member_index][curr_subgroup_settings.num_received_offset + sender_count] + 1;
            uint32_t slot = num_received % window_size;
            message_id_t next_seq = (uint64_t&)sst.slots[node_id_to_sst_index.at(curr_subgroup_settings.members[shard_ranks_by_sender_rank.at(sender_count)])]
                                                        [(sst_max_msg_size + 2 * sizeof(uint64_t)) * (subgroup_num * window_size + slot + 1) - sizeof(uint64_t)];
            if(next_seq == num_received / static_cast<int32_t>(window_size) + 1) {
                whenlog(logger->trace("receiver_trig calling sst_receive_handler_lambda. next_seq = {}, num_received = {}, sender rank = {}. Reading from SST row {}, slot {}",
                                      next_seq, num_received, sender_count, node_id_to_sst_index.at(curr_subgroup_settings.members[shard_ranks_by_sender_rank.at(sender_count)]), (subgroup_num * window_size + slot)););
                sst_receive_handler_lambda(sender_count,
                                           &sst.slots[node_id_to_sst_index.at(curr_subgroup_settings.members[shard_ranks_by_sender_rank.at(sender_count)])]
                                                     [(sst_max_msg_size + 2 * sizeof(uint64_t)) * (subgroup_num * window_size + slot)],
                                           (uint64_t&)sst.slots[node_id_to_sst_index.at(curr_subgroup_settings.members[shard_ranks_by_sender_rank.at(sender_count)])]
                                                               [(sst_max_msg_size + 2 * sizeof(uint64_t)) * (subgroup_num * window_size + slot + 1) - 2 * sizeof(uint64_t)]);
                sst.num_received_sst[member_index][curr_subgroup_settings.num_received_offset + sender_count] = num_received;
            }
        }
    }
    sst.put((char*)std::addressof(sst.num_received_sst[0][curr_subgroup_settings.num_received_offset]) - sst.getBaseAddress(),
            sizeof(decltype(sst.num_received_sst)::value_type) * num_shard_senders);
    // std::atomic_signal_fence(std::memory_order_acq_rel);
    auto* min_ptr = std::min_element(&sst.num_received[member_index][curr_subgroup_settings.num_received_offset],
                                     &sst.num_received[member_index][curr_subgroup_settings.num_received_offset + num_shard_senders]);
    int min_index = std::distance(&sst.num_received[member_index][curr_subgroup_settings.num_received_offset], min_ptr);
    message_id_t new_seq_num = (*min_ptr + 1) * num_shard_senders + min_index - 1;
    if(new_seq_num > sst.seq_num[member_index][subgroup_num]) {
        whenlog(logger->trace("Updating seq_num for subgroup {} to {}", subgroup_num, new_seq_num););
        sst.seq_num[member_index][subgroup_num] = new_seq_num;
        sst.put((char*)std::addressof(sst.seq_num[0][subgroup_num]) - sst.getBaseAddress(),
                sizeof(decltype(sst.seq_num)::value_type));
    }
    sst.put((char*)std::addressof(sst.num_received[0][curr_subgroup_settings.num_received_offset]) - sst.getBaseAddress(),
            sizeof(decltype(sst.num_received)::value_type) * num_shard_senders);
}

void MulticastGroup::delivery_trigger(subgroup_id_t subgroup_num, const SubgroupSettings& curr_subgroup_settings,
                                      const uint32_t num_shard_members, DerechoSST& sst) {
    std::lock_guard<std::mutex> lock(msg_state_mtx);
    // compute the min of the seq_num
    message_id_t min_stable_num
            = sst.seq_num[node_id_to_sst_index.at(curr_subgroup_settings.members[0])][subgroup_num];
    for(uint i = 0; i < num_shard_members; ++i) {
        // to avoid a race condition, do not read the same SST entry twice
        min_stable_num = std::min(min_stable_num, (message_id_t)sst.seq_num[node_id_to_sst_index.at(curr_subgroup_settings.members[i])][subgroup_num]);
    }

    bool update_sst = false;
    bool non_null_msgs_delivered = false;
    persistent::version_t assigned_version = INVALID_VERSION;
    while(true) {
        if(locally_stable_rdmc_messages[subgroup_num].empty() && locally_stable_sst_messages[subgroup_num].empty()) {
            break;
        }
        int32_t least_undelivered_rdmc_seq_num, least_undelivered_sst_seq_num;
        least_undelivered_rdmc_seq_num = least_undelivered_sst_seq_num = std::numeric_limits<int32_t>::max();
        if(!locally_stable_rdmc_messages[subgroup_num].empty()) {
            least_undelivered_rdmc_seq_num = locally_stable_rdmc_messages[subgroup_num].begin()->first;
        }
        if(!locally_stable_sst_messages[subgroup_num].empty()) {
            least_undelivered_sst_seq_num = locally_stable_sst_messages[subgroup_num].begin()->first;
        }
        if(least_undelivered_rdmc_seq_num < least_undelivered_sst_seq_num && least_undelivered_rdmc_seq_num <= min_stable_num) {
            update_sst = true;
            whenlog(logger->trace("Subgroup {}, can deliver a locally stable RDMC message: min_stable_num={} and least_undelivered_seq_num={}",
                                  subgroup_num, min_stable_num, least_undelivered_rdmc_seq_num););
            RDMCMessage& msg = locally_stable_rdmc_messages[subgroup_num].begin()->second;
            char* buf = msg.message_buffer.buffer.get();
            uint64_t msg_ts = ((header*)buf)->timestamp;
            //Note: deliver_message frees the RDMC buffer in msg, which is why the timestamp must be saved before calling this
            assigned_version = persistent::combine_int32s(sst.vid[member_index], least_undelivered_rdmc_seq_num);
            deliver_message(msg, subgroup_num, assigned_version);
            non_null_msgs_delivered |= version_message(msg, subgroup_num, assigned_version, msg_ts);
            // free the message buffer only after it version_message has been called
            free_message_buffers[subgroup_num].push_back(std::move(msg.message_buffer));
            sst.delivered_num[member_index][subgroup_num] = least_undelivered_rdmc_seq_num;
            locally_stable_rdmc_messages[subgroup_num].erase(locally_stable_rdmc_messages[subgroup_num].begin());
        } else if(least_undelivered_sst_seq_num < least_undelivered_rdmc_seq_num && least_undelivered_sst_seq_num <= min_stable_num) {
            update_sst = true;
            whenlog(logger->trace("Subgroup {}, can deliver a locally stable SST message: min_stable_num={} and least_undelivered_seq_num={}",
                                  subgroup_num, min_stable_num, least_undelivered_sst_seq_num););
            SSTMessage& msg = locally_stable_sst_messages[subgroup_num].begin()->second;
            char* buf = (char*)msg.buf;
            uint64_t msg_ts = ((header*)buf)->timestamp;
            assigned_version = persistent::combine_int32s(sst.vid[member_index], least_undelivered_sst_seq_num);
            deliver_message(msg, subgroup_num, assigned_version);
            non_null_msgs_delivered |= version_message(msg, subgroup_num, assigned_version, msg_ts);
            sst.delivered_num[member_index][subgroup_num] = least_undelivered_sst_seq_num;
            locally_stable_sst_messages[subgroup_num].erase(locally_stable_sst_messages[subgroup_num].begin());
        } else {
            break;
        }
    }
    if(update_sst) {
        sst.put(get_shard_sst_indices(subgroup_num),
                (char*)std::addressof(sst.delivered_num[0][subgroup_num]) - sst.getBaseAddress(),
                sizeof(decltype(sst.delivered_num)::value_type));
        // post persistence request for ordered mode.
        if(non_null_msgs_delivered) {
            std::get<1>(persistence_manager_callbacks)(subgroup_num, assigned_version);
        }
    }
}
void MulticastGroup::register_predicates() {
    for(const auto& p : subgroup_settings) {
        subgroup_id_t subgroup_num = p.first;
        const SubgroupSettings& curr_subgroup_settings = p.second;
        auto num_shard_members = curr_subgroup_settings.members.size();
        std::vector<int> shard_senders = curr_subgroup_settings.senders;
        auto num_shard_senders = get_num_senders(shard_senders);
        std::map<uint32_t, uint32_t> shard_ranks_by_sender_rank;
        for(uint j = 0, l = 0; j < num_shard_members; ++j) {
            if(shard_senders[j]) {
                shard_ranks_by_sender_rank[l] = j;
                l++;
            }
        }

        auto receiver_pred = [=](const DerechoSST& sst) {
            return receiver_predicate(subgroup_num, curr_subgroup_settings,
                                      shard_ranks_by_sender_rank, num_shard_senders, sst);
        };
        auto batch_size = window_size / 2;
        if(!batch_size) {
            batch_size = 1;
        }
        auto sst_receive_handler_lambda = [=](uint32_t sender_rank, volatile char* data, uint64_t size) {
            sst_receive_handler(subgroup_num, curr_subgroup_settings,
                                shard_ranks_by_sender_rank, num_shard_senders,
                                sender_rank, data, size);
        };
        auto receiver_trig = [=](DerechoSST& sst) mutable {
            receiver_function(subgroup_num, curr_subgroup_settings,
                              shard_ranks_by_sender_rank, num_shard_senders, sst,
                              batch_size, sst_receive_handler_lambda);
        };
        receiver_pred_handles.emplace_back(sst->predicates.insert(receiver_pred, receiver_trig,
                                                                  sst::PredicateType::RECURRENT));

        if(curr_subgroup_settings.mode != Mode::UNORDERED) {
            auto delivery_pred = [this](const DerechoSST& sst) { return true; };
            auto delivery_trig = [=](DerechoSST& sst) mutable {
                delivery_trigger(subgroup_num, curr_subgroup_settings, num_shard_members, sst);
            };

            delivery_pred_handles.emplace_back(sst->predicates.insert(delivery_pred, delivery_trig,
                                                                      sst::PredicateType::RECURRENT));

            auto persistence_pred = [this](const DerechoSST& sst) { return true; };
            auto persistence_trig = [this, subgroup_num, curr_subgroup_settings, num_shard_members, version_seen = (persistent::version_t)INVALID_VERSION](DerechoSST& sst) mutable {
                std::lock_guard<std::mutex> lock(msg_state_mtx);
                // compute the min of the persisted_num
                persistent::version_t min_persisted_num
                        = sst.persisted_num[node_id_to_sst_index.at(curr_subgroup_settings.members[0])][subgroup_num];
                for(uint i = 1; i < num_shard_members; ++i) {
                    if(sst.persisted_num[node_id_to_sst_index.at(curr_subgroup_settings.members[i])][subgroup_num] < min_persisted_num) {
                        min_persisted_num = sst.persisted_num[node_id_to_sst_index.at(curr_subgroup_settings.members[i])][subgroup_num];
                    }
                }
                // callbacks
                if((version_seen < min_persisted_num) && callbacks.global_persistence_callback) {
                    callbacks.global_persistence_callback(subgroup_num, min_persisted_num);
                    version_seen = min_persisted_num;
                }
            };

            persistence_pred_handles.emplace_back(sst->predicates.insert(persistence_pred, persistence_trig, sst::PredicateType::RECURRENT));

            if(curr_subgroup_settings.sender_rank >= 0) {
                auto sender_pred = [this, subgroup_num, curr_subgroup_settings, num_shard_members, num_shard_senders](const DerechoSST& sst) {
                    message_id_t seq_num = next_message_to_deliver[subgroup_num] * num_shard_senders + curr_subgroup_settings.sender_rank;
                    for(uint i = 0; i < num_shard_members; ++i) {
                        if(sst.delivered_num[node_id_to_sst_index.at(curr_subgroup_settings.members[i])][subgroup_num] < seq_num
                           || (sst.persisted_num[node_id_to_sst_index.at(curr_subgroup_settings.members[i])][subgroup_num] < seq_num)) {
                            return false;
                        }
                    }
                    return true;
                };
                auto sender_trig = [this, subgroup_num](DerechoSST& sst) {
                    sender_cv.notify_all();
                    next_message_to_deliver[subgroup_num]++;
                };
                sender_pred_handles.emplace_back(sst->predicates.insert(sender_pred, sender_trig,
                                                                        sst::PredicateType::RECURRENT));
            }
        } else {
            //This subgroup is in UNORDERED mode
            if(curr_subgroup_settings.sender_rank >= 0) {
                auto sender_pred = [this, subgroup_num, curr_subgroup_settings, num_shard_members](const DerechoSST& sst) {
                    for(uint i = 0; i < num_shard_members; ++i) {
                        uint32_t num_received_offset = subgroup_settings.at(subgroup_num).num_received_offset;
                        if(sst.num_received[node_id_to_sst_index.at(curr_subgroup_settings.members[i])][num_received_offset + curr_subgroup_settings.sender_rank]
                           < static_cast<int32_t>(future_message_indices[subgroup_num] - 1 - window_size)) {
                            return false;
                        }
                    }
                    return true;
                };
                auto sender_trig = [this, subgroup_num](DerechoSST& sst) {
                    sender_cv.notify_all();
                };
                sender_pred_handles.emplace_back(sst->predicates.insert(sender_pred, sender_trig,
                                                                        sst::PredicateType::RECURRENT));
            }
        }
    }
}

MulticastGroup::~MulticastGroup() {
    wedge();
    if(timeout_thread.joinable()) {
        timeout_thread.join();
    }
}

long long unsigned int MulticastGroup::compute_max_msg_size(
        const long long unsigned int max_payload_size,
        const long long unsigned int block_size,
        bool using_rdmc) {
    auto max_msg_size = max_payload_size + sizeof(header);
    if(using_rdmc) {
        if(max_msg_size % block_size != 0) {
            max_msg_size = (max_msg_size / block_size + 1) * block_size;
        }
    }
    return max_msg_size;
}

void MulticastGroup::wedge() {
    bool thread_shutdown_existing = thread_shutdown.exchange(true);
    if(thread_shutdown_existing) {  // Wedge has already been called
        return;
    }

    //Consume and remove all the predicate handles
    for(auto handle_iter = sender_pred_handles.begin(); handle_iter != sender_pred_handles.end();) {
        sst->predicates.remove(*handle_iter);
        handle_iter = sender_pred_handles.erase(handle_iter);
    }
    for(auto handle_iter = receiver_pred_handles.begin(); handle_iter != receiver_pred_handles.end();) {
        sst->predicates.remove(*handle_iter);
        handle_iter = receiver_pred_handles.erase(handle_iter);
    }
    for(auto handle_iter = delivery_pred_handles.begin(); handle_iter != delivery_pred_handles.end();) {
        sst->predicates.remove(*handle_iter);
        handle_iter = delivery_pred_handles.erase(handle_iter);
    }
    for(auto handle_iter = persistence_pred_handles.begin(); handle_iter != persistence_pred_handles.end();) {
        sst->predicates.remove(*handle_iter);
        handle_iter = persistence_pred_handles.erase(handle_iter);
    }

    for(uint i = 0; i < num_members; ++i) {
        rdmc::destroy_group(i + rdmc_group_num_offset);
    }

    sender_cv.notify_all();
    if(sender_thread.joinable()) {
        sender_thread.join();
    }
}

void MulticastGroup::send_loop() {
    pthread_setname_np(pthread_self(), "sender_thread");
    subgroup_id_t subgroup_to_send = 0;
    auto should_send_to_subgroup = [&](subgroup_id_t subgroup_num) {
        if(!rdmc_sst_groups_created) {
            return false;
        }
        if(pending_sends[subgroup_num].empty()) {
            return false;
        }
        RDMCMessage& msg = pending_sends[subgroup_num].front();

        int shard_sender_index = subgroup_settings.at(subgroup_num).sender_rank;
        std::vector<int> shard_senders = subgroup_settings.at(subgroup_num).senders;
        uint32_t num_shard_senders = get_num_senders(shard_senders);
        assert(shard_sender_index >= 0);

        if(sst->num_received[member_index][subgroup_settings.at(subgroup_num).num_received_offset + shard_sender_index] < msg.index - 1) {
            return false;
        }

        std::vector<node_id_t> shard_members = subgroup_settings.at(subgroup_num).members;
        auto num_shard_members = shard_members.size();
        assert(num_shard_members >= 1);
        if(subgroup_settings.at(subgroup_num).mode != Mode::UNORDERED) {
            for(uint i = 0; i < num_shard_members; ++i) {
                if(sst->delivered_num[node_id_to_sst_index.at(shard_members[i])][subgroup_num] < static_cast<message_id_t>((msg.index - window_size) * num_shard_senders + shard_sender_index)
                   || (sst->persisted_num[node_id_to_sst_index.at(shard_members[i])][subgroup_num] < static_cast<message_id_t>((msg.index - window_size) * num_shard_senders + shard_sender_index))) {
                    return false;
                }
            }
        } else {
            for(uint i = 0; i < num_shard_members; ++i) {
                auto num_received_offset = subgroup_settings.at(subgroup_num).num_received_offset;
                if(sst->num_received[node_id_to_sst_index.at(shard_members[i])][num_received_offset + shard_sender_index]
                   < static_cast<int32_t>(future_message_indices[subgroup_num] - 1 - window_size)) {
                    return false;
                }
            }
        }

        return true;
    };
    auto should_send = [&]() {
        for(uint i = 1; i <= total_num_subgroups; ++i) {
            auto subgroup_num = (subgroup_to_send + i) % total_num_subgroups;
            if(should_send_to_subgroup(subgroup_num)) {
                subgroup_to_send = subgroup_num;
                return true;
            }
        }
        return false;
    };
    auto should_wake = [&]() { return thread_shutdown || should_send(); };
    try {
        std::unique_lock<std::mutex> lock(msg_state_mtx);
        while(!thread_shutdown) {
            sender_cv.wait(lock, should_wake);
            if(!thread_shutdown) {
                current_sends[subgroup_to_send] = std::move(pending_sends[subgroup_to_send].front());
                whenlog(logger->trace("Calling send in subgroup {} on message {} from sender {}", subgroup_to_send, current_sends[subgroup_to_send]->index, current_sends[subgroup_to_send]->sender_id););
                if(!rdmc::send(subgroup_to_rdmc_group[subgroup_to_send],
                               current_sends[subgroup_to_send]->message_buffer.mr, 0,
                               current_sends[subgroup_to_send]->size)) {
                    throw std::runtime_error("rdmc::send returned false");
                }
                pending_sends[subgroup_to_send].pop();
            }
        }
        std::cout << "DerechoGroup send thread shutting down" << std::endl;
    } catch(const std::exception& e) {
        std::cout << "DerechoGroup send thread had an exception: " << e.what() << std::endl;
    }
}

uint64_t MulticastGroup::get_time() {
    struct timespec start_time;
    clock_gettime(CLOCK_REALTIME, &start_time);
    return start_time.tv_sec * 1e9 + start_time.tv_nsec;
}

const uint64_t MulticastGroup::compute_global_stability_frontier(uint32_t subgroup_num) {
    auto global_stability_frontier = sst->local_stability_frontier[member_index][subgroup_num];
    auto shard_sst_indices = get_shard_sst_indices(subgroup_num);
    for(auto index : shard_sst_indices) {
        global_stability_frontier = std::min(global_stability_frontier, static_cast<uint64_t>(sst->local_stability_frontier[index][subgroup_num]));
    }
    return global_stability_frontier;
}

void MulticastGroup::check_failures_loop() {
    pthread_setname_np(pthread_self(), "timeout_thread");
    while(!thread_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(sender_timeout));
        if(sst) {
            std::unique_lock<std::mutex> lock(msg_state_mtx);
            auto current_time = get_time();
            for(auto p : subgroup_settings) {
                auto subgroup_num = p.first;
                auto members = p.second.members;
                auto sst_indices = get_shard_sst_indices(subgroup_num);
                // clean up timestamps of persisted messages
                auto min_persisted_num = sst->persisted_num[member_index][subgroup_num];
                for(auto i : sst_indices) {
                    if(min_persisted_num < sst->persisted_num[i][subgroup_num]) {
                        min_persisted_num = sst->persisted_num[i][subgroup_num];
                    }
                }
                while(!pending_persistence[subgroup_num].empty() && pending_persistence[subgroup_num].begin()->first <= min_persisted_num) {
                    auto timestamp = pending_persistence[subgroup_num].begin()->second;
                    pending_persistence[subgroup_num].erase(pending_persistence[subgroup_num].begin());
                    pending_message_timestamps[subgroup_num].erase(timestamp);
                }
                if(pending_message_timestamps[subgroup_num].empty()) {
                    sst->local_stability_frontier[member_index][subgroup_num] = current_time;
                } else {
                    sst->local_stability_frontier[member_index][subgroup_num] = std::min(current_time,
                                                                                         *pending_message_timestamps[subgroup_num].begin());
                }
            }
            sst->put_with_completion((char*)std::addressof(sst->local_stability_frontier[0][0]) - sst->getBaseAddress(),
                                     sizeof(sst->local_stability_frontier[0][0]) * sst->local_stability_frontier.size());
        }
    }

    std::cout << "timeout_thread shutting down" << std::endl;
}

// we already hold the lock on msg_state_mtx when we call this
void MulticastGroup::get_buffer_and_send_auto_null(subgroup_id_t subgroup_num) {
    // short-circuits most of the normal checks because
    // we know that we received a message and are sending a null
    long long unsigned int msg_size = sizeof(header);
    // very unlikely that msg_size does not fit in the max_msg_size since we are sending a NULL
    // but the user might not be interested in using SSTMC at all, then sst::max_msg_size can be zero
    if(msg_size > sst_max_msg_size) {
        // Create new Message
        RDMCMessage msg;
        msg.sender_id = members[member_index];
        msg.index = future_message_indices[subgroup_num];
        msg.size = msg_size;
        msg.message_buffer = std::move(free_message_buffers[subgroup_num].back());
        free_message_buffers[subgroup_num].pop_back();

        auto current_time = get_time();
        pending_message_timestamps[subgroup_num].insert(current_time);

        // Fill header
        char* buf = msg.message_buffer.buffer.get();
        ((header*)buf)->header_size = sizeof(header);
        ((header*)buf)->index = msg.index;
        ((header*)buf)->timestamp = current_time;
        ((header*)buf)->cooked_send = false;

        future_message_indices[subgroup_num]++;
        pending_sends[subgroup_num].push(std::move(msg));
        sender_cv.notify_all();
    } else {
        char* buf = (char*)sst_multicast_group_ptrs[subgroup_num]->get_buffer(msg_size);

        assert(buf);

        auto current_time = get_time();
        pending_message_timestamps[subgroup_num].insert(current_time);

        ((header*)buf)->header_size = sizeof(header);
        ((header*)buf)->index = future_message_indices[subgroup_num];
        ((header*)buf)->timestamp = current_time;
        ((header*)buf)->cooked_send = false;

        future_message_indices[subgroup_num]++;
        sst_multicast_group_ptrs[subgroup_num]->send();
    }
}

char* MulticastGroup::get_sendbuffer_ptr(subgroup_id_t subgroup_num,
                                         long long unsigned int payload_size,
                                         bool cooked_send) {
    long long unsigned int msg_size = payload_size + sizeof(header);
    if(msg_size > max_msg_size) {
        std::cout << "Can't send messages of size larger than the maximum message "
                     "size which is equal to "
                  << max_msg_size << std::endl;
        return nullptr;
    }

    std::vector<node_id_t> shard_members = subgroup_settings.at(subgroup_num).members;
    auto num_shard_members = shard_members.size();
    // if the current node is not a sender, shard_sender_index will be -1
    uint32_t num_shard_senders;
    std::vector<int> shard_senders = subgroup_settings.at(subgroup_num).senders;
    int shard_sender_index = subgroup_settings.at(subgroup_num).sender_rank;
    num_shard_senders = get_num_senders(shard_senders);
    assert(shard_sender_index >= 0);

    if(subgroup_settings.at(subgroup_num).mode != Mode::UNORDERED) {
        for(uint i = 0; i < num_shard_members; ++i) {
            if(sst->delivered_num[node_id_to_sst_index.at(shard_members[i])][subgroup_num]
               < static_cast<int32_t>((future_message_indices[subgroup_num] - window_size) * num_shard_senders + shard_sender_index)) {
                return nullptr;
            }
        }
    } else {
        for(uint i = 0; i < num_shard_members; ++i) {
            auto num_received_offset = subgroup_settings.at(subgroup_num).num_received_offset;
            if(sst->num_received[node_id_to_sst_index.at(shard_members[i])][num_received_offset + shard_sender_index]
               < static_cast<int32_t>(future_message_indices[subgroup_num] - window_size)) {
                return nullptr;
            }
        }
    }

    if(msg_size > sst_max_msg_size) {
        if(thread_shutdown) {
            return nullptr;
        }

        if(free_message_buffers[subgroup_num].empty()) {
            return nullptr;
        }

        if(pending_sst_sends[subgroup_num] || next_sends[subgroup_num]) {
            return nullptr;
        }

        // Create new Message
        RDMCMessage msg;
        msg.sender_id = members[member_index];
        msg.index = future_message_indices[subgroup_num];
        msg.size = msg_size;
        msg.message_buffer = std::move(free_message_buffers[subgroup_num].back());
        free_message_buffers[subgroup_num].pop_back();

        auto current_time = get_time();
        pending_message_timestamps[subgroup_num].insert(current_time);

        // Fill header
        char* buf = msg.message_buffer.buffer.get();
        ((header*)buf)->header_size = sizeof(header);
        ((header*)buf)->index = msg.index;
        ((header*)buf)->timestamp = current_time;
        ((header*)buf)->cooked_send = cooked_send;

        next_sends[subgroup_num] = std::move(msg);
        future_message_indices[subgroup_num]++;

        last_transfer_medium[subgroup_num] = true;
        return buf + sizeof(header);
    } else {
        if(pending_sst_sends[subgroup_num] || next_sends[subgroup_num]) {
            return nullptr;
        }

        pending_sst_sends[subgroup_num] = true;
        if(thread_shutdown) {
            pending_sst_sends[subgroup_num] = false;
            return nullptr;
        }
        char* buf = (char*)sst_multicast_group_ptrs[subgroup_num]->get_buffer(msg_size);
        if(!buf) {
            pending_sst_sends[subgroup_num] = false;
            return nullptr;
        }
        auto current_time = get_time();
        pending_message_timestamps[subgroup_num].insert(current_time);

        ((header*)buf)->header_size = sizeof(header);
        ((header*)buf)->index = future_message_indices[subgroup_num];
        ((header*)buf)->timestamp = current_time;
        ((header*)buf)->cooked_send = cooked_send;
        future_message_indices[subgroup_num]++;
        whenlog(logger->trace("Subgroup {}: get_sendbuffer_ptr increased future_message_indices to {}", subgroup_num, future_message_indices[subgroup_num]););

        last_transfer_medium[subgroup_num] = false;
        return buf + sizeof(header);
    }
}

bool MulticastGroup::send(subgroup_id_t subgroup_num, long long unsigned int payload_size,
                          const std::function<void(char* buf)>& msg_generator, bool cooked_send) {
    if(!rdmc_sst_groups_created) {
        return false;
    }
    std::unique_lock<std::mutex> lock(msg_state_mtx);

    char* buf = get_sendbuffer_ptr(subgroup_num, payload_size, cooked_send);
    while(!buf) {
        // Don't want any deadlocks. For example, this thread cannot get a buffer because delivery is lagging
        // but the SST detect thread cannot proceed (and deliver) because it requires the same lock
        // do not use defer_lock in the unique_lock declaration above and move unlock to the end of the loop.
        // That will cause a bug. We want to unlock only when we are sure that buf is nullptr.
        lock.unlock();
        if(thread_shutdown) {
            return false;
        }
        lock.lock();
        buf = get_sendbuffer_ptr(subgroup_num, payload_size, cooked_send);
    }

    // call to the user supplied message generator
    msg_generator(buf);

    if(last_transfer_medium[subgroup_num]) {
        assert(next_sends[subgroup_num]);
        pending_sends[subgroup_num].push(std::move(*next_sends[subgroup_num]));
        next_sends[subgroup_num] = std::nullopt;
        sender_cv.notify_all();
        return true;
    } else {
        sst_multicast_group_ptrs[subgroup_num]->send();
        pending_sst_sends[subgroup_num] = false;
        return true;
    }
}

bool MulticastGroup::check_pending_sst_sends(subgroup_id_t subgroup_num) {
    std::lock_guard<std::mutex> lock(msg_state_mtx);
    return pending_sst_sends[subgroup_num];
}

std::vector<uint32_t> MulticastGroup::get_shard_sst_indices(subgroup_id_t subgroup_num) {
    std::vector<node_id_t> shard_members = subgroup_settings.at(subgroup_num).members;

    std::vector<uint32_t> shard_sst_indices;
    for(auto m : shard_members) {
        shard_sst_indices.push_back(node_id_to_sst_index.at(m));
    }
    return shard_sst_indices;
}

void MulticastGroup::debug_print() {
    using std::cout;
    using std::endl;
    cout << "In DerechoGroup SST has " << sst->get_num_rows()
         << " rows; member_index is " << member_index << endl;
    uint num_received_offset = 0;
    cout << "Printing SST" << endl;
    for(uint subgroup_num = 0; subgroup_num < total_num_subgroups; ++subgroup_num) {
        cout << "Subgroup " << subgroup_num << endl;
        cout << "Printing seq_num, delivered_num" << endl;
        for(uint i = 0; i < num_members; ++i) {
            cout << sst->seq_num[i][subgroup_num] << " " << sst->delivered_num[i][subgroup_num] << endl;
        }
        cout << endl;

        uint32_t num_shard_senders;
        std::vector<int> shard_senders = subgroup_settings.at(subgroup_num).senders;
        num_shard_senders = get_num_senders(shard_senders);
        cout << "Printing last_received_messages" << endl;
        for(uint k = 0; k < num_members; ++k) {
            for(uint i = 0; i < num_shard_senders; ++i) {
                cout << sst->num_received[k][num_received_offset + i] << " ";
            }
            cout << endl;
        }
        num_received_offset += num_shard_senders;
        cout << "Printing multicastSST fields" << endl;
        sst_multicast_group_ptrs[subgroup_num]->debug_print();
        cout << endl;
    }
}

}  // namespace derecho
