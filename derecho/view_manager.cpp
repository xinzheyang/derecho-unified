/**
 * @file ViewManager.cpp
 *
 * @date Feb 6, 2017
 */

#include <arpa/inet.h>
#include <tuple>

#include "container_template_functions.h"
#include "derecho_exception.h"
#include "replicated.h"  //Needed for the ReplicatedObject interface
#include "view_manager.h"

#include "persistent/Persistent.hpp"
#include "utils/logger.hpp"

namespace derecho {

using lock_guard_t = std::lock_guard<std::mutex>;
using unique_lock_t = std::unique_lock<std::mutex>;
using shared_lock_t = std::shared_lock<std::shared_timed_mutex>;

/* Leader/Restart Leader Constructor */
ViewManager::ViewManager(
        CallbackSet callbacks, const SubgroupInfo& subgroup_info,
        const std::vector<std::type_index>& subgroup_type_order,
        const bool any_persistent_objects,
        const std::shared_ptr<tcp::tcp_connections>& group_tcp_sockets,
        ReplicatedObjectReferenceMap& object_reference_map,
        const persistence_manager_callbacks_t& _persistence_manager_callbacks,
        std::vector<view_upcall_t> _view_upcalls)
        : whenlog(logger(LoggerFactory::getDefaultLogger()), )
          server_socket(getConfUInt16(CONF_DERECHO_GMS_PORT)),
          thread_shutdown(false),
          view_upcalls(_view_upcalls),
          subgroup_info(subgroup_info),
          subgroup_type_order(subgroup_type_order),
          derecho_params(),
          group_member_sockets(group_tcp_sockets),
          subgroup_objects(object_reference_map),
          any_persistent_objects(any_persistent_objects),
          persistence_manager_callbacks(_persistence_manager_callbacks) {
    if(any_persistent_objects) {
        //Attempt to load a saved View from disk, to see if one is there
        curr_view = persistent::loadObject<View>();
    }
    std::map<subgroup_id_t, SubgroupSettings> subgroup_settings_map;
    uint32_t num_received_size = 0;
    bool is_total_restart;
    uint32_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    if(curr_view) {
        is_total_restart = true;
        whenlog(logger->debug("Found view {} on disk", curr_view->vid););
        whenlog(logger->info("Logged View found on disk. Restarting in recovery mode."););
        //The subgroup_type_order can't be serialized, but it's constant across restarts
        curr_view->subgroup_type_order = subgroup_type_order;
        restart_state = std::make_unique<RestartState>();
        restart_state->load_ragged_trim(*curr_view);
        await_rejoining_nodes(my_id, subgroup_settings_map, num_received_size);
    } else {
        is_total_restart = false;
        curr_view = std::make_unique<View>(
                0, std::vector<node_id_t>{my_id},
                std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t>>{
                        {getConfString(CONF_DERECHO_LOCAL_IP),
                         getConfUInt16(CONF_DERECHO_GMS_PORT),
                         getConfUInt16(CONF_DERECHO_RPC_PORT),
                         getConfUInt16(CONF_DERECHO_SST_PORT),
                         getConfUInt16(CONF_DERECHO_RDMC_PORT)}},
                std::vector<char>{0},
                std::vector<node_id_t>{}, std::vector<node_id_t>{},
                0, 0, subgroup_type_order);
        await_first_view(my_id, subgroup_settings_map, num_received_size);
    }
    curr_view->my_rank = curr_view->rank_of(my_id);
    last_suspected = std::vector<bool>(curr_view->members.size());
    if(any_persistent_objects) {
        persistent::saveObject(*curr_view);
    }
    initialize_rdmc_sst();
    whenlog(logger->debug("Initializing SST and RDMC for the first time."););
    construct_multicast_group(callbacks, subgroup_settings_map, num_received_size);
    curr_view->gmsSST->vid[curr_view->my_rank] = curr_view->vid;
    if(is_total_restart) {
        restart_existing_tcp_connections(my_id);
    }
}

/* Non-leader Constructor */
ViewManager::ViewManager(
        tcp::socket& leader_connection, CallbackSet callbacks,
        const SubgroupInfo& subgroup_info,
        const std::vector<std::type_index>& subgroup_type_order,
        const bool any_persistent_objects,
        const std::shared_ptr<tcp::tcp_connections>& group_tcp_sockets,
        ReplicatedObjectReferenceMap& object_reference_map,
        const persistence_manager_callbacks_t& _persistence_manager_callbacks,
        std::vector<view_upcall_t> _view_upcalls)
        : whenlog(logger(LoggerFactory::getDefaultLogger()), )
          server_socket(getConfUInt16(CONF_DERECHO_GMS_PORT)),
          thread_shutdown(false),
          view_upcalls(_view_upcalls),
          subgroup_info(subgroup_info),
          subgroup_type_order(subgroup_type_order),
          derecho_params(),
          group_member_sockets(group_tcp_sockets),
          subgroup_objects(object_reference_map),
          any_persistent_objects(any_persistent_objects),
          persistence_manager_callbacks(_persistence_manager_callbacks) {
    //First, receive the view and parameters over the given socket
    uint32_t my_id = getConfUInt32(CONF_DERECHO_LOCAL_ID);
    bool is_total_restart = receive_configuration(my_id, leader_connection);

    // Set this while we still know my_id
    curr_view->my_rank = curr_view->rank_of(my_id);

    last_suspected = std::vector<bool>(curr_view->members.size());
    initialize_rdmc_sst();
    std::map<subgroup_id_t, SubgroupSettings> subgroup_settings_map;
    uint32_t num_received_size = derive_subgroup_settings(*curr_view, subgroup_settings_map);
    whenlog(logger->trace("Received initial view: {}", curr_view->debug_string()););
    if(any_persistent_objects) {
        //Persist the initial View to disk as soon as possible, which is after my_subgroups has been initialized
        persistent::saveObject(*curr_view);
    }

    whenlog(logger->debug("Initializing SST and RDMC for the first time."););
    construct_multicast_group(callbacks, subgroup_settings_map, num_received_size);
    curr_view->gmsSST->vid[curr_view->my_rank] = curr_view->vid;
    if(is_total_restart) {
        restart_existing_tcp_connections(my_id);
    }
}

ViewManager::~ViewManager() {
    thread_shutdown = true;
    // force accept to return.
    tcp::socket s{"localhost", getConfUInt16(CONF_DERECHO_GMS_PORT)};
    if(client_listener_thread.joinable()) {
        client_listener_thread.join();
    }
    old_views_cv.notify_all();
    if(old_view_cleanup_thread.joinable()) {
        old_view_cleanup_thread.join();
    }
}

/* ----------  1. Constructor Components ------------- */
bool ViewManager::receive_configuration(node_id_t my_id, tcp::socket& leader_connection) {
    JoinResponse leader_response;
    bool leader_redirect;
    do {
        leader_redirect = false;
        bool success;
        whenlog(logger->debug("Socket connected to leader, exchanging IDs."););
        success = leader_connection.write(my_id);
        if(!success) throw derecho_exception("Failed to exchange IDs with the leader! Leader has crashed.");
        success = leader_connection.read(leader_response);
        if(!success) throw derecho_exception("Failed to exchange IDs with the leader! Leader has crashed.");
        if(leader_response.code == JoinResponseCode::ID_IN_USE) {
            whenlog(logger->error("Error! Leader refused connection because ID {} is already in use!", my_id););
            whenlog(logger->flush();) throw derecho_exception("Leader rejected join, ID already in use");
        }
        if(leader_response.code == JoinResponseCode::LEADER_REDIRECT) {
            std::size_t ip_addr_size;
            leader_connection.read(ip_addr_size);
            char buffer[ip_addr_size];
            leader_connection.read(buffer, ip_addr_size);
            ip_addr_t leader_ip(buffer);
            uint16_t leader_gms_port;
            leader_connection.read(leader_gms_port);
            whenlog(logger->info("That node was not the leader! Redirecting to {}", leader_ip););
            //Use move-assignment to reconnect the socket to the given IP address, and try again
            //(good thing that leader_connection reference is mutable)
            leader_connection = tcp::socket(leader_ip, leader_gms_port);
            leader_redirect = true;
        }
    } while(leader_redirect);

    bool is_total_restart = (leader_response.code == JoinResponseCode::TOTAL_RESTART);
    if(is_total_restart) {
        curr_view = persistent::loadObject<View>();
        whenlog(logger->debug("In restart mode, sending view {} to leader", curr_view->vid););
        bool success = leader_connection.write(mutils::bytes_size(*curr_view));
        if(!success) throw derecho_exception("Restart leader crashed before sending a restart View!");
        auto leader_socket_write = [&leader_connection](const char* bytes, std::size_t size) {
            if(!leader_connection.write(bytes, size)) {
                throw derecho_exception("Restart leader crashed before sending a restart View!");
            }
        };
        mutils::post_object(leader_socket_write, *curr_view);
        //Restore this non-serializeable field to curr_view before using it
        curr_view->subgroup_type_order = subgroup_type_order;
        restart_state = std::make_unique<RestartState>();
        restart_state->load_ragged_trim(*curr_view);
        whenlog(logger->debug("In restart mode, sending {} ragged trims to leader", restart_state->logged_ragged_trim.size()););
        /* Protocol: Send the number of RaggedTrim objects, then serialize each RaggedTrim */
        /* Since we know this node is only a member of one shard per subgroup,
         * the size of the outer map (subgroup IDs) is the number of RaggedTrims. */
        success = leader_connection.write(restart_state->logged_ragged_trim.size());
        if(!success) throw derecho_exception("Restart leader crashed before sending a restart View!");
        for(const auto& id_to_shard_map : restart_state->logged_ragged_trim) {
            const std::unique_ptr<RaggedTrim>& ragged_trim = id_to_shard_map.second.begin()->second;  //The inner map has one entry
            success = leader_connection.write(mutils::bytes_size(*ragged_trim));
            if(!success) throw derecho_exception("Restart leader crashed before sending a restart View!");
            mutils::post_object(leader_socket_write, *ragged_trim);
        }
    }
    leader_connection.write(getConfUInt16(CONF_DERECHO_GMS_PORT));
    leader_connection.write(getConfUInt16(CONF_DERECHO_RPC_PORT));
    leader_connection.write(getConfUInt16(CONF_DERECHO_SST_PORT));
    leader_connection.write(getConfUInt16(CONF_DERECHO_RDMC_PORT));

    bool view_confirmed = false;
    while(!view_confirmed) {
        //The leader will first send the size of the necessary buffer, then the serialized View
        std::size_t size_of_view;
        bool success = leader_connection.read(size_of_view);
        if(!success) {
            throw derecho_exception("Leader crashed before it could send the initial View! Try joining again at the new leader.");
        }
        char buffer[size_of_view];
        success = leader_connection.read(buffer, size_of_view);
        if(!success) {
            throw derecho_exception("Leader crashed before it could send the initial View! Try joining again at the new leader.");
        }
        curr_view = mutils::from_bytes<View>(nullptr, buffer);
        //Next, the leader sends DerechoParams
        std::size_t size_of_derecho_params;
        success = leader_connection.read(size_of_derecho_params);
        char buffer2[size_of_derecho_params];
        success = leader_connection.read(buffer2, size_of_derecho_params);
        if(!success) {
            throw derecho_exception("Leader crashed before it could send the initial View! Try joining again at the new leader.");
        }
        derecho_params = *mutils::from_bytes<DerechoParams>(nullptr, buffer2);
        if(is_total_restart) {
            //In total restart mode, the leader will also send the RaggedTrims it has collected
            whenlog(logger->debug("In restart mode, receiving ragged trim from leader"););
            restart_state->logged_ragged_trim.clear();
            std::size_t num_of_ragged_trims;
            leader_connection.read(num_of_ragged_trims);
            for(std::size_t i = 0; i < num_of_ragged_trims; ++i) {
                std::size_t size_of_ragged_trim;
                leader_connection.read(size_of_ragged_trim);
                char buffer[size_of_ragged_trim];
                leader_connection.read(buffer, size_of_ragged_trim);
                std::unique_ptr<RaggedTrim> ragged_trim = mutils::from_bytes<RaggedTrim>(nullptr, buffer);
                //operator[] is intentional: Create an empty inner map at subgroup_id if one does not exist
                restart_state->logged_ragged_trim[ragged_trim->subgroup_id].emplace(
                        ragged_trim->shard_num, std::move(ragged_trim));
            }
        }
        //Next, the leader will send a single bool indicating whether this View has been committed
        //at all newly joining members. If not, we must go back to waiting for a View.
        success = leader_connection.read(view_confirmed);
        if(!success) {
            throw derecho_exception("Leader crashed before it could send the initial View! Try joining again at the new leader.");
        }
        whenlog(logger->debug("Received view {} from leader. View_confirmed = {}", curr_view->vid, view_confirmed););
    }
    //This must be set up locally, since it's not serializable
    curr_view->subgroup_type_order = subgroup_type_order;
    return is_total_restart;
}

std::vector<std::vector<int64_t>> ViewManager::finish_setup() {
    curr_view->gmsSST->put();
    curr_view->gmsSST->sync_with_members();
    whenlog(logger->debug("Done setting up initial SST and RDMC"););

    if(curr_view->vid != 0 && curr_view->my_rank != curr_view->rank_of_leader()) {
        // If this node is joining an existing group with a non-initial view, copy the leader's num_changes, num_acked, and num_committed
        // Otherwise, you'll immediately think that there's a new proposed view change because gmsSST.num_changes[leader] > num_acked[my_rank]
        curr_view->gmsSST->init_local_change_proposals(curr_view->rank_of_leader());
        curr_view->gmsSST->put();
        whenlog(logger->debug("Joining node initialized its SST row from the leader"););
    }
    create_threads();
    register_predicates();

    shared_lock_t lock(view_mutex);
    for(auto& view_upcall : view_upcalls) {
        view_upcall(*curr_view);
    }
    if(restart_state) {
        //Hand this vector back to the Group constructor so it can call receive_objects if this node is the restart leader
        return restart_state->restart_shard_leaders;
    } else {
        return std::vector<std::vector<int64_t>>{};
    }
}

void ViewManager::send_logs_if_total_restart(const std::unique_ptr<std::vector<std::vector<int64_t>>>& shard_leaders) {
    if(!restart_state) {
        return;
    }
    /* The leader will call this method with nullptr, because it already set restart_shard_leaders
     * in await_rejoining_nodes, but for other nodes this is the first point at which they find out
     * the restart leaders. */
    if(shard_leaders) {
        restart_state->restart_shard_leaders = *shard_leaders;
    }
    node_id_t my_id = curr_view->members[curr_view->my_rank];
    for(subgroup_id_t subgroup_id = 0; subgroup_id < restart_state->restart_shard_leaders.size(); ++subgroup_id) {
        for(uint32_t shard = 0; shard < restart_state->restart_shard_leaders[subgroup_id].size(); ++shard) {
            if(my_id == restart_state->restart_shard_leaders[subgroup_id][shard]) {
                whenlog(logger->debug("This node is the restart leader for subgroup {}, shard {}. Sending object data to shard members.", subgroup_id, shard););
                //Send object data to all shard members, since they will all be in receive_objects()
                for(node_id_t shard_member : curr_view->subgroup_shard_views[subgroup_id][shard].members) {
                    if(shard_member != my_id) {
                        send_subgroup_object(subgroup_id, shard_member);
                    }
                }
            }
        }
    }
}

void ViewManager::restart_existing_tcp_connections(node_id_t my_id) {
    /* If this node is marked as a joiner in the restart view, it will establish TCP
     * connections to everyone in the new view upcall anyway, so do nothing. */
    if(std::find(curr_view->joined.begin(), curr_view->joined.end(), my_id) != curr_view->joined.end()) {
        return;
    }
    /* If this node is not a joiner, it "should" already have a TCP connection to every
     * other current member, so establish those TCP connections. */
    for(int i = 0; i < curr_view->num_members; ++i) {
        if(curr_view->members[i] != my_id
                && std::find(curr_view->joined.begin(), curr_view->joined.end(),
                             curr_view->members[i]) == curr_view->joined.end()) {
            group_member_sockets->add_node(curr_view->members[i], {std::get<0>(curr_view->member_ips_and_ports[i]), std::get<PORT_TYPE::RPC>(curr_view->member_ips_and_ports[i])});
            whendebug(logger->debug("Established a TCP connection to node {}", curr_view->members[i]);)
        }
    }
}

void ViewManager::start() {
    /* If this node is doing a total restart, it has just received the tails of any logs it needed
     * in order to implement the leader's ragged trim proposal from the nodes with the longest logs
     * (specifically, it got them in receive_objects). It should now log the ragged trim to disk,
     * and then truncate its own logs if they are longer than the leader's ragged trim proposal.
     */
    if(restart_state) {
        for(const auto& subgroup_and_map : restart_state->logged_ragged_trim) {
            for(const auto& shard_and_trim : subgroup_and_map.second) {
                persistent::saveObject(*shard_and_trim.second,
                                       ragged_trim_filename(subgroup_and_map.first, shard_and_trim.first).c_str());
            }
        }
        whenlog(logger->debug("Truncating persistent logs to conform to leader's ragged trim"););
        truncate_persistent_logs(restart_state->logged_ragged_trim);
        //Once this is finished, we no longer need logged_ragged_trim or restart_shard_leaders
        restart_state.reset();
    }
    whenlog(logger->debug("Starting predicate evaluation"););
    curr_view->gmsSST->start_predicate_evaluation();
}

void ViewManager::truncate_persistent_logs(const ragged_trim_map_t& logged_ragged_trim) {
    for(const auto& id_to_shard_map : logged_ragged_trim) {
        subgroup_id_t subgroup_id = id_to_shard_map.first;
        const auto find_my_shard = curr_view->my_subgroups.find(subgroup_id);
        if(find_my_shard == curr_view->my_subgroups.end()) {
            continue;
        }
        const auto& my_shard_ragged_trim = id_to_shard_map.second.at(find_my_shard->second);
        persistent::version_t max_delivered_version = RestartState::ragged_trim_to_latest_version(
                my_shard_ragged_trim->vid, my_shard_ragged_trim->max_received_by_sender);
        whenlog(logger->trace("Truncating persistent log for subgroup {} to version {}", subgroup_id, max_delivered_version););
        whenlog(logger->flush(););
        subgroup_objects.at(subgroup_id).get().truncate(max_delivered_version);
    }
}

void ViewManager::await_first_view(const node_id_t my_id,
                                   std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings,
                                   uint32_t& num_received_size) {
    std::map<node_id_t, tcp::socket> waiting_join_sockets;
    std::set<node_id_t> members_sent_view;
    curr_view->is_adequately_provisioned = false;
    bool joiner_failed = false;
    do {
        while(!curr_view->is_adequately_provisioned) {
            tcp::socket client_socket = server_socket.accept();
            node_id_t joiner_id = 0;
            client_socket.read(joiner_id);
            if(curr_view->rank_of(joiner_id) != -1) {
                client_socket.write(JoinResponse{JoinResponseCode::ID_IN_USE, my_id});
                continue;
            }
            client_socket.write(JoinResponse{JoinResponseCode::OK, my_id});
            uint16_t joiner_gms_port = 0;
            client_socket.read(joiner_gms_port);
            uint16_t joiner_rpc_port = 0;
            client_socket.read(joiner_rpc_port);
            uint16_t joiner_sst_port = 0;
            client_socket.read(joiner_sst_port);
            uint16_t joiner_rdmc_port = 0;
            client_socket.read(joiner_rdmc_port);
            const ip_addr_t& joiner_ip = client_socket.get_remote_ip();
            ip_addr_t my_ip = client_socket.get_self_ip();
            //Construct a new view by appending this joiner to the previous view
            //None of these views are ever installed, so we don't use curr_view/next_view like normal
            curr_view = std::make_unique<View>(curr_view->vid,
                                               functional_append(curr_view->members, joiner_id),
                                               functional_append(curr_view->member_ips_and_ports, {joiner_ip, joiner_gms_port, joiner_rpc_port, joiner_sst_port, joiner_rdmc_port}),
                                               std::vector<char>(curr_view->num_members + 1, 0),
                                               functional_append(curr_view->joined, joiner_id),
                                               std::vector<node_id_t>{}, 0, 0,
                                               subgroup_type_order);
            num_received_size = make_subgroup_maps(subgroup_info, std::unique_ptr<View>(),
                                                   *curr_view, subgroup_settings);
            waiting_join_sockets.emplace(joiner_id, std::move(client_socket));
            whenlog(logger->debug("Node {} connected from IP address {} and GMS port {}", joiner_id, joiner_ip, joiner_gms_port););
        }
        joiner_failed = false;
        for(auto waiting_sockets_iter = waiting_join_sockets.begin();
            waiting_sockets_iter != waiting_join_sockets.end();) {
            std::size_t view_buffer_size = mutils::bytes_size(*curr_view);
            std::size_t params_buffer_size = mutils::bytes_size(derecho_params);
            char view_buffer[view_buffer_size];
            char params_buffer[params_buffer_size];
            bool send_success;
            //Within this try block, any send that returns failure throws the ID of the node that failed
            try {
                send_success = waiting_sockets_iter->second.write(view_buffer_size);
                if(!send_success) {
                    throw waiting_sockets_iter->first;
                }
                mutils::to_bytes(*curr_view, view_buffer);
                send_success = waiting_sockets_iter->second.write(view_buffer, view_buffer_size);
                if(!send_success) {
                    throw waiting_sockets_iter->first;
                }
                send_success = waiting_sockets_iter->second.write(params_buffer_size);
                if(!send_success) {
                    throw waiting_sockets_iter->first;
                }
                mutils::to_bytes(derecho_params, params_buffer);
                send_success = waiting_sockets_iter->second.write(params_buffer, params_buffer_size);
                if(!send_success) {
                    throw waiting_sockets_iter->first;
                }
                members_sent_view.emplace(waiting_sockets_iter->first);
                waiting_sockets_iter++;
            } catch(node_id_t failed_joiner_id) {
                whenlog(logger->info("Node {} failed after contacting the leader! Removing it from the initial view.", failed_joiner_id););
                //Remove the failed client and recompute the view
                std::vector<node_id_t> filtered_members(curr_view->members.size() - 1);
                std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t>> filtered_ips_and_ports(curr_view->member_ips_and_ports.size() - 1);
                std::vector<node_id_t> filtered_joiners(curr_view->joined.size() - 1);
                std::remove_copy(curr_view->members.begin(), curr_view->members.end(),
                                 filtered_members.begin(), failed_joiner_id);
                std::remove_copy(curr_view->member_ips_and_ports.begin(),
                                 curr_view->member_ips_and_ports.end(),
                                 filtered_ips_and_ports.begin(),
                                 curr_view->member_ips_and_ports[curr_view->rank_of(failed_joiner_id)]);
                std::remove_copy(curr_view->joined.begin(), curr_view->joined.end(),
                                 filtered_joiners.begin(), failed_joiner_id);
                curr_view = std::make_unique<View>(0, filtered_members, filtered_ips_and_ports,
                                                   std::vector<char>(curr_view->num_members - 1, 0), filtered_joiners,
                                                   std::vector<node_id_t>{}, 0, 0,
                                                   subgroup_type_order);
                /* This will update curr_view->is_adequately_provisioned, so set joiner_failed to true
                 * to start over from the beginning and test if we need to wait for more joiners. */
                num_received_size = make_subgroup_maps(subgroup_info, std::unique_ptr<View>(), *curr_view, subgroup_settings);
                waiting_join_sockets.erase(waiting_sockets_iter);
                joiner_failed = true;
                break;
            }
        }  //for (waiting_join_sockets)
        //Tell each node whether to commit or abort the view they received, which is the opposite of joiner_failed
        for(const node_id_t& member_sent_view : members_sent_view) {
            whenlog(logger->debug("Sending view commit message to node {}: {}", member_sent_view, !joiner_failed););
            waiting_join_sockets.at(member_sent_view).write(!joiner_failed);
        }
        members_sent_view.clear();
    } while(joiner_failed);
    whenlog(logger->trace("Decided on initial view: {}", curr_view->debug_string()););
    //At this point, we have successfully sent an initial view to all joining nodes
    //Now send a "0" as the size of the "old shard leaders" vector, since there are no old leaders, and close the socket
    for(auto waiting_sockets_iter = waiting_join_sockets.begin();
        waiting_sockets_iter != waiting_join_sockets.end();) {
        waiting_sockets_iter->second.write(std::size_t{0});
        waiting_sockets_iter = waiting_join_sockets.erase(waiting_sockets_iter);
    }
}

void ViewManager::await_rejoining_nodes(const node_id_t my_id,
                                        std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings,
                                        uint32_t& num_received_size) {
    RestartLeaderState restart_leader_state_machine(std::move(curr_view), *restart_state, subgroup_settings, num_received_size, subgroup_info, my_id);
    bool still_need_quorum = true;
    while(still_need_quorum) {
        restart_leader_state_machine.await_quorum(server_socket);
        whenlog(logger->debug("Reached a quorum of nodes from view {}, created view {}", restart_leader_state_machine.get_curr_view().vid, restart_leader_state_machine.get_restart_view().vid););
        still_need_quorum = false;
        //Compute a final ragged trim
        //Actually, I don't think there's anything to "compute" because
        //we only kept the latest ragged trim from each subgroup and shard
        //So just mark all of the RaggedTrims with the "restart leader" value to stamp them with our approval
        for(auto& subgroup_to_map : restart_state->logged_ragged_trim) {
            for(auto& shard_trim_pair : subgroup_to_map.second) {
                shard_trim_pair.second->leader_id = std::numeric_limits<node_id_t>::max();
            }
        }
        whenlog(restart_leader_state_machine.print_longest_logs(););

        //Send the next view to all the members
        int64_t failed_node_id
                = restart_leader_state_machine.send_restart_view(derecho_params);
        //If a node failed while waiting for the View, abort this restart view and try again
        if(failed_node_id != -1) {
            whenlog(logger->info("Node {} failed while waiting for restart leader to reach a quorum!", failed_node_id););
            restart_leader_state_machine.confirm_restart_view(false);
            still_need_quorum = true;
            //Try recomputing the restart view without the failed node, to see if we can restart anyway
            bool can_restart = restart_leader_state_machine.compute_restart_view();
            while(can_restart) {
                failed_node_id = restart_leader_state_machine.send_restart_view(derecho_params);
                if(failed_node_id != -1) {
                    whenlog(logger->debug("Recomputed View would still have been adequate, but node {} failed while sending it!", failed_node_id););
                    restart_leader_state_machine.confirm_restart_view(false);
                    //Recompute the restart view again, and try again if it's still adequate
                    can_restart = restart_leader_state_machine.compute_restart_view();
                } else {
                    //Successfully sent the recomputed View to all remaining nodes, so we can proceed after all
                    still_need_quorum = false;
                    break;
                }
            }
        }
    }
    whenlog(logger->trace("Decided on restart view: {}", restart_leader_state_machine.get_restart_view().debug_string()););
    //Commit the restart view at all joining clients
    restart_leader_state_machine.confirm_restart_view(true);
    restart_leader_state_machine.send_shard_leaders();
    curr_view = restart_leader_state_machine.take_restart_view();
}

void ViewManager::initialize_rdmc_sst() {
    whenlog(logger->debug("Starting global initialization of RDMC and SST, including internal TCP connection setup"););
    // construct member_ips
    auto member_ips_and_rdmc_ports_map = make_member_ips_and_ports_map<PORT_TYPE::RDMC>(*curr_view);
    if(!rdmc::initialize(member_ips_and_rdmc_ports_map,
                         curr_view->members[curr_view->my_rank])) {
        std::cout << "Global setup failed" << std::endl;
        exit(0);
    }
    auto member_ips_and_sst_ports_map = make_member_ips_and_ports_map<PORT_TYPE::SST>(*curr_view);

#ifdef USE_VERBS_API
    sst::verbs_initialize(member_ips_and_sst_ports_map,
                          curr_view->members[curr_view->my_rank]);
#else
    sst::lf_initialize(member_ips_and_sst_ports_map,
                       curr_view->members[curr_view->my_rank]);
#endif
}

void ViewManager::create_threads() {
    client_listener_thread = std::thread{[this]() {
        pthread_setname_np(pthread_self(), "client_thread");
        while(!thread_shutdown) {
            tcp::socket client_socket = server_socket.accept();
            whenlog(logger->debug("Background thread got a client connection from {}", client_socket.get_remote_ip()););
            pending_join_sockets.locked().access.emplace_back(std::move(client_socket));
        }
        std::cout << "Connection listener thread shutting down." << std::endl;
    }};

    old_view_cleanup_thread = std::thread([this]() {
        pthread_setname_np(pthread_self(), "old_view");
        while(!thread_shutdown) {
            unique_lock_t old_views_lock(old_views_mutex);
            old_views_cv.wait(old_views_lock, [this]() {
                return !old_views.empty() || thread_shutdown;
            });
            if(!thread_shutdown) {
                old_views.front().reset();
                old_views.pop();
            }
        }
        std::cout << "Old View cleanup thread shutting down." << std::endl;
    });
}

void ViewManager::register_predicates() {
    /* Note that each trigger function must be wrapped in a lambda because it's
   * a member function, and lambdas are the only way to bind "this" to a member
   * function invocation. */
    auto suspected_changed = [this](const DerechoSST& sst) {
        return suspected_not_equal(sst, last_suspected);
    };
    auto suspected_changed_trig = [this](DerechoSST& sst) { new_suspicion(sst); };

    auto start_join_pred = [this](const DerechoSST& sst) {
        return curr_view->i_am_leader() && has_pending_join();
    };
    auto start_join_trig = [this](DerechoSST& sst) { leader_start_join(sst); };

    auto reject_join_pred = [this](const DerechoSST& sst) {
        return !curr_view->i_am_leader() && has_pending_join();
    };
    auto reject_join = [this](DerechoSST& sst) { redirect_join_attempt(sst); };

    auto change_commit_ready = [this](const DerechoSST& gmsSST) {
        return curr_view->i_am_leader()
               && min_acked(gmsSST, curr_view->failed) > gmsSST.num_committed[gmsSST.get_local_index()];
    };
    auto commit_change = [this](DerechoSST& sst) { leader_commit_change(sst); };

    auto leader_proposed_change = [this](const DerechoSST& gmsSST) {
        return gmsSST.num_changes[curr_view->rank_of_leader()]
               > gmsSST.num_acked[gmsSST.get_local_index()];
    };
    auto ack_proposed_change = [this](DerechoSST& sst) {
        acknowledge_proposed_change(sst);
    };

    auto leader_committed_changes = [this](const DerechoSST& gmsSST) {
        return gmsSST.num_committed[curr_view->rank_of_leader()]
               > gmsSST.num_installed[curr_view->my_rank];
    };
    auto view_change_trig = [this](DerechoSST& sst) { start_meta_wedge(sst); };

    if(!suspected_changed_handle.is_valid()) {
        suspected_changed_handle = curr_view->gmsSST->predicates.insert(
                suspected_changed, suspected_changed_trig,
                sst::PredicateType::RECURRENT);
    }
    if(!start_join_handle.is_valid()) {
        start_join_handle = curr_view->gmsSST->predicates.insert(
                start_join_pred, start_join_trig, sst::PredicateType::RECURRENT);
    }
    if(!reject_join_handle.is_valid()) {
        reject_join_handle = curr_view->gmsSST->predicates.insert(reject_join_pred, reject_join,
                                                                  sst::PredicateType::RECURRENT);
    }
    if(!change_commit_ready_handle.is_valid()) {
        change_commit_ready_handle = curr_view->gmsSST->predicates.insert(
                change_commit_ready, commit_change, sst::PredicateType::RECURRENT);
    }
    if(!leader_proposed_handle.is_valid()) {
        leader_proposed_handle = curr_view->gmsSST->predicates.insert(
                leader_proposed_change, ack_proposed_change,
                sst::PredicateType::RECURRENT);
    }
    if(!leader_committed_handle.is_valid()) {
        leader_committed_handle = curr_view->gmsSST->predicates.insert(
                leader_committed_changes, view_change_trig,
                sst::PredicateType::ONE_TIME);
    }
}

/* ------------- 2. Predicate-Triggers That Implement View Management Logic ---------- */

void ViewManager::new_suspicion(DerechoSST& gmsSST) {
    whenlog(logger->debug("Suspected[] changed"););
    View& Vc = *curr_view;
    int myRank = curr_view->my_rank;
    int num_left = 0;
    // Aggregate suspicions into gmsSST[myRank].Suspected;
    for(int r = 0; r < Vc.num_members; r++) {
        for(int who = 0; who < Vc.num_members; who++) {
            gmssst::set(gmsSST.suspected[myRank][who],
                        gmsSST.suspected[myRank][who] || gmsSST.suspected[r][who]);
        }
	if (gmsSST.rip[r]) {
	  num_left++;
	}
    }

    for(int q = 0; q < Vc.num_members; q++) {
        // if(gmsSST.suspected[myRank][q] && !Vc.failed[q]) {
        if(gmsSST.suspected[myRank][q] && !last_suspected[q]) {
            // This is safer than copy_suspected, since suspected[] might change during this loop
            last_suspected[q] = gmsSST.suspected[myRank][q];
            whenlog(logger->debug("Marking {} failed", Vc.members[q]););

            if(!gmsSST.rip[myRank] && Vc.num_failed != 0 && (Vc.num_failed - num_left >= (Vc.num_members - num_left + 1) / 2)) {
                throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
            }

            whenlog(logger->debug("GMS telling SST to freeze row {}", q););
            gmsSST.freeze(q);  // Cease to accept new updates from q
            Vc.multicast_group->wedge();
            gmssst::set(gmsSST.wedged[myRank], true);  // RDMC has halted new sends and receives in theView
            Vc.failed[q] = true;
            Vc.num_failed++;

            if(!gmsSST.rip[myRank] && Vc.num_failed != 0 && (Vc.num_failed - num_left >= (Vc.num_members - num_left + 1) / 2)) {
                throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
            }

            // push change to gmsSST.suspected[myRank]
            gmsSST.put(gmsSST.suspected.get_base() - gmsSST.getBaseAddress(),
                       gmsSST.changes.get_base() - gmsSST.suspected.get_base());
            // push change to gmsSST.wedged[myRank]
            gmsSST.put(gmsSST.wedged.get_base() - gmsSST.getBaseAddress(),
                       sizeof(gmsSST.wedged[0]));
            if(Vc.i_am_leader() && !changes_contains(gmsSST, Vc.members[q]))  // Leader initiated
            {
                const int next_change_index = gmsSST.num_changes[myRank] - gmsSST.num_installed[myRank];
                if(next_change_index == (int)gmsSST.changes.size()) {
                    throw derecho_exception("Ran out of room in the pending changes list");
                }

                gmssst::set(gmsSST.changes[myRank][next_change_index],
                            Vc.members[q]);  // Reports the failure (note that q NotIn members)
                gmssst::increment(gmsSST.num_changes[myRank]);
                whenlog(logger->debug("Leader proposed a change to remove failed node {}", Vc.members[q]););
                gmsSST.put(
                        (char*)std::addressof(gmsSST.changes[0][next_change_index]) - gmsSST.getBaseAddress(),
                        sizeof(gmsSST.changes[0][next_change_index]));
                gmsSST.put(gmsSST.num_changes.get_base() - gmsSST.getBaseAddress(),
                           sizeof(gmsSST.num_changes[0]));
            }
        }
    }
}

void ViewManager::leader_start_join(DerechoSST& gmsSST) {
    whenlog(logger->debug("GMS handling a new client connection"););
    {
        //Hold the lock on pending_join_sockets while moving a socket into proposed_join_sockets
        auto pending_join_sockets_locked = pending_join_sockets.locked();
        proposed_join_sockets.splice(proposed_join_sockets.end(),
                                     pending_join_sockets_locked.access,
                                     pending_join_sockets_locked.access.begin());
    }
    bool success = receive_join(proposed_join_sockets.back());
    //If the join failed, close the socket
    if(!success) proposed_join_sockets.pop_back();
}

void ViewManager::redirect_join_attempt(DerechoSST& gmsSST) {
    tcp::socket client_socket;
    {
        auto pending_join_sockets_locked = pending_join_sockets.locked();
        client_socket = std::move(pending_join_sockets_locked.access.front());
        pending_join_sockets_locked.access.pop_front();
    }
    node_id_t joiner_id;
    client_socket.read(joiner_id);
    client_socket.write(JoinResponse{JoinResponseCode::LEADER_REDIRECT,
                                     curr_view->members[curr_view->my_rank]});
    //Send the client the IP address of the current leader
    client_socket.write(mutils::bytes_size(std::get<0>(
            curr_view->member_ips_and_ports[curr_view->rank_of_leader()])));
    auto bind_socket_write = [&client_socket](const char* bytes, std::size_t size) {
        client_socket.write(bytes, size);
    };
    mutils::post_object(bind_socket_write, std::get<0>(
            curr_view->member_ips_and_ports[curr_view->rank_of_leader()]));
    client_socket.write(std::get<PORT_TYPE::GMS>(
            curr_view->member_ips_and_ports[curr_view->rank_of_leader()]));
}

void ViewManager::leader_commit_change(DerechoSST& gmsSST) {
    gmssst::set(gmsSST.num_committed[gmsSST.get_local_index()],
                min_acked(gmsSST, curr_view->failed));  // Leader commits a new request
    whenlog(logger->debug("Leader committing change proposal #{}", gmsSST.num_committed[gmsSST.get_local_index()]););
    gmsSST.put(gmsSST.num_committed.get_base() - gmsSST.getBaseAddress(),
               sizeof(gmsSST.num_committed[0]));
}

void ViewManager::acknowledge_proposed_change(DerechoSST& gmsSST) {
    int myRank = gmsSST.get_local_index();
    int leader = curr_view->rank_of_leader();
    whenlog(logger->debug("Detected that leader proposed change #{}. Acknowledging.", gmsSST.num_changes[leader]););
    if(myRank != leader) {
        // Echo the count
        gmssst::set(gmsSST.num_changes[myRank], gmsSST.num_changes[leader]);

        // Echo (copy) the vector including the new changes
        gmssst::set(gmsSST.changes[myRank], gmsSST.changes[leader],
                    gmsSST.changes.size());
        // Echo the new member's IP and ports
        gmssst::set(gmsSST.joiner_ips[myRank], gmsSST.joiner_ips[leader],
                    gmsSST.joiner_ips.size());
        gmssst::set(gmsSST.joiner_gms_ports[myRank], gmsSST.joiner_gms_ports[leader],
                    gmsSST.joiner_gms_ports.size());
        gmssst::set(gmsSST.joiner_rpc_ports[myRank], gmsSST.joiner_rpc_ports[leader],
                    gmsSST.joiner_rpc_ports.size());
        gmssst::set(gmsSST.joiner_sst_ports[myRank], gmsSST.joiner_sst_ports[leader],
                    gmsSST.joiner_sst_ports.size());
        gmssst::set(gmsSST.joiner_rdmc_ports[myRank], gmsSST.joiner_rdmc_ports[leader],
                    gmsSST.joiner_rdmc_ports.size());
        gmssst::set(gmsSST.num_committed[myRank], gmsSST.num_committed[leader]);
    }

    // Notice a new request, acknowledge it
    gmssst::set(gmsSST.num_acked[myRank], gmsSST.num_changes[myRank]);
    /* breaking the above put statement into individual put calls, to be sure that
   * if we were relying on any ordering guarantees, we won't run into issue when
   * guarantees do not hold*/
    gmsSST.put(gmsSST.changes.get_base() - gmsSST.getBaseAddress(),
               gmsSST.joiner_ips.get_base() - gmsSST.changes.get_base());
    gmsSST.put(gmsSST.joiner_ips.get_base() - gmsSST.getBaseAddress(),
               gmsSST.num_changes.get_base() - gmsSST.joiner_ips.get_base());
    gmsSST.put(gmsSST.num_changes.get_base() - gmsSST.getBaseAddress(),
               gmsSST.num_committed.get_base() - gmsSST.num_changes.get_base());
    gmsSST.put(gmsSST.num_committed.get_base() - gmsSST.getBaseAddress(),
               gmsSST.num_acked.get_base() - gmsSST.num_committed.get_base());
    gmsSST.put(gmsSST.num_acked.get_base() - gmsSST.getBaseAddress(),
               gmsSST.num_installed.get_base() - gmsSST.num_acked.get_base());
    gmsSST.put(gmsSST.num_installed.get_base() - gmsSST.getBaseAddress(),
               gmsSST.num_received.get_base() - gmsSST.num_installed.get_base());
    whenlog(logger->debug("Wedging current view."););
    curr_view->wedge();
    whenlog(logger->debug("Done wedging current view."););
}

void ViewManager::start_meta_wedge(DerechoSST& gmsSST) {
    whenlog(logger->debug("Meta-wedging view {}", curr_view->vid););
    // Disable all the other SST predicates, except suspected_changed and the
    // one I'm about to register
    gmsSST.predicates.remove(start_join_handle);
    gmsSST.predicates.remove(reject_join_handle);
    gmsSST.predicates.remove(change_commit_ready_handle);
    gmsSST.predicates.remove(leader_proposed_handle);

    curr_view->wedge();

    /* We now need to wait for all other nodes to wedge the current view,
   * which is called "meta-wedged." To do that, this predicate trigger
   * creates a new predicate that will fire when meta-wedged is true, and
   * registers the next epoch termination method as its trigger.
   */
    auto is_meta_wedged = [this](const DerechoSST& gmsSST) {
        for(unsigned int n = 0; n < gmsSST.get_num_rows(); ++n) {
            if(!curr_view->failed[n] && !gmsSST.wedged[n]) {
                return false;
            }
        }
        return true;
    };
    auto meta_wedged_continuation = [this](DerechoSST& gmsSST) {
        // Before the first call to terminate_epoch(), heap-allocate this map
        auto next_subgroup_settings = std::make_shared<std::map<subgroup_id_t, SubgroupSettings>>();
        terminate_epoch(next_subgroup_settings, 0, gmsSST);
    };
    gmsSST.predicates.insert(is_meta_wedged, meta_wedged_continuation,
                             sst::PredicateType::ONE_TIME);
}

void ViewManager::terminate_epoch(
        std::shared_ptr<std::map<subgroup_id_t, SubgroupSettings>> next_subgroup_settings,
        uint32_t next_num_received_size, DerechoSST& gmsSST) {
    whenlog(logger->debug("MetaWedged is true; continuing epoch termination"););
    // If this is the first time terminate_epoch() was called, next_view will
    // still be null
    bool first_call = false;
    if(!next_view) {
        first_call = true;
    }
    std::unique_lock<std::shared_timed_mutex> write_lock(view_mutex);
    next_view = make_next_view(curr_view, gmsSST whenlog(, logger));
    whenlog(logger->debug("Checking provisioning of view {}", next_view->vid););
    next_subgroup_settings->clear();
    next_num_received_size = make_subgroup_maps(subgroup_info, curr_view, *next_view,
                                                *next_subgroup_settings);
    if(!next_view->is_adequately_provisioned) {
        whenlog(logger->debug("Next view would not be adequately provisioned, waiting for more joins."););
        if(first_call) {
            // Re-register the predicates for accepting and acknowledging joins
            register_predicates();
            // But remove the one for start_meta_wedge
            gmsSST.predicates.remove(leader_committed_handle);
        }
        // Construct a predicate that watches for any new committed change that is a
        // join
        int curr_num_committed = gmsSST.num_committed[curr_view->rank_of_leader()];
        auto leader_committed_change = [this, curr_num_committed](const DerechoSST& gmsSST) {
            return gmsSST.num_committed[curr_view->rank_of_leader()] > curr_num_committed;
        };
        // Construct a trigger that will re-call finish_view_change() with the same parameters
        auto retry_next_view = [this, next_subgroup_settings,
                                next_num_received_size](DerechoSST& sst) {
            terminate_epoch(next_subgroup_settings, next_num_received_size, sst);
        };
        gmsSST.predicates.insert(leader_committed_change, retry_next_view,
                                 sst::PredicateType::ONE_TIME);
        return;
    }
    // If execution reached here, we have a valid next view

    // go through all subgroups first and acknowledge all messages received through SST
    for(const auto& shard_settings_pair :
        curr_view->multicast_group->get_subgroup_settings()) {
        const subgroup_id_t subgroup_id = shard_settings_pair.first;
        const auto& curr_subgroup_settings = shard_settings_pair.second;
        auto num_shard_members = curr_subgroup_settings.members.size();
        std::vector<int> shard_senders = curr_subgroup_settings.senders;
        auto num_shard_senders = curr_view->multicast_group->get_num_senders(shard_senders);
        std::map<uint32_t, uint32_t> shard_ranks_by_sender_rank;
        for(uint j = 0, l = 0; j < num_shard_members; ++j) {
            if(shard_senders[j]) {
                shard_ranks_by_sender_rank[l] = j;
                l++;
            }
        }
        // wait for all pending sst sends to finish
        while(curr_view->multicast_group->check_pending_sst_sends(subgroup_id)) {
        }
        curr_view->gmsSST->put_with_completion();
        curr_view->gmsSST->sync_with_members(
                curr_view->multicast_group->get_shard_sst_indices(subgroup_id));
        while(curr_view->multicast_group->receiver_predicate(
                subgroup_id, curr_subgroup_settings, shard_ranks_by_sender_rank,
                num_shard_senders, *curr_view->gmsSST)) {
            auto sst_receive_handler_lambda =
                    [this, subgroup_id, curr_subgroup_settings,
                     shard_ranks_by_sender_rank, num_shard_senders](
                            uint32_t sender_rank, volatile char* data, uint32_t size) {
                        curr_view->multicast_group->sst_receive_handler(
                                subgroup_id, curr_subgroup_settings, shard_ranks_by_sender_rank,
                                num_shard_senders, sender_rank, data, size);
                    };
            curr_view->multicast_group->receiver_function(
                    subgroup_id, curr_subgroup_settings, shard_ranks_by_sender_rank,
                    num_shard_senders, *curr_view->gmsSST,
                    curr_view->multicast_group->window_size, sst_receive_handler_lambda);
        }
    }

    curr_view->gmsSST->put_with_completion();
    curr_view->gmsSST->sync_with_members();

    // First, for subgroups in which I'm the shard leader, do RaggedEdgeCleanup for the leader
    auto follower_subgroups_and_shards = std::make_shared<std::map<subgroup_id_t, uint32_t>>();
    for(const auto& shard_settings_pair : curr_view->multicast_group->get_subgroup_settings()) {
        const subgroup_id_t subgroup_id = shard_settings_pair.first;
        const uint32_t shard_num = shard_settings_pair.second.shard_num;
        SubView& shard_view = curr_view->subgroup_shard_views.at(subgroup_id).at(shard_num);
        uint num_shard_senders = 0;
        for(auto v : shard_view.is_sender) {
            if(v) num_shard_senders++;
        }
        if(num_shard_senders) {
            if(shard_view.my_rank == curr_view->subview_rank_of_shard_leader(subgroup_id, shard_num)) {
                leader_ragged_edge_cleanup(
                        subgroup_id,
                        shard_settings_pair.second.num_received_offset, shard_view.members,
                        num_shard_senders);
            } else {
                // Keep track of which subgroups I'm a non-leader in, and what my
                // corresponding shard ID is
                follower_subgroups_and_shards->emplace(subgroup_id, shard_num);
            }
        }
    }

    // Wait for the shard leaders of subgroups I'm not a leader in to post
    // global_min_ready before continuing
    auto leader_global_mins_are_ready =
            [this, follower_subgroups_and_shards](const DerechoSST& gmsSST) {
                for(const auto& subgroup_shard_pair : *follower_subgroups_and_shards) {
                    SubView& shard_view = curr_view->subgroup_shard_views.at(subgroup_shard_pair.first)
                                                  .at(subgroup_shard_pair.second);
                    node_id_t shard_leader = shard_view.members.at(
                            curr_view->subview_rank_of_shard_leader(
                                    subgroup_shard_pair.first, subgroup_shard_pair.second));
                    if(!gmsSST.global_min_ready[curr_view->rank_of(shard_leader)]
                                               [subgroup_shard_pair.first])
                        return false;
                }
                return true;
            };

    auto global_min_ready_continuation =
            [this, follower_subgroups_and_shards, next_subgroup_settings,
             next_num_received_size](DerechoSST& gmsSST) {
                whenlog(logger->debug("GlobalMins are ready for all {} subgroup leaders this node is waiting on", follower_subgroups_and_shards->size()););
                // Finish RaggedEdgeCleanup for subgroups in which I'm not the leader
                for(const auto& subgroup_shard_pair : *follower_subgroups_and_shards) {
                    const subgroup_id_t subgroup_id = subgroup_shard_pair.first;
                    const uint32_t shard_num = subgroup_shard_pair.second;
                    SubView& shard_view = curr_view->subgroup_shard_views.at(subgroup_id)
                                                  .at(shard_num);
                    uint num_shard_senders = 0;
                    for(auto v : shard_view.is_sender) {
                        if(v)
                            num_shard_senders++;
                    }
                    node_id_t shard_leader = shard_view.members[curr_view->subview_rank_of_shard_leader(
                            subgroup_id, shard_num)];
                    follower_ragged_edge_cleanup(
                            subgroup_id,
                            curr_view->rank_of(shard_leader),
                            curr_view->multicast_group->get_subgroup_settings()
                                    .at(subgroup_id)
                                    .num_received_offset,
                            shard_view.members, num_shard_senders);
                }

                // Wait for persistence to finish for messages delivered in RaggedEdgeCleanup
                auto persistence_finished_pred = [this](const DerechoSST& gmsSST) {
                    // For each subgroup/shard that this node is a member of...
                    for(auto subgroup_shard_pair : curr_view->my_subgroups) {
                        subgroup_id_t subgroup_id = subgroup_shard_pair.first;
                        const uint32_t shard_num = subgroup_shard_pair.second;
                        if(curr_view->subgroup_shard_views.at(subgroup_id).at(shard_num).mode == Mode::UNORDERED) {
                            // Skip non-ordered subgroups, they never do persistence
                            continue;
                        }
                        message_id_t last_delivered_seq_num = gmsSST.delivered_num[curr_view->my_rank][subgroup_id];
                        // For each member of that shard...
                        for(const node_id_t& shard_member : curr_view->subgroup_shard_views.at(subgroup_id).at(shard_num).members) {
                            uint member_row = curr_view->rank_of(shard_member);
                            // Check to see if the member persisted up to the ragged edge trim
                            if(!curr_view->failed[member_row]
                               && persistent::unpack_version<int32_t>(gmsSST.persisted_num[member_row][subgroup_id]).second < last_delivered_seq_num) {
                                return false;
                            }
                        }
                    }
                    return true;
                };

                auto finish_view_change_trig =
                        [this, follower_subgroups_and_shards, next_subgroup_settings,
                         next_num_received_size](DerechoSST& gmsSST) {
                            finish_view_change(follower_subgroups_and_shards,
                                               next_subgroup_settings, next_num_received_size,
                                               gmsSST);
                        };

                // Last statement in global_min_ready_continuation: register finish_view_change_trig
                gmsSST.predicates.insert(persistence_finished_pred, finish_view_change_trig,
                                         sst::PredicateType::ONE_TIME);
            };

    // Last statement in finish_view_change: register global_min_ready_continuation
    gmsSST.predicates.insert(leader_global_mins_are_ready, global_min_ready_continuation,
                             sst::PredicateType::ONE_TIME);
}

void ViewManager::finish_view_change(
        std::shared_ptr<std::map<subgroup_id_t, uint32_t>> follower_subgroups_and_shards,
        std::shared_ptr<std::map<subgroup_id_t, SubgroupSettings>> next_subgroup_settings,
        uint32_t next_num_received_size, DerechoSST& gmsSST) {
    std::unique_lock<std::shared_timed_mutex> write_lock(view_mutex);

    // Disable all the other SST predicates, except suspected_changed
    gmsSST.predicates.remove(start_join_handle);
    gmsSST.predicates.remove(reject_join_handle);
    gmsSST.predicates.remove(change_commit_ready_handle);
    gmsSST.predicates.remove(leader_proposed_handle);

    std::list<tcp::socket> joiner_sockets;
    if(curr_view->i_am_leader() && next_view->joined.size() > 0) {
        // If j joins have been committed, pop the next j sockets off
        // proposed_join_sockets and send them the new View (must happen before we
        // try to do SST setup)
        for(std::size_t c = 0; c < next_view->joined.size(); ++c) {
            commit_join(*next_view, proposed_join_sockets.front());
            // save the socket for later
            joiner_sockets.emplace_back(std::move(proposed_join_sockets.front()));
            proposed_join_sockets.pop_front();
        }
    }

    // Delete the last two GMS predicates from the old SST in preparation for deleting it
    gmsSST.predicates.remove(leader_committed_handle);
    gmsSST.predicates.remove(suspected_changed_handle);

    node_id_t my_id = next_view->members[next_view->my_rank];
    whenlog(logger->debug("Starting creation of new SST and DerechoGroup for view {}", next_view->vid););
    for(const node_id_t failed_node_id : next_view->departed) {
        whenlog(logger->debug("Removing global TCP connections for failed node {} from RDMC and SST", failed_node_id););
#ifdef USE_VERBS_API
        rdma::impl::verbs_remove_connection(failed_node_id);
#else
        rdma::impl::lf_remove_connection(failed_node_id);
#endif
        sst::remove_node(failed_node_id);
    }
    // if new members have joined, add their RDMA connections to SST and RDMC
    for(std::size_t i = 0; i < next_view->joined.size(); ++i) {
        // The new members will be the last joined.size() elements of the members lists
        int joiner_rank = next_view->num_members - next_view->joined.size() + i;
        whenlog(logger->debug("Adding RDMC connection to node {}, at IP {} and port {}", next_view->members[joiner_rank], std::get<0>(next_view->member_ips_and_ports[joiner_rank]), std::get<PORT_TYPE::RDMC>(next_view->member_ips_and_ports[joiner_rank])););

#ifdef USE_VERBS_API
        rdma::impl::verbs_add_connection(next_view->members[joiner_rank],
                                         next_view->member_ips_and_ports[joiner_rank], my_id);
#else
        rdma::impl::lf_add_connection(
                next_view->members[joiner_rank],
                std::pair<ip_addr_t, uint16_t>{
                        std::get<0>(next_view->member_ips_and_ports[joiner_rank]),
                        std::get<PORT_TYPE::RDMC>(
                                next_view->member_ips_and_ports[joiner_rank])});
#endif
    }
    for(std::size_t i = 0; i < next_view->joined.size(); ++i) {
        int joiner_rank = next_view->num_members - next_view->joined.size() + i;
        sst::add_node(next_view->members[joiner_rank],
                      std::pair<ip_addr_t, uint16_t>{
                              std::get<0>(next_view->member_ips_and_ports[joiner_rank]),
                              std::get<PORT_TYPE::SST>(
                                      next_view->member_ips_and_ports[joiner_rank])});
    }
    // This will block until everyone responds to SST/RDMC initial handshakes
    transition_multicast_group(*next_subgroup_settings, next_num_received_size);

    // Determine the shard leaders in the old view and re-index them by new subgroup IDs
    std::vector<std::vector<int64_t>> old_shard_leaders_by_id = old_shard_leaders_by_new_ids(*curr_view, *next_view);

    if(curr_view->i_am_leader()) {
        while(!joiner_sockets.empty()) {
            // Send the array of old shard leaders, so the new member knows who to receive from
            std::size_t size_of_vector = mutils::bytes_size(old_shard_leaders_by_id);
            joiner_sockets.front().write(size_of_vector);
            mutils::post_object(
                    [&joiner_sockets](const char* bytes, std::size_t size) {
                        joiner_sockets.front().write(bytes, size);
                    },
                    old_shard_leaders_by_id);
            joiner_sockets.pop_front();
        }
    }
    // New members can now proceed to view_manager.start(), which will call sync()
    next_view->gmsSST->put();
    next_view->gmsSST->sync_with_members();
    whenlog(logger->debug("Done setting up SST and DerechoGroup for view {}", next_view->vid););
    {
        lock_guard_t old_views_lock(old_views_mutex);
        old_views.push(std::move(curr_view));
        old_views_cv.notify_all();
    }
    curr_view = std::move(next_view);

    if(any_persistent_objects) {
        // Write the new view to disk before using it
        persistent::saveObject(*curr_view);
    }

    // Re-initialize last_suspected (suspected[] has been reset to all false in the new view)
    last_suspected.assign(curr_view->members.size(), false);

    // Register predicates in the new view
    register_predicates();

    // First task with my new view...
    if(curr_view->i_am_new_leader()) {
        curr_view->merge_changes();  // Create a combined list of Changes
    }

    // Announce the new view to the application
    for(auto& view_upcall : view_upcalls) {
        view_upcall(*curr_view);
    }

    // One of those view upcalls is a function in Group that sets up TCP connections to the new members
    // After doing that, shard leaders can send them RPC objects
    send_objects_to_new_members(old_shard_leaders_by_id);

    // Re-initialize this node's RPC objects, which includes receiving them
    // from shard leaders if it is newly a member of a subgroup
    whenlog(logger->debug("Initializing local Replicated Objects"););
    initialize_subgroup_objects(my_id, *curr_view, old_shard_leaders_by_id);
    // It's only safe to start evaluating predicates once all RPC objects exist
    curr_view->gmsSST->start_predicate_evaluation();
    view_change_cv.notify_all();
}

/* ------------- 3. Helper Functions for Predicates and Triggers -------------
 */

void ViewManager::construct_multicast_group(CallbackSet callbacks,
                                            const std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings,
                                            const uint32_t num_received_size) {
    const auto num_subgroups = curr_view->subgroup_shard_views.size();
    curr_view->gmsSST = std::make_shared<DerechoSST>(
            sst::SSTParams(curr_view->members, curr_view->members[curr_view->my_rank],
                           [this](const uint32_t node_id) { report_failure(node_id); },
                           curr_view->failed, false),
            num_subgroups, num_received_size, derecho_params.window_size,
            derecho_params.max_smc_payload_size + sizeof(header) + 2 * sizeof(uint64_t));

    curr_view->multicast_group = std::make_unique<MulticastGroup>(
            curr_view->members, curr_view->members[curr_view->my_rank],
            curr_view->gmsSST, callbacks, num_subgroups, subgroup_settings,
            derecho_params,
	    [this](const subgroup_id_t& subgroup_id,const persistent::version_t& ver){
	        assert(subgroup_objects.find(subgroup_id) != subgroup_objects.end());
		subgroup_objects.at(subgroup_id).get().post_next_version(ver);
	    },
	    persistence_manager_callbacks, curr_view->failed);
}

void ViewManager::transition_multicast_group(
        const std::map<subgroup_id_t, SubgroupSettings>& new_subgroup_settings,
        const uint32_t new_num_received_size) {
    const auto num_subgroups = next_view->subgroup_shard_views.size();
    next_view->gmsSST = std::make_shared<DerechoSST>(
            sst::SSTParams(next_view->members, next_view->members[next_view->my_rank],
                           [this](const uint32_t node_id) { report_failure(node_id); },
                           next_view->failed, false),
            num_subgroups, new_num_received_size, derecho_params.window_size,
            derecho_params.max_smc_payload_size + sizeof(header) + 2 * sizeof(uint64_t));

    next_view->multicast_group = std::make_unique<MulticastGroup>(
            next_view->members, next_view->members[next_view->my_rank],
            next_view->gmsSST, std::move(*curr_view->multicast_group), num_subgroups,
            new_subgroup_settings, 
	    [this](const subgroup_id_t& subgroup_id,const persistent::version_t& ver){
	        assert(subgroup_objects.find(subgroup_id) != subgroup_objects.end());
		subgroup_objects.at(subgroup_id).get().post_next_version(ver);
	    },
	    persistence_manager_callbacks, next_view->failed);

    curr_view->multicast_group.reset();

    // Initialize this node's row in the new SST
    int changes_installed = next_view->joined.size() + next_view->departed.size();
    next_view->gmsSST->init_local_row_from_previous(
            (*curr_view->gmsSST), curr_view->my_rank, changes_installed);
    gmssst::set(next_view->gmsSST->vid[next_view->my_rank], next_view->vid);
}

bool ViewManager::receive_join(tcp::socket& client_socket) {
    DerechoSST& gmsSST = *curr_view->gmsSST;
    if((gmsSST.num_changes[curr_view->my_rank] - gmsSST.num_committed[curr_view->my_rank]) == (int)gmsSST.changes.size()) {
        // TODO: this shouldn't throw an exception, it should just block the client
        // until the group stabilizes
        throw derecho_exception("Too many changes to allow a Join right now");
    }

    struct in_addr joiner_ip_packed;
    inet_aton(client_socket.get_remote_ip().c_str(), &joiner_ip_packed);

    node_id_t joining_client_id = 0;
    client_socket.read(joining_client_id);

    if(curr_view->rank_of(joining_client_id) != -1) {
        whenlog(logger->warn("Joining node at IP {} announced it has ID {}, which is already in the View!", client_socket.get_remote_ip(), joining_client_id););
        client_socket.write(JoinResponse{JoinResponseCode::ID_IN_USE,
                                         curr_view->members[curr_view->my_rank]});
        return false;
    }
    client_socket.write(JoinResponse{JoinResponseCode::OK, curr_view->members[curr_view->my_rank]});

    uint16_t joiner_gms_port = 0;
    client_socket.read(joiner_gms_port);
    uint16_t joiner_rpc_port = 0;
    client_socket.read(joiner_rpc_port);
    uint16_t joiner_sst_port = 0;
    client_socket.read(joiner_sst_port);
    uint16_t joiner_rdmc_port = 0;
    client_socket.read(joiner_rdmc_port);

    whenlog(logger->debug("Proposing change to add node {}", joining_client_id););
    size_t next_change = gmsSST.num_changes[curr_view->my_rank] - gmsSST.num_installed[curr_view->my_rank];
    gmssst::set(gmsSST.changes[curr_view->my_rank][next_change],
                joining_client_id);
    gmssst::set(gmsSST.joiner_ips[curr_view->my_rank][next_change],
                joiner_ip_packed.s_addr);
    gmssst::set(gmsSST.joiner_gms_ports[curr_view->my_rank][next_change],
                joiner_gms_port);
    gmssst::set(gmsSST.joiner_rpc_ports[curr_view->my_rank][next_change],
                joiner_rpc_port);
    gmssst::set(gmsSST.joiner_sst_ports[curr_view->my_rank][next_change],
                joiner_sst_port);
    gmssst::set(gmsSST.joiner_rdmc_ports[curr_view->my_rank][next_change],
                joiner_rdmc_port);

    gmssst::increment(gmsSST.num_changes[curr_view->my_rank]);

    whenlog(logger->debug("Wedging view {}", curr_view->vid););
    curr_view->wedge();
    whenlog(logger->debug("Leader done wedging view."););
    // gmsSST.put(gmsSST.changes.get_base() - gmsSST.getBaseAddress(),
    // gmsSST.num_committed.get_base() - gmsSST.changes.get_base());
    /* breaking the above put statement into individual put calls, to be sure
     * that if we were relying on any ordering guarantees, we won't run into
     * issue when guarantees do not hold*/
    gmsSST.put(gmsSST.changes.get_base() - gmsSST.getBaseAddress(),
               gmsSST.joiner_ips.get_base() - gmsSST.changes.get_base());
    gmsSST.put(gmsSST.joiner_ips.get_base() - gmsSST.getBaseAddress(),
               gmsSST.num_changes.get_base() - gmsSST.joiner_ips.get_base());
    gmsSST.put(gmsSST.num_changes.get_base() - gmsSST.getBaseAddress(),
               gmsSST.num_committed.get_base() - gmsSST.num_changes.get_base());
    return true;
}

void ViewManager::commit_join(const View& new_view, tcp::socket& client_socket) {
    whenlog(logger->debug("Sending client the new view"););
    auto bind_socket_write = [&client_socket](const char* bytes, std::size_t size) { client_socket.write(bytes, size); };
    std::size_t size_of_view = mutils::bytes_size(new_view);
    client_socket.write(size_of_view);
    mutils::post_object(bind_socket_write, new_view);
    std::size_t size_of_derecho_params = mutils::bytes_size(derecho_params);
    client_socket.write(size_of_derecho_params);
    mutils::post_object(bind_socket_write, derecho_params);
    //Send True to indicate that the client should commit this View (for compatibility with restart mode)
    client_socket.write(true);
}

void ViewManager::send_objects_to_new_members(const std::vector<std::vector<int64_t>>& old_shard_leaders) {
    node_id_t my_id = curr_view->members[curr_view->my_rank];
    for(subgroup_id_t subgroup_id = 0; subgroup_id < old_shard_leaders.size(); ++subgroup_id) {
        for(uint32_t shard = 0; shard < old_shard_leaders[subgroup_id].size(); ++shard) {
            //if I was the leader of the shard in the old view...
            if(my_id == old_shard_leaders[subgroup_id][shard]) {
                //send its object state to the new members
                for(node_id_t shard_joiner : curr_view->subgroup_shard_views[subgroup_id][shard].joined) {
                    if(shard_joiner != my_id) {
                        send_subgroup_object(subgroup_id, shard_joiner);
                    }
                }
            }
        }
    }
}

/* Note for the future: Since this "send" requires first receiving the log tail length,
 * it's really a blocking receive-then-send. Since all nodes call send_subgroup_object
 * before initialize_subgroup_objects, there's a small chance of a deadlock: node A could
 * be attempting to send an object to node B at the same time as B is attempting to send a
 * different object to A, and neither node will be able to send the log tail length that
 * the other one is waiting on. */
void ViewManager::send_subgroup_object(subgroup_id_t subgroup_id, node_id_t new_node_id) {
    LockedReference<std::unique_lock<std::mutex>, tcp::socket> joiner_socket = group_member_sockets->get_socket(new_node_id);
    ReplicatedObject& subgroup_object = subgroup_objects.at(subgroup_id);
    if(subgroup_object.is_persistent()) {
        //First, read the log tail length sent by the joining node
        int64_t persistent_log_length = 0;
        joiner_socket.get().read(persistent_log_length);
        PersistentRegistry::setEarliestVersionToSerialize(persistent_log_length);
        whenlog(logger->debug("Got log tail length {}", persistent_log_length););
    }
    whenlog(logger->debug("Sending Replicated Object state for subgroup {} to node {}", subgroup_id, new_node_id););
    subgroup_object.send_object(joiner_socket.get());
}

uint32_t ViewManager::compute_num_received_size(const View& view) {
    uint32_t num_received_size = 0;
    for(subgroup_id_t subgroup_num = 0; subgroup_num < view.subgroup_shard_views.size(); ++subgroup_num) {
        uint32_t max_shard_senders = 0;
        for(uint32_t shard_num = 0; shard_num < view.subgroup_shard_views[subgroup_num].size(); ++shard_num) {
            std::size_t shard_size = view.subgroup_shard_views[subgroup_num][shard_num].members.size();
            uint32_t num_shard_senders = view.subgroup_shard_views[subgroup_num][shard_num].num_senders();
            if(num_shard_senders > max_shard_senders) {
                max_shard_senders = shard_size;
            }
        }
        num_received_size += max_shard_senders;
    }
    return num_received_size;
}

uint32_t ViewManager::make_subgroup_maps(const SubgroupInfo& subgroup_info,
                                         const std::unique_ptr<View>& prev_view, View& curr_view,
                                         std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings) {
    uint32_t num_received_offset = 0;
    int32_t initial_next_unassigned_rank = curr_view.next_unassigned_rank;
    curr_view.subgroup_shard_views.clear();
    curr_view.subgroup_ids_by_type_id.clear();
    //A subgroup's type ID is just its index in the ordered list of subgroup types
    for(subgroup_type_id_t subgroup_type_id = 0; subgroup_type_id < curr_view.subgroup_type_order.size(); ++subgroup_type_id) {
        const std::type_index& subgroup_type = curr_view.subgroup_type_order[subgroup_type_id];
        subgroup_shard_layout_t curr_type_subviews;
        try {
            auto temp = subgroup_info.subgroup_membership_function(subgroup_type, prev_view, curr_view);
            // Hack to ensure RVO still works even though curr_type_subviews had to be declared outside this scope
            curr_type_subviews = std::move(temp);
        } catch(subgroup_provisioning_exception& ex) {
            // Mark the view as inadequate and roll back everything done by previous allocation functions
            curr_view.is_adequately_provisioned = false;
            curr_view.next_unassigned_rank = initial_next_unassigned_rank;
            curr_view.subgroup_shard_views.clear();
            curr_view.subgroup_ids_by_type_id.clear();
            subgroup_settings.clear();
            return 0;
        }
        std::size_t num_subgroups = curr_type_subviews.size();
        curr_view.subgroup_ids_by_type_id[subgroup_type_id] = std::vector<subgroup_id_t>(num_subgroups);
        for(uint32_t subgroup_index = 0; subgroup_index < num_subgroups; ++subgroup_index) {
            // Assign this (type, index) pair a new unique subgroup ID
            subgroup_id_t curr_subgroup_num = curr_view.subgroup_shard_views.size();
            curr_view.subgroup_ids_by_type_id[subgroup_type_id][subgroup_index] = curr_subgroup_num;
            uint32_t num_shards = curr_type_subviews.at(subgroup_index).size();
            uint32_t max_shard_senders = 0;
            for(uint shard_num = 0; shard_num < num_shards; ++shard_num) {
                SubView& shard_view = curr_type_subviews.at(subgroup_index).at(shard_num);
                std::size_t shard_size = shard_view.members.size();
                uint32_t num_shard_senders = shard_view.num_senders();
                if(num_shard_senders > max_shard_senders) {
                    max_shard_senders = shard_size;
                }
                // Initialize my_rank in the SubView for this node's ID
                shard_view.my_rank = shard_view.rank_of(curr_view.members[curr_view.my_rank]);
                if(shard_view.my_rank != -1) {
                    // Initialize my_subgroups
                    curr_view.my_subgroups[curr_subgroup_num] = shard_num;
                    // Save the settings for MulticastGroup
                    subgroup_settings[curr_subgroup_num] = {
                            shard_num,
                            (uint32_t)shard_view.my_rank,
                            shard_view.members,
                            shard_view.is_sender,
                            shard_view.sender_rank_of(shard_view.my_rank),
                            num_received_offset,
                            shard_view.mode};
                }
                if(prev_view) {
                    // Initialize this shard's SubView.joined and SubView.departed
                    subgroup_id_t prev_subgroup_id = prev_view->subgroup_ids_by_type_id.at(subgroup_type_id)
                                                             .at(subgroup_index);
                    SubView& prev_shard_view = prev_view->subgroup_shard_views[prev_subgroup_id][shard_num];
                    std::set<node_id_t> prev_members(prev_shard_view.members.begin(),
                                                     prev_shard_view.members.end());
                    std::set<node_id_t> curr_members(shard_view.members.begin(),
                                                     shard_view.members.end());
                    std::set_difference(curr_members.begin(), curr_members.end(),
                                        prev_members.begin(), prev_members.end(),
                                        std::back_inserter(shard_view.joined));
                    std::set_difference(prev_members.begin(), prev_members.end(),
                                        curr_members.begin(), curr_members.end(),
                                        std::back_inserter(shard_view.departed));
                }
            }  // for (shard_num)
            /* Pull the shard->SubView mapping out of the subgroup membership list
             * and save it under its subgroup ID (which was
             * shard_views_by_subgroup.size()) */
            curr_view.subgroup_shard_views.emplace_back(
                    std::move(curr_type_subviews[subgroup_index]));
            num_received_offset += max_shard_senders;
        }  // for (subgroup_index)
    }
    return num_received_offset;
}

uint32_t ViewManager::derive_subgroup_settings(View& curr_view,
                                               std::map<subgroup_id_t, SubgroupSettings>& subgroup_settings) {
    uint32_t num_received_offset = 0;
    curr_view.my_subgroups.clear();
    for(subgroup_id_t subgroup_id = 0; subgroup_id < curr_view.subgroup_shard_views.size(); ++subgroup_id) {
        uint32_t num_shards = curr_view.subgroup_shard_views.at(subgroup_id).size();
        uint32_t max_shard_senders = 0;

        for(uint32_t shard_num = 0; shard_num < num_shards; ++shard_num) {
            SubView& shard_view = curr_view.subgroup_shard_views.at(subgroup_id).at(shard_num);
            std::size_t shard_size = shard_view.members.size();
            uint32_t num_shard_senders = shard_view.num_senders();
            if(num_shard_senders > max_shard_senders) {
                max_shard_senders = shard_size;  //really? why not max_shard_senders = num_shard_senders?
            }
            //Initialize my_rank in the SubView for this node's ID
            shard_view.my_rank = shard_view.rank_of(curr_view.members[curr_view.my_rank]);
            if(shard_view.my_rank != -1) {
                //Initialize my_subgroups
                curr_view.my_subgroups[subgroup_id] = shard_num;
                //Save the settings for MulticastGroup
                subgroup_settings[subgroup_id] = {
                        shard_num,
                        (uint32_t)shard_view.my_rank,
                        shard_view.members,
                        shard_view.is_sender,
                        shard_view.sender_rank_of(shard_view.my_rank),
                        num_received_offset,
                        shard_view.mode};
            }
        }  // for(shard_num)
        num_received_offset += max_shard_senders;
    }  // for(subgroup_id)

    return num_received_offset;
}

std::unique_ptr<View> ViewManager::make_next_view(const std::unique_ptr<View>& curr_view,
                                                  const DerechoSST& gmsSST
                                                  whenlog(, std::shared_ptr<spdlog::logger> logger)) {
    int myRank = curr_view->my_rank;
    std::set<int> leave_ranks;
    std::vector<int> join_indexes;
    // Look through pending changes up to num_committed and filter the joins and leaves
    const int committed_count = gmsSST.num_committed[curr_view->rank_of_leader()]
                                - gmsSST.num_installed[curr_view->rank_of_leader()];
    for(int change_index = 0; change_index < committed_count; change_index++) {
        node_id_t change_id = gmsSST.changes[myRank][change_index];
        int change_rank = curr_view->rank_of(change_id);
        if(change_rank != -1) {
            // Might as well save the rank, since we'll need it again
            leave_ranks.emplace(change_rank);
        } else {
            join_indexes.emplace_back(change_index);
        }
    }

    int next_num_members = curr_view->num_members - leave_ranks.size() + join_indexes.size();
    // Initialize the next view
    std::vector<node_id_t> joined, members(next_num_members), departed;
    std::vector<char> failed(next_num_members);
    std::vector<std::tuple<ip_addr_t, uint16_t, uint16_t, uint16_t, uint16_t>> member_ips_and_ports(next_num_members);
    int next_unassigned_rank = curr_view->next_unassigned_rank;
    for(std::size_t i = 0; i < join_indexes.size(); ++i) {
        const int join_index = join_indexes[i];
        node_id_t joiner_id = gmsSST.changes[myRank][join_index];
        struct in_addr joiner_ip_packed;
        joiner_ip_packed.s_addr = gmsSST.joiner_ips[myRank][join_index];
        char* joiner_ip_cstr = inet_ntoa(joiner_ip_packed);
        std::string joiner_ip(joiner_ip_cstr);

        joined.emplace_back(joiner_id);
        // New members go at the end of the members list, but it may shrink in the new view
        int new_member_rank = curr_view->num_members - leave_ranks.size() + i;
        members[new_member_rank] = joiner_id;
        member_ips_and_ports[new_member_rank] = std::tuple{joiner_ip, gmsSST.joiner_gms_ports[myRank][join_index],
                                                           gmsSST.joiner_rpc_ports[myRank][join_index],
                                                           gmsSST.joiner_sst_ports[myRank][join_index],
                                                           gmsSST.joiner_rdmc_ports[myRank][join_index]};
        whenlog(logger->debug("Next view will add new member with ID {}", joiner_id););
    }
    for(const auto& leaver_rank : leave_ranks) {
        departed.emplace_back(curr_view->members[leaver_rank]);
        // Decrement next_unassigned_rank for every failure,
        // unless the failed node wasn't assigned to a subgroup anyway
        if(leaver_rank <= curr_view->next_unassigned_rank) {
            next_unassigned_rank--;
        }
    }
    whenlog(logger->debug("Next view will exclude {} failed members.", leave_ranks.size()););

    // Copy member information, excluding the members that have failed
    int new_rank = 0;
    for(int old_rank = 0; old_rank < curr_view->num_members; old_rank++) {
        // This is why leave_ranks needs to be a set
        if(leave_ranks.find(old_rank) == leave_ranks.end()) {
            members[new_rank] = curr_view->members[old_rank];
            member_ips_and_ports[new_rank] = curr_view->member_ips_and_ports[old_rank];
            failed[new_rank] = curr_view->failed[old_rank];
            ++new_rank;
        }
    }

    // Initialize my_rank in next_view
    int32_t my_new_rank = -1;
    node_id_t myID = curr_view->members[myRank];
    for(int i = 0; i < next_num_members; ++i) {
        if(members[i] == myID) {
            my_new_rank = i;
            break;
        }
    }
    if(my_new_rank == -1) {
        throw derecho_exception("Some other node reported that I failed.  Node " + std::to_string(myID) + " terminating");
    }

    auto next_view = std::make_unique<View>(
            curr_view->vid + 1, members, member_ips_and_ports, failed, joined,
            departed, my_new_rank, next_unassigned_rank,
            curr_view->subgroup_type_order);
    next_view->i_know_i_am_leader = curr_view->i_know_i_am_leader;
    return std::move(next_view);
}

std::vector<std::vector<int64_t>> ViewManager::old_shard_leaders_by_new_ids(const View& curr_view,
                                                                            const View& next_view) {
    std::vector<std::vector<int64_t>> old_shard_leaders_by_new_id(next_view.subgroup_shard_views.size());
    for(const auto& type_to_old_ids : curr_view.subgroup_ids_by_type_id) {
        for(uint32_t subgroup_index = 0; subgroup_index < type_to_old_ids.second.size(); ++subgroup_index) {
            subgroup_id_t old_subgroup_id = type_to_old_ids.second[subgroup_index];
            //The subgroup is uniquely identified by (type ID, subgroup index) in both old and new views
            subgroup_id_t new_subgroup_id = next_view.subgroup_ids_by_type_id.at(type_to_old_ids.first)
                                                    .at(subgroup_index);
            std::size_t new_num_shards = next_view.subgroup_shard_views[new_subgroup_id].size();
            old_shard_leaders_by_new_id[new_subgroup_id].resize(new_num_shards, -1);
            for(uint32_t shard_num = 0; shard_num < new_num_shards; ++shard_num) {
                int64_t old_shard_leader = -1;
                //Raw subgroups don't have any state to send to new members, so they have no leaders
                if(curr_view.subgroup_type_order.at(type_to_old_ids.first)
                   != std::type_index(typeid(RawObject))) {
                    int old_shard_leader_rank = curr_view.subview_rank_of_shard_leader(old_subgroup_id, shard_num);
                    if(old_shard_leader_rank >= 0) {
                        old_shard_leader = curr_view.subgroup_shard_views[old_subgroup_id][shard_num]
                                                   .members[old_shard_leader_rank];
                    }
                }
                old_shard_leaders_by_new_id[new_subgroup_id][shard_num] = old_shard_leader;
            }  // for(shard_num)
        }      // for(subgroup_index)
    }          // for(type_to_old_ids)
    return old_shard_leaders_by_new_id;
}

bool ViewManager::suspected_not_equal(const DerechoSST& gmsSST, const std::vector<bool>& old) {
    for(unsigned int r = 0; r < gmsSST.get_num_rows(); r++) {
        for(size_t who = 0; who < gmsSST.suspected.size(); who++) {
            if(gmsSST.suspected[r][who] && !old[who]) {
                // std::cout<<__func__<<" returns true:
                // old[who]="<<old[who]<<",who="<<who<<std::endl;
                return true;
            }
        }
    }
    return false;
}

void ViewManager::copy_suspected(const DerechoSST& gmsSST, std::vector<bool>& old) {
    for(size_t who = 0; who < gmsSST.suspected.size(); ++who) {
        old[who] = gmsSST.suspected[gmsSST.get_local_index()][who];
    }
}

bool ViewManager::changes_contains(const DerechoSST& gmsSST, const node_id_t q) {
    int myRow = gmsSST.get_local_index();
    for(int p_index = 0;
        p_index < gmsSST.num_changes[myRow] - gmsSST.num_installed[myRow];
        p_index++) {
        const node_id_t p(const_cast<node_id_t&>(gmsSST.changes[myRow][p_index]));
        if(p == q) {
            return true;
        }
    }
    return false;
}

int ViewManager::min_acked(const DerechoSST& gmsSST, const std::vector<char>& failed) {
    int myRank = gmsSST.get_local_index();
    int min = gmsSST.num_acked[myRank];
    for(size_t n = 0; n < failed.size(); n++) {
        if(!failed[n] && gmsSST.num_acked[n] < min) {
            min = gmsSST.num_acked[n];
        }
    }

    return min;
}

void ViewManager::deliver_in_order(const int shard_leader_rank,
                                   const uint32_t subgroup_num, const uint32_t num_received_offset,
                                   const std::vector<node_id_t>& shard_members, uint num_shard_senders) {
    // Ragged cleanup is finished, deliver in the implied order
    const View& Vc = *curr_view;
    std::vector<int32_t> max_received_indices(num_shard_senders);
    std::stringstream delivery_order;
    for(uint sender_rank = 0; sender_rank < num_shard_senders; sender_rank++) {
        whenlog(if(logger->should_log(spdlog::level::debug)) {
            delivery_order << "Subgroup " << subgroup_num
                           << ", shard " << Vc.my_subgroups.at(subgroup_num)
                           << " " << Vc.members[Vc.my_rank]
                           << ":0..."
                           << Vc.gmsSST->global_min[shard_leader_rank][num_received_offset + sender_rank]
                           << " ";
        });
        max_received_indices[sender_rank]
                = Vc.gmsSST->global_min[shard_leader_rank][num_received_offset + sender_rank];
    }
    if(any_persistent_objects) {
        uint32_t shard_num = Vc.my_subgroups.at(subgroup_num);
        RaggedTrim trim_log{subgroup_num, shard_num, Vc.vid,
            static_cast<int32_t>(Vc.members[Vc.rank_of_leader()]), max_received_indices};
        whenlog(logger->debug("Logging ragged trim to disk"););
        persistent::saveObject(trim_log, ragged_trim_filename(subgroup_num, shard_num).c_str());
    }
    whenlog(logger->debug("Delivering ragged-edge messages in order: {}", delivery_order.str()););
    Vc.multicast_group->deliver_messages_upto(max_received_indices, subgroup_num, num_shard_senders);
}

void ViewManager::leader_ragged_edge_cleanup(const subgroup_id_t subgroup_num,
                                             const uint32_t num_received_offset,
                                             const std::vector<node_id_t>& shard_members,
                                             uint num_shard_senders) {
    whenlog(logger->debug("Running leader RaggedEdgeCleanup for subgroup {}", subgroup_num););
    View& Vc = *curr_view;
    int myRank = Vc.my_rank;
    bool found = false;
    for(uint n = 0; n < shard_members.size() && !found; n++) {
        const auto node_id = shard_members[n];
        const auto node_rank = Vc.rank_of(node_id);
        if(Vc.gmsSST->global_min_ready[node_rank][subgroup_num]) {
            gmssst::set(Vc.gmsSST->global_min[myRank] + num_received_offset,
                        Vc.gmsSST->global_min[node_rank] + num_received_offset, num_shard_senders);
            found = true;
        }
    }

    if(!found) {
        for(uint n = 0; n < num_shard_senders; n++) {
            int min = Vc.gmsSST->num_received[myRank][num_received_offset + n];
            for(uint r = 0; r < shard_members.size(); r++) {
                const auto node_id = shard_members[r];
                const auto node_rank = Vc.rank_of(node_id);
                if(!Vc.failed[node_rank] && min > Vc.gmsSST->num_received[node_rank][num_received_offset + n]) {
                    min = Vc.gmsSST->num_received[node_rank][num_received_offset + n];
                }
            }

            gmssst::set(Vc.gmsSST->global_min[myRank][num_received_offset + n], min);
        }
    }

    whenlog(logger->debug("Shard leader for subgroup {} finished computing global_min", subgroup_num););
    gmssst::set(Vc.gmsSST->global_min_ready[myRank][subgroup_num], true);
    Vc.gmsSST->put(
            Vc.multicast_group->get_shard_sst_indices(subgroup_num),
            (char*)std::addressof(Vc.gmsSST->global_min[0][num_received_offset]) - Vc.gmsSST->getBaseAddress(),
            sizeof(Vc.gmsSST->global_min[0][num_received_offset]) * num_shard_senders);
    Vc.gmsSST->put(
            Vc.multicast_group->get_shard_sst_indices(subgroup_num),
            (char*)std::addressof(Vc.gmsSST->global_min_ready[0][subgroup_num]) - Vc.gmsSST->getBaseAddress(),
            sizeof(Vc.gmsSST->global_min_ready[0][subgroup_num]));

    deliver_in_order(myRank, subgroup_num, num_received_offset, shard_members,
                     num_shard_senders);
    whenlog(logger->debug("Done with RaggedEdgeCleanup for subgroup {}", subgroup_num););
}

void ViewManager::follower_ragged_edge_cleanup(
        const subgroup_id_t subgroup_num, uint shard_leader_rank,
        const uint32_t num_received_offset,
        const std::vector<node_id_t>& shard_members,
        uint num_shard_senders) {
    const View& Vc = *curr_view;
    int myRank = Vc.my_rank;
    // Learn the leader's data and push it before acting upon it
    whenlog(logger->debug("Running follower RaggedEdgeCleanup for subgroup {}; echoing leader's global_min", subgroup_num););
    gmssst::set(Vc.gmsSST->global_min[myRank] + num_received_offset,
                Vc.gmsSST->global_min[shard_leader_rank] + num_received_offset,
                num_shard_senders);
    gmssst::set(Vc.gmsSST->global_min_ready[myRank][subgroup_num], true);
    Vc.gmsSST->put(
            Vc.multicast_group->get_shard_sst_indices(subgroup_num),
            (char*)std::addressof(Vc.gmsSST->global_min[0][num_received_offset]) - Vc.gmsSST->getBaseAddress(),
            sizeof(Vc.gmsSST->global_min[0][num_received_offset]) * num_shard_senders);
    Vc.gmsSST->put(
            Vc.multicast_group->get_shard_sst_indices(subgroup_num),
            (char*)std::addressof(Vc.gmsSST->global_min_ready[0][subgroup_num]) - Vc.gmsSST->getBaseAddress(),
            sizeof(Vc.gmsSST->global_min_ready[0][subgroup_num]));
    deliver_in_order(shard_leader_rank, subgroup_num, num_received_offset,
                     shard_members, num_shard_senders);
    whenlog(logger->debug("Done with RaggedEdgeCleanup for subgroup {}", subgroup_num);)
}

/* ------------- 4. Public-Interface methods of ViewManager ------------- */

void ViewManager::report_failure(const node_id_t who) {
    int r = curr_view->rank_of(who);
    whenlog(logger->debug("Node ID {} failure reported; marking suspected[{}]", who, r););
    curr_view->gmsSST->suspected[curr_view->my_rank][r] = true;
    int failed_cnt = 0;
    int rip_cnt = 0;
    for(r = 0; r < (int)curr_view->gmsSST->suspected.size(); r++) {
	if(curr_view->gmsSST->rip[r]) {
	  ++rip_cnt;
	}
        else if(curr_view->gmsSST->suspected[curr_view->my_rank][r]) {
	  ++failed_cnt;
        }
    }

    if(!curr_view->gmsSST->rip[curr_view->my_rank] && failed_cnt != 0 && (failed_cnt >= (curr_view->num_members - rip_cnt + 1) / 2)) {
        throw derecho_exception("Potential partitioning event: this node is no longer in the majority and must shut down!");
    }
    curr_view->gmsSST->put(
            (char*)std::addressof(curr_view->gmsSST->suspected[0][r]) - curr_view->gmsSST->getBaseAddress(),
            sizeof(curr_view->gmsSST->suspected[0][r]));
}

void ViewManager::leave() {
    shared_lock_t lock(view_mutex);
    whenlog(logger->debug("Cleanly leaving the group."););
    curr_view->multicast_group->wedge();
    curr_view->gmsSST->predicates.clear();
    curr_view->gmsSST->suspected[curr_view->my_rank][curr_view->my_rank] = true;
    curr_view->gmsSST->put(
            (char*)std::addressof(curr_view->gmsSST->suspected[0][curr_view->my_rank])
                    - curr_view->gmsSST->getBaseAddress(),
            sizeof(curr_view->gmsSST->suspected[0][curr_view->my_rank]));
    curr_view->gmsSST->rip[curr_view->my_rank] = true;
    curr_view->gmsSST->put_with_completion(curr_view->gmsSST->rip.get_base() - curr_view->gmsSST->getBaseAddress(),
               sizeof(curr_view->gmsSST->rip[0]));
    thread_shutdown = true;
}

void ViewManager::send(subgroup_id_t subgroup_num, long long unsigned int payload_size,
                       const std::function<void(char* buf)>& msg_generator, bool cooked_send) {
    shared_lock_t lock(view_mutex);
    view_change_cv.wait(lock, [&]() {
        return curr_view->multicast_group->send(subgroup_num, payload_size,
                                                msg_generator, cooked_send);
    });
}

const uint64_t ViewManager::compute_global_stability_frontier(subgroup_id_t subgroup_num) {
    shared_lock_t lock(view_mutex);
    return curr_view->multicast_group->compute_global_stability_frontier(subgroup_num);
}

void ViewManager::add_view_upcall(const view_upcall_t& upcall) {
    view_upcalls.emplace_back(upcall);
}

std::vector<node_id_t> ViewManager::get_members() {
    shared_lock_t read_lock(view_mutex);
    return curr_view->members;
}

int32_t ViewManager::get_my_rank() {
    shared_lock_t read_lock(view_mutex);
    return curr_view->my_rank;
}

std::vector<std::vector<node_id_t>> ViewManager::get_subgroup_members(subgroup_type_id_t subgroup_type, uint32_t subgroup_index) {
    shared_lock_t read_lock(view_mutex);
    subgroup_id_t subgroup_id = curr_view->subgroup_ids_by_type_id.at(subgroup_type).at(subgroup_index);
    std::vector<std::vector<node_id_t>> subgroup_members;
    for(const auto& shard_view : curr_view->subgroup_shard_views.at(subgroup_id)) {
        subgroup_members.push_back(shard_view.members);
    }
    return subgroup_members;
}

int32_t ViewManager::get_my_shard(subgroup_type_id_t subgroup_type, uint32_t subgroup_index) {
    shared_lock_t read_lock(view_mutex);
    subgroup_id_t subgroup_id = curr_view->subgroup_ids_by_type_id.at(subgroup_type).at(subgroup_index);
    auto find_id_result = curr_view->my_subgroups.find(subgroup_id);
    if(find_id_result == curr_view->my_subgroups.end()) {
        return -1;
    } else {
        return find_id_result->second;
    }
}

void ViewManager::barrier_sync() {
    shared_lock_t read_lock(view_mutex);
    curr_view->gmsSST->sync_with_members();
}

SharedLockedReference<View> ViewManager::get_current_view() {
    return SharedLockedReference<View>(*curr_view, view_mutex);
}

void ViewManager::debug_print_status() const {
    std::cout << "curr_view = " << curr_view->debug_string() << std::endl;
}
} /* namespace derecho */
