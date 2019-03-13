/**
 * @file group_impl.h
 * @brief Contains implementations of all the ManagedGroup functions
 * @date Apr 22, 2016
 */

#include "spdlog/async.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include <mutils-serialization/SerializationSupport.hpp>

#include "utils/logger.hpp"
#include "container_template_functions.h"
#include "derecho_internal.h"
#include "group.h"
#include "make_kind_map.h"

namespace derecho {

template <typename SubgroupType>
auto& _Group::get_subgroup(uint32_t subgroup_num) {
    return (dynamic_cast<GroupProjection<SubgroupType>*>(this))
            ->get_subgroup(subgroup_num);
}

template <typename ReplicatedType>
Replicated<ReplicatedType>&
GroupProjection<ReplicatedType>::get_subgroup(uint32_t subgroup_num) {
    void* ret{nullptr};
    set_replicated_pointer(std::type_index{typeid(ReplicatedType)}, subgroup_num,
                           &ret);
    return *((Replicated<ReplicatedType>*)ret);
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::set_replicated_pointer(std::type_index type,
                                                       uint32_t subgroup_num,
                                                       void** ret) {
    ((*ret = (type == std::type_index{typeid(ReplicatedTypes)}
                      ? &get_subgroup<ReplicatedTypes>(subgroup_num)
                      : *ret)),
     ...);
}

/* There is only one constructor */
template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::Group(const CallbackSet& callbacks,
                                 const SubgroupInfo& subgroup_info,
                                 std::shared_ptr<IDeserializationContext> deserialization_context,
                                 std::vector<view_upcall_t> _view_upcalls,
                                 Factory<ReplicatedTypes>... factories)
        : whenlog(logger(LoggerFactory::getDefaultLogger()), )
          my_id(getConfUInt32(CONF_DERECHO_LOCAL_ID)),
          is_starting_leader((getConfString(CONF_DERECHO_LOCAL_IP) == getConfString(CONF_DERECHO_LEADER_IP))
                             && (getConfUInt16(CONF_DERECHO_GMS_PORT) == getConfUInt16(CONF_DERECHO_LEADER_GMS_PORT))),
          leader_connection([&]() -> std::optional<tcp::socket> {
              if(!is_starting_leader) {
                  return tcp::socket{getConfString(CONF_DERECHO_LEADER_IP), getConfUInt16(CONF_DERECHO_LEADER_GMS_PORT)};
              }
              return std::nullopt;
          }()),
          user_deserialization_context(deserialization_context),
          persistence_manager(callbacks.local_persistence_callback),
          //Initially empty, all connections are added in the new view callback
          tcp_sockets(std::make_shared<tcp::tcp_connections>(my_id, std::map<node_id_t, std::pair<ip_addr_t, uint16_t>>{{my_id, {getConfString(CONF_DERECHO_LOCAL_IP), getConfUInt16(CONF_DERECHO_RPC_PORT)}}})),
          view_manager([&]() {
              if(is_starting_leader) {
                  return ViewManager(callbacks, subgroup_info,
                                     {std::type_index(typeid(ReplicatedTypes))...},
                                     std::disjunction_v<has_persistent_fields<ReplicatedTypes>...>,
                                     tcp_sockets, objects_by_subgroup_id,
                                     persistence_manager.get_callbacks(),
                                     _view_upcalls);
              } else {
                  return ViewManager(leader_connection.value(), callbacks,
                                     subgroup_info,
                                     {std::type_index(typeid(ReplicatedTypes))...},
                                     std::disjunction_v<has_persistent_fields<ReplicatedTypes>...>,
                                     tcp_sockets, objects_by_subgroup_id,
                                     persistence_manager.get_callbacks(),
                                     _view_upcalls);
              }
          }()),
          rpc_manager(view_manager,deserialization_context.get()),
          factories(make_kind_map(factories...)) {
    set_up_components();
    vector_int64_2d restart_shard_leaders = view_manager.finish_setup();
    std::set<std::pair<subgroup_id_t, node_id_t>> subgroups_and_leaders_to_receive;
    std::unique_ptr<vector_int64_2d> old_shard_leaders;
    if(is_starting_leader) {
        /* If in total restart mode, ViewManager will have computed the members of each shard
         * with the longest logs, and this node will need to receive state from them even
         * though it's the leader. Otherwise, this vector will be empty because the leader
         * normally doesn't need to receive any object state. */
        subgroups_and_leaders_to_receive = construct_objects<ReplicatedTypes...>(
                view_manager.get_current_view().get(), restart_shard_leaders);
    } else {
        // I am a non-leader
        old_shard_leaders = receive_old_shard_leaders(leader_connection.value());
        subgroups_and_leaders_to_receive = construct_objects<ReplicatedTypes...>(
                view_manager.get_current_view().get(), *old_shard_leaders);
    }
    //The next two methods will do nothing unless we're in total restart mode
    view_manager.send_logs_if_total_restart(old_shard_leaders);
    receive_objects(subgroups_and_leaders_to_receive);
    rpc_manager.start_listening();
    view_manager.start();
    persistence_manager.start();
}

template <typename... ReplicatedTypes>
Group<ReplicatedTypes...>::~Group() {
    // shutdown the persistence manager
    // TODO-discussion:
    // Will a nodebe able to come back once it leaves? if not, maybe we should
    // shut it down on leave().
    persistence_manager.shutdown(true);
    tcp_sockets->destroy();
}

template <typename... ReplicatedTypes>
template <typename FirstType, typename... RestTypes>
std::set<std::pair<subgroup_id_t, node_id_t>> Group<ReplicatedTypes...>::construct_objects(
        const View& curr_view,
        const vector_int64_2d& old_shard_leaders) {
    std::set<std::pair<subgroup_id_t, uint32_t>> subgroups_to_receive;
    if(!curr_view.is_adequately_provisioned) {
        return subgroups_to_receive;
    }
    //The numeric type ID of this subgroup type is its position in the ordered list of subgroup types
    const subgroup_type_id_t subgroup_type_id = index_of_type<FirstType, ReplicatedTypes...>;
    const auto& subgroup_ids = curr_view.subgroup_ids_by_type_id.at(subgroup_type_id);
    for(uint32_t subgroup_index = 0; subgroup_index < subgroup_ids.size(); ++subgroup_index) {
        subgroup_id_t subgroup_id = subgroup_ids.at(subgroup_index);
        // Find out if this node is in any shard of this subgroup
        bool in_subgroup = false;
        uint32_t num_shards = curr_view.subgroup_shard_views.at(subgroup_id).size();
        for(uint32_t shard_num = 0; shard_num < num_shards; ++shard_num) {
            const std::vector<node_id_t>& members = curr_view.subgroup_shard_views.at(subgroup_id).at(shard_num).members;
            //"If this node is in subview->members for this shard"
            if(std::find(members.begin(), members.end(), my_id) != members.end()) {
                in_subgroup = true;
                // This node may have been re-assigned from a different shard, in which
                // case we should delete the old shard's object state
                auto old_object = replicated_objects.template get<FirstType>().find(subgroup_index);
                if(old_object != replicated_objects.template get<FirstType>().end() && old_object->second.get_shard_num() != shard_num) {
                    whenlog(logger->debug("Deleting old Replicated Object state for type {}; I was reassigned from shard {} to shard {}", typeid(FirstType).name(), old_object->second.get_shard_num(), shard_num));
                    replicated_objects.template get<FirstType>().erase(old_object);
                    // also erase from objects_by_subgroup_id
                    objects_by_subgroup_id.erase(subgroup_id);
                }
                //If we don't have a Replicated<T> for this (type, subgroup index), we just became a member of the shard
                if(replicated_objects.template get<FirstType>().count(subgroup_index) == 0) {
                    //Determine if there is existing state for this shard that will need to be received
                    bool has_previous_leader = old_shard_leaders.size() > subgroup_id
                                               && old_shard_leaders[subgroup_id].size() > shard_num
                                               && old_shard_leaders[subgroup_id][shard_num] > -1
                                               && old_shard_leaders[subgroup_id][shard_num] != my_id;
                    if(has_previous_leader) {
                        subgroups_to_receive.emplace(subgroup_id, old_shard_leaders[subgroup_id][shard_num]);
                    }
                    if(has_previous_leader && !has_persistent_fields<FirstType>::value) {
                        /* Construct an "empty" Replicated<T>, since all of T's state will
                         * be received from the leader and there are no logs to update */
                        replicated_objects.template get<FirstType>().emplace(
                                subgroup_index, Replicated<FirstType>(subgroup_type_id, my_id,
                                                                      subgroup_id, subgroup_index,
                                                                      shard_num, rpc_manager, this));
                    } else {
                        replicated_objects.template get<FirstType>().emplace(
                                subgroup_index, Replicated<FirstType>(subgroup_type_id, my_id,
                                                                      subgroup_id, subgroup_index, shard_num, rpc_manager,
                                                                      factories.template get<FirstType>(), this));
                    }
                    // Store a reference to the Replicated<T> just constructed
                    objects_by_subgroup_id.emplace(subgroup_id,
                                                   replicated_objects.template get<FirstType>().at(subgroup_index));
                    break;  // This node can be in at most one shard, so stop here
                }
            }
        }
        if(!in_subgroup) {
            // If we have a Replicated<T> for the subgroup, but we're no longer a
            // member, delete it
            auto old_object = replicated_objects.template get<FirstType>().find(subgroup_index);
            if(old_object != replicated_objects.template get<FirstType>().end()) {
                whenlog(logger->debug("Deleting old Replicated Object state (of type {}) for subgroup {} because this node is no longer a member", typeid(FirstType).name(), subgroup_index));
                replicated_objects.template get<FirstType>().erase(old_object);
                objects_by_subgroup_id.erase(subgroup_id);
            }
            // Create an ExternalCaller for the subgroup if we don't already have one
            external_callers.template get<FirstType>().emplace(
                    subgroup_index, ExternalCaller<FirstType>(subgroup_type_id,
                                                              my_id, subgroup_id, rpc_manager));
        }
    }
    return functional_insert(subgroups_to_receive, construct_objects<RestTypes...>(curr_view, old_shard_leaders));
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::set_up_components() {
    //Give PersistenceManager some pointers
    persistence_manager.set_objects(objects_by_subgroup_id);
    persistence_manager.set_view_manager(view_manager);
    //Now that MulticastGroup is constructed, tell it about RPCManager's message handler
    SharedLockedReference<View> curr_view = view_manager.get_current_view();
    curr_view.get().multicast_group->register_rpc_callback([this](subgroup_id_t subgroup, node_id_t sender, char* buf, uint32_t size) {
        rpc_manager.rpc_message_handler(subgroup, sender, buf, size);
    });
    //Now that ViewManager is constructed, register some new-view upcalls for system functionality
    view_manager.add_view_upcall([this](const View& new_view) {
        update_tcp_connections_callback(new_view);
    });
    view_manager.add_view_upcall([this](const View& new_view) {
        rpc_manager.new_view_callback(new_view);
    });
    //ViewManager must call back to Group after a view change in order to call construct_objects,
    //since ViewManager doesn't know the template parameters
    view_manager.register_initialize_objects_upcall([this](node_id_t my_id, const View& view,
                                                           const vector_int64_2d& old_shard_leaders) {
        std::set<std::pair<subgroup_id_t, node_id_t>> subgroups_and_leaders
                = construct_objects<ReplicatedTypes...>(view, old_shard_leaders);
        receive_objects(subgroups_and_leaders);
    });
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::update_tcp_connections_callback(const View& new_view) {
    if(std::find(new_view.joined.begin(), new_view.joined.end(), my_id) != new_view.joined.end()) {
        //If this node is in the joined list, we need to set up a connection to everyone
        for(int i = 0; i < new_view.num_members; ++i) {
            if(new_view.members[i] != my_id) {
                tcp_sockets->add_node(new_view.members[i], {std::get<0>(new_view.member_ips_and_ports[i]), std::get<PORT_TYPE::RPC>(new_view.member_ips_and_ports[i])});
                whendebug(logger->debug("Established a TCP connection to node {}", new_view.members[i]);)
            }
        }
    } else {
        //This node is already a member, so we already have connections to the previous view's members
        for(const node_id_t& joiner_id : new_view.joined) {
            tcp_sockets->add_node(joiner_id,
                                  {std::get<0>(new_view.member_ips_and_ports[new_view.rank_of(joiner_id)]),
                                   std::get<PORT_TYPE::RPC>(new_view.member_ips_and_ports[new_view.rank_of(joiner_id)])});
            whenlog(logger->debug("Established a TCP connection to node {}", joiner_id););
        }
        for(const node_id_t& removed_id : new_view.departed) {
            whenlog(logger->debug("Removing TCP connection for failed node {}", removed_id););
            tcp_sockets->delete_node(removed_id);
        }
    }
}

template <typename... ReplicatedTypes>
std::unique_ptr<std::vector<std::vector<int64_t>>> Group<ReplicatedTypes...>::receive_old_shard_leaders(
        tcp::socket& leader_socket) {
    std::size_t buffer_size;
    leader_socket.read(buffer_size);
    if(buffer_size == 0) {
        return std::make_unique<vector_int64_2d>();
    }
    char buffer[buffer_size];
    leader_socket.read(buffer, buffer_size);
    return mutils::from_bytes<std::vector<std::vector<int64_t>>>(nullptr, buffer);
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
Replicated<SubgroupType>& Group<ReplicatedTypes...>::get_subgroup(uint32_t subgroup_index) {
    if(!view_manager.get_current_view().get().is_adequately_provisioned) {
        throw subgroup_provisioning_exception("View is inadequately provisioned because subgroup provisioning failed!");
    }
    try {
        return replicated_objects.template get<SubgroupType>().at(subgroup_index);
    } catch(std::out_of_range& ex) {
        throw invalid_subgroup_exception("Not a member of the requested subgroup.");
    }
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
ExternalCaller<SubgroupType>& Group<ReplicatedTypes...>::get_nonmember_subgroup(uint32_t subgroup_index) {
    try {
        return external_callers.template get<SubgroupType>().at(subgroup_index);
    } catch(std::out_of_range& ex) {
        throw invalid_subgroup_exception("No ExternalCaller exists for the requested subgroup; this node may be a member of the subgroup");
    }
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
ShardIterator<SubgroupType> Group<ReplicatedTypes...>::get_shard_iterator(uint32_t subgroup_index) {
    try {
        auto& EC = external_callers.template get<SubgroupType>().at(subgroup_index);
        View& curr_view = view_manager.get_current_view().get();
        auto subgroup_id = curr_view.subgroup_ids_by_type_id.at(index_of_type<SubgroupType, ReplicatedTypes...>)
                                   .at(subgroup_index);
        const auto& shard_subviews = curr_view.subgroup_shard_views.at(subgroup_id);
        std::vector<node_id_t> shard_reps(shard_subviews.size());
        for(uint i = 0; i < shard_subviews.size(); ++i) {
            // for shard iteration to be possible, each shard must contain at least one member
            shard_reps[i] = shard_subviews[i].members.at(0);
        }
        return ShardIterator<SubgroupType>(EC, shard_reps);
    } catch(std::out_of_range& ex) {
        throw invalid_subgroup_exception("No ExternalCaller exists for the requested subgroup; this node may be a member of the subgroup");
    }
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::receive_objects(const std::set<std::pair<subgroup_id_t, node_id_t>>& subgroups_and_leaders) {
    //This will receive one object from each shard leader in ascending order of subgroup ID
    for(const auto& subgroup_and_leader : subgroups_and_leaders) {
        LockedReference<std::unique_lock<std::mutex>, tcp::socket> leader_socket
                = tcp_sockets->get_socket(subgroup_and_leader.second);
        ReplicatedObject& subgroup_object = objects_by_subgroup_id.at(subgroup_and_leader.first);
        if(subgroup_object.is_persistent()) {
            int64_t log_tail_length = subgroup_object.get_minimum_latest_persisted_version();
            whenlog(logger->debug("Sending log tail length of {} for subgroup {} to node {}.", log_tail_length, subgroup_and_leader.first, subgroup_and_leader.second));
            leader_socket.get().write(log_tail_length);
        }
        whenlog(logger->debug("Receiving Replicated Object state for subgroup {} from node {}", subgroup_and_leader.first, subgroup_and_leader.second));
        std::size_t buffer_size;
        bool success = leader_socket.get().read(buffer_size);
        assert_always(success);
        char* buffer = new char[buffer_size];
        success = leader_socket.get().read(buffer, buffer_size);
        assert_always(success);
        subgroup_object.receive_object(buffer);
	delete[] buffer;
    }
    whenlog(logger->debug("Done receiving all Replicated Objects from subgroup leaders"));
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::report_failure(const node_id_t who) {
    view_manager.report_failure(who);
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::leave() {
    view_manager.leave();
}

template <typename... ReplicatedTypes>
std::vector<node_id_t> Group<ReplicatedTypes...>::get_members() {
    return view_manager.get_members();
}

template <typename... ReplicatedTypes>
template <typename SubgroupType>
std::vector<std::vector<node_id_t>> Group<ReplicatedTypes...>::get_subgroup_members(uint32_t subgroup_index) {
    return view_manager.get_subgroup_members(index_of_type<SubgroupType, ReplicatedTypes...>, subgroup_index);
}
template <typename... ReplicatedTypes>
template <typename SubgroupType>
int32_t Group<ReplicatedTypes...>::get_my_shard(uint32_t subgroup_index) {
    return view_manager.get_my_shard(index_of_type<SubgroupType, ReplicatedTypes...>, subgroup_index);
}

template <typename... ReplicatedTypes>
int32_t Group<ReplicatedTypes...>::get_my_rank() {
    return view_manager.get_my_rank();
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::barrier_sync() {
    view_manager.barrier_sync();
}

template <typename... ReplicatedTypes>
void Group<ReplicatedTypes...>::debug_print_status() const {
    view_manager.debug_print_status();
}

} /* namespace derecho */
