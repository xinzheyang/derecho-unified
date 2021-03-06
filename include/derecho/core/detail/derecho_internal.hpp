/**
 * This file include all common types internal to derecho and 
 * not necessarily being known by a client program.
 *
 */
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <sys/types.h>
#include <utility>

#include "../derecho_type_definitions.hpp"
#include <derecho/persistent/HLC.hpp>
#include <derecho/persistent/PersistentTypenames.hpp>

namespace persistent {
class PersistentRegistry;
}

namespace derecho {
/** Type alias for the internal Subgroup IDs generated by ViewManager.
 * This allows us to change exactly which numeric type we use to store it. */
using subgroup_id_t = uint32_t;
/** Type alias for a message's unique "sequence number" or index.
 * This allows us to change exactly which numeric type we use to store it.*/
using message_id_t = int32_t;
/**
 * Type of the numeric ID used to refer to subgroup types within a Group; this is
 * currently computed as the index of the subgroup type within Group's template
 * parameters.
 */
using subgroup_type_id_t = uint32_t;

/** Alias for the type of std::function that is used for message delivery event callbacks. */
// using message_callback_t = std::function<void(subgroup_id_t, node_id_t, message_id_t, char*, long long int, persistent::version_t)>;
using message_callback_t = std::function<void(subgroup_id_t, node_id_t, message_id_t, std::optional<std::pair<char*, long long int>>, persistent::version_t)>;
using persistence_callback_t = std::function<void(subgroup_id_t, persistent::version_t)>;
using rpc_handler_t = std::function<void(subgroup_id_t, node_id_t, char*, uint32_t)>;

/** The type of factory function the user must provide to the Group constructor,
 * to construct each Replicated Object that is assigned to a subgroup */
template <typename T>
using Factory = std::function<std::unique_ptr<T>(persistent::PersistentRegistry*)>;

// for persistence manager
using persistence_manager_make_version_func_t = std::function<void(
        const subgroup_id_t&,
        const persistent::version_t&,
        const HLC&)>;
using persistence_manager_post_persist_func_t = std::function<void(
        const subgroup_id_t&,
        const persistent::version_t&)>;
using persistence_manager_callbacks_t = std::tuple<persistence_manager_make_version_func_t,
                                                   persistence_manager_post_persist_func_t>;

// to post the next version in a subgroup
using subgroup_post_next_version_func_t = std::function<void(
        const subgroup_id_t&,
        const persistent::version_t&,
        const uint64_t&)>;

}  // namespace derecho
