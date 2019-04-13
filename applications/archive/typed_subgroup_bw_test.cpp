#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <time.h>
#include <vector>

#include "bytes_object.h"
#include "derecho/derecho.h"
#include <conf/conf.hpp>
#include <mutils-serialization/SerializationSupport.hpp>
#include <persistent/Persistent.hpp>

#define NUM_APP_ARGS (3)

using derecho::Bytes;

/**
 * RPC Object with a single function that accepts a string
 */
class TestObject {
public:
    void fun(const std::string& words) {
    }

    void bytes_fun(const Bytes& bytes) {
    }

    bool finishing_call(int x) {
        return true;
    }

    REGISTER_RPC_FUNCTIONS(TestObject, fun, bytes_fun, finishing_call);
};

int main(int argc, char* argv[]) {
    if((argc < (NUM_APP_ARGS + 1)) || ((argc > (NUM_APP_ARGS + 1)) && strcmp("--", argv[argc - NUM_APP_ARGS - 1]))) {
        std::cout << "Usage:" << argv[0] << " [ derecho-config-list -- ] <num_of_nodes> <num_senders_selector (0 - all senders, 1 - half senders, 2 - one sender)> <num_messages>" << std::endl;
        return -1;
    }

    derecho::Conf::initialize(argc, argv);

    
    const uint num_of_nodes = std::stoi(argv[argc - 3]);
    const uint64_t max_msg_size = derecho::getConfUInt64(CONF_DERECHO_MAX_PAYLOAD_SIZE) - 128;
    const uint num_senders_selector = std::stoi(argv[argc - 2]);
    const uint num_messages = std::stoi(argv[argc - 1]);

    // variable 'done' tracks the end of the test
    volatile bool done = false;
    // callback into the application code at each message delivery
    auto stability_callback = [&num_messages,
                               &done,
                               &num_nodes,
                               num_senders_selector,
                               num_delivered = 0u](uint32_t subgroup, uint32_t sender_id, long long int index, std::optional<std::pair<char*, long long int>> data, persistent::version_t ver) mutable {
        // increment the total number of messages delivered
        ++num_delivered;
        if(num_senders_selector == 0) {
            if(num_delivered == num_messages * num_nodes) {
                done = true;
            }
        } else if(num_senders_selector == 1) {
            if(num_delivered == num_messages * (num_nodes / 2)) {
                done = true;
            }
        } else {
            if(num_delivered == num_messages) {
                done = true;
            }
        }
    };

    derecho::SubgroupInfo subgroup_info{[num_senders_selector, num_of_nodes](
                                                const std::vector<std::type_index>& subgroup_type_order,
                                                const std::unique_ptr<derecho::View>& prev_view, derecho::View& curr_view) {
        if(curr_view.num_members < num_of_nodes) {
            std::cout << "not enough members yet:" << curr_view.num_members << " < " << num_of_nodes << std::endl;
            throw derecho::subgroup_provisioning_exception();
        }
        derecho::subgroup_shard_layout_t subgroup_layout(1);
        auto num_members = curr_view.members.size();

        // all senders case
        if(num_senders_selector == 0) {
            // a call to make_subview without the sender information
            // defaults to all members sending
            subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members, mode));
        } else {
            // configure the number of senders
            vector<int> is_sender(num_members, 1);
            // half senders case
            if(num_senders_selector == 1) {
                // mark members ranked 0 to num_members/2 as non-senders
                for(uint i = 0; i <= (num_members - 1) / 2; ++i) {
                    is_sender[i] = 0;
                }
            } else {
                // mark all members except the last ranked one as non-senders
                for(uint i = 0; i < num_members - 1; ++i) {
                    is_sender[i] = 0;
                }
            }
            // provide the sender information in a call to make_subview
            subgroup_vector[0].emplace_back(curr_view.make_subview(curr_view.members, mode, is_sender));
        }
        curr_view.next_unassigned_rank = curr_view.members.size();
        //Since we know there is only one subgroup type, just put a single entry in the map
        derecho::subgroup_allocation_map_t subgroup_allocation;
        subgroup_allocation.emplace(std::type_index(typeid(RawObject)), std::move(subgroup_vector));
        return subgroup_allocation;
    }};

    auto ba_factory = [](PersistentRegistry*) { return std::make_unique<TestObject>(); };

    derecho::Group<TestObject> group(CallbackSet{stability_callback}, subgroup_info, nullptr, std::vector<derecho::view_upcall_t>{}, ba_factory);
    std::cout << "Finished constructing/joining Group" << std::endl;

    derecho::Replicated<TestObject>& handle = group.get_subgroup<TestObject>();
    //std::string str_1k(max_msg_size, 'x');
    char* bbuf = (char*)malloc(max_msg_size);
    bzero(bbuf, max_msg_size);
    Bytes bytes(bbuf, max_msg_size);

    uint32_t node_rank = group.get_my_rank();
    // this function sends all the messages
    auto send_all = [&]() {
        for(int i = 0; i < num_messages; i++) {
            //handle.ordered_send<RPC_NAME(fun)>(str_1k);
            handle.ordered_send<RPC_NAME(bytes_fun)>(bytes);
        }
    };

    struct timespec t1, t2;
    clock_gettime(CLOCK_REALTIME, &t1);

    // send all messages or skip if not a sender
    if(num_senders_selector == 0) {
        send_all();
    } else if(num_senders_selector == 1) {
        if(node_rank > (num_nodes - 1) / 2) {
            send_all();
        }
    } else {
        if(node_rank == num_of_nodes - 1) {
            send_all();
        }
    }
    // wait for the test to finish
    while(!done) {
    }

//     if(node_rank == 0) {
//         derecho::rpc::QueryResults<bool> results = handle.ordered_send<RPC_NAME(finishing_call)>(0);
//         std::cout << "waiting for response..." << std::endl;
// #pragma GCC diagnostic ignored "-Wunused-variable"
//         decltype(results)::ReplyMap& replies = results.get();
// #pragma GCC diagnostic pop
//     }

    clock_gettime(CLOCK_REALTIME, &t2);
    free(bbuf);

    int64_t nsec = ((int64_t)t2.tv_sec - t1.tv_sec) * 1000000000 + t2.tv_nsec - t1.tv_nsec;
    double msec = (double)nsec / 1000000;
    double thp_gbps = ((double)num_messages * max_msg_size) / nsec;
    double thp_ops = ((double)num_messages * 1000000000) / nsec;
    std::cout << "timespan:" << msec << " millisecond." << std::endl;
    std::cout << "throughput:" << thp_gbps << "GB/s." << std::endl;
    std::cout << "throughput:" << thp_ops << "ops." << std::endl;

    group.barrier_sync();
    group.leave();
}
