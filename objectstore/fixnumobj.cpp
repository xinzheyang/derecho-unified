#include "ObjectStore.hpp"
#include "conf/conf.hpp"
#include <iostream>
#include <time.h>
#define NUM_APP_ARGS (3)

int main(int argc, char** argv) {
    if((argc < (NUM_APP_ARGS + 1)) || ((argc > (NUM_APP_ARGS + 1)) && strcmp("--", argv[argc - NUM_APP_ARGS - 1]))) {
        std::cerr << "Usage: " << argv[0] << " [ derecho-config-list -- ] <aio|bio> num_objs issender(0-not sending, 1-sending)" << std::endl;
        return -1;
    }

    bool use_aio = false;
    if(strcmp("aio", argv[argc - NUM_APP_ARGS]) == 0) {
        use_aio = true;
    } else if(strcmp("bio", argv[argc - NUM_APP_ARGS]) != 0) {
        std::cerr << "unrecognized argument:" << argv[argc - NUM_APP_ARGS] << ". Using bio (blocking io) instead." << std::endl;
    }

    struct timespec t_start, t_end;
    derecho::Conf::initialize(argc, argv);
    std::cout << "Starting object store service..." << std::endl;

    uint64_t num_msg = std::stoi(argv[argc - 2]);
    bool issender = std::stoi(argv[argc - 1]);
    volatile bool done = false;
    // oss - objectstore service
    auto& oss = objectstore::IObjectStoreService::getObjectStoreService(argc, argv,
                                                                        [&](const objectstore::OID& oid, const objectstore::Object& object) {
                                                                            if(oid == num_msg - 1) {
                                                                                done = true;
                                                                            }
                                                                        });
    // print some message
    std::cout << "Object store service started. Is replica:" << std::boolalpha << oss.isReplica()
              << std::noboolalpha << "." << std::endl;

    uint64_t max_msg_size = derecho::getConfUInt64(CONF_DERECHO_MAX_PAYLOAD_SIZE);
    int msg_size = max_msg_size - 128;

    if(issender) {
        char odata[msg_size];
        srand(time(0));
        for(int i = 0; i < msg_size; i++) {
            odata[i] = '1' + (rand() % 74);
        }
        // create a pool of objects
        std::vector<objectstore::Object> objpool;
        for(uint64_t i = 0; i < num_msg; i++) {
            objpool.push_back(objectstore::Object(i, odata, msg_size + 1));
        }

        // trial run to get an approximate number of objects to reach runtime
        clock_gettime(CLOCK_REALTIME, &t_start);
        if(use_aio) {
            for(uint64_t i = 0; i < num_msg; i++) {
                oss.aio_put(objpool[i]);
            }
        } else {
            for(uint64_t i = 0; i < num_msg; i++) {
                oss.bio_put(objpool[i]);
            }
        }
        oss.bio_get(num_msg - 1);
        clock_gettime(CLOCK_REALTIME, &t_end);

        long long int nsec = (t_end.tv_sec - t_start.tv_sec) * 1000000000 + (t_end.tv_nsec - t_start.tv_nsec);
        double msec = (double)nsec / 1000000;

        double thp_mBps = ((double)max_msg_size * num_msg * 1000) / nsec;
        double thp_ops = ((double)num_msg * 1000000000) / nsec;
        std::cout << "timespan:" << msec << " millisecond." << std::endl;
        std::cout << "throughput:" << thp_mBps << "MB/s." << std::endl;
        std::cout << "throughput:" << thp_ops << "op/s." << std::endl;
        std::cout << std::flush;
        oss.leave();
    } else {
        while(!done) {
        }
        oss.leave();
    }
}
