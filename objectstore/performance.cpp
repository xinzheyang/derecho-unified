#include "ObjectStore.hpp"
#include "conf/conf.hpp"
#include <iostream>
#include <time.h>

int main(int argc, char** argv) {
    struct timespec t_start, t_end;
    derecho::Conf::initialize(argc, argv);
    std::cout << "Starting object store service..." << std::endl;
    // oss - objectstore service
    auto& oss = objectstore::IObjectStoreService::getObjectStoreService(argc, argv,
                                                                        [&](const objectstore::OID& oid, const objectstore::Object& object) {
                                                                            std::cout << "watcher: " << oid << "->" << object << std::endl;
                                                                        });
    // print some message
    std::cout << "Object store service started. Is replica:" << std::boolalpha << oss.isReplica()
              << std::noboolalpha << "." << std::endl;

    int msg_size = 10000;
    int num_msg = 100000;
    char odata[msg_size];
    for(int i = 0; i < msg_size; i++) {
        odata[i] = 'A';
    }

    clock_gettime(CLOCK_REALTIME, &t_start);
    for(int i = 0; i < num_msg; i++) {
        objectstore::Object object(i, odata, msg_size + 1);
        oss.bio_put(object);
    }
    oss.bio_get(num_msg - 1);
    clock_gettime(CLOCK_REALTIME, &t_end);
    long long int nsec = (t_end.tv_sec - t_start.tv_sec) * 1000000000 + (t_end.tv_nsec - t_start.tv_nsec);
    double msec = (double)nsec / 1000000;
    double thp_mBps = ((double)msg_size * num_msg * 1000) / nsec;
    double thp_ops = ((double)num_msg * 1000000000) / nsec;
    std::cout << "timespan:" << msec << " millisecond." << std::endl;
    std::cout << "throughput:" << thp_mBps << "MB/s." << std::endl;
    std::cout << "throughput:" << thp_ops << "op/s." << std::endl;
    std::cout << std::flush;
    while(true) {
    }
}
