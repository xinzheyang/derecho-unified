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
                                                                            // std::cout << "watcher: " << oid << "->" << object << std::endl;
                                                                        });
    // print some message
    std::cout << "Object store service started. Is replica:" << std::boolalpha << oss.isReplica()
              << std::noboolalpha << "." << std::endl;
    bool use_aio = true;
    if ( !strcmp("aio",argv[argc-3]) ) {
        use_aio = false;
    }
    int runtime = std::stoi(argv[argc - 2]);
    int num_msg = std::stoi(argv[argc - 1]);
    uint64_t max_msg_size = derecho::getConfUInt64(CONF_DERECHO_MAX_PAYLOAD_SIZE);
    int msg_size = max_msg_size - 128;
    char odata[msg_size];
    for(int i = 0; i < msg_size; i++) {
        odata[i] = 'A';
    }
    objectstore::Object object(i, odata, msg_size + 1);

    // trial run to get an approximate number of objects to reach runtime
    clock_gettime(CLOCK_REALTIME, &t_start);
    if (use_aio) {

   	 for(int i = 0; i < num_msg; i++) {
        	oss.aio_put(object);
    	}
    } else {
	 for(int i = 0; i < num_msg; i++) {
                oss.bio_put(object);
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
    while(true) {
    }
}
