#ifndef OBJECTSTORE_HPP
#define OBJECTSTORE_HPP

#include <optional>

#include "Object.hpp"

namespace objectstore {
// if object is valid, this is a PUT operation; otherwise, a REMOVE operation.
using ObjectWatcher = std::function<void(const OID&,const Object&)>;

// The core API. See `test.cpp` for how to use it.
class IObjectStoreService : public derecho::IDeserializationContext {
private:
    static std::unique_ptr<IObjectStoreService> singleton;
public:
    virtual const bool isReplica() = 0;
    // blocking operations: all operations are guaranteed to be finished before
    // return. Note: the internal implementation of objectstore has two
    // versions, the client version and replica version. Only the nodes in the
    // ObjectStore subgroup can use replica version, and other nodes have to
    // relay the request to a replica node with the client version. On
    // receiving the request, the replica in turn uses the replica version
    // to do the real work. Obviously, the replica version is more efficient
    // because it saves one level of indirection.
    //
    // By default, the api use replica version for replica nodes and client 
    // version for the others. To use the client API uniformly, the user can
    // set the 'force_client' parameter to true.
    // 
    // 1 - blocking put
    // @PARAM object - const reference of the object to be inserted. If
    //        corresponding object id exists, the object is replaced
    // @PARAM force_client - see above
    // @RETURN true/false
    virtual bool bio_put(const Object& object, bool force_client = false) = 0;
    // 2 - blocking remove
    // @PARAM oid - const reference of the object id.
    // @PARAM force_client - see above
    // @RETURN true/false
    virtual bool bio_remove(const OID& oid, bool force_client = false) = 0;
    // 3 - blocking get
    // @PARAM oid - const reference of the object id.
    // @PARAM force_client - see above
    // @RETURN the object of oid, invalid object if corresponding object does not exists.
    virtual Object bio_get(const OID& oid, bool force_client = false) = 0;

    // non blocking operations: the operations will return a future.
    // The arguments align to the blocking apis.
    virtual derecho::rpc::QueryResults<bool> aio_put(const Object& object, bool force_client = false) = 0;
    virtual derecho::rpc::QueryResults<bool> aio_remove(const OID& oid, bool force_client = false) = 0;
    virtual derecho::rpc::QueryResults<const Object> aio_get(const OID& oid, bool force_client = false) = 0;

    virtual void leave() = 0; // leave gracefully
    virtual const ObjectWatcher& getObjectWatcher() = 0;

    // get singleton
    static IObjectStoreService& getObjectStoreService(int argc, char ** argv, const ObjectWatcher& ow = {});
};

}  // namespace objectstore
#endif//OBJECTSTORE_HPP
