//
// Created by Lukas on 09.03.2018.
//

#include <pistache/http.h>
#include <pistache/router.h>
#include <pistache/endpoint.h>
#include "store/KVStore.h"

using namespace std;
using namespace Pistache;

#ifndef CELONIS_KV_SERVER_ENDPOINT_H
#define CELONIS_KV_SERVER_ENDPOINT_H


class Endpoint {

public:

    shared_ptr<Http::Endpoint> http_endpoint;
    Rest::Router router;
    KVStore store;

    Endpoint(Address addr)
            : http_endpoint(make_shared<Http::Endpoint>(addr))
    { }

    void init();
    void start();
    void shutdown();


private:
    void setup_routes();

    void handle_put(const Rest::Request& request, Http::ResponseWriter response);

    void handle_get(const Rest::Request& request, Http::ResponseWriter response);

    void handle_delete(const Rest::Request& request, Http::ResponseWriter response);
};


#endif //CELONIS_KV_SERVER_ENDPOINT_H
