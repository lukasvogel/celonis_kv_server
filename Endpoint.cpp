//
// Created by Lukas on 09.03.2018.
//

#include "Endpoint.h"


void Endpoint::init()  {
    auto opts = Http::Endpoint::options();
    opts.threads(4);

    http_endpoint->init(opts);
    setup_routes();
}

void Endpoint::start() {
    http_endpoint->setHandler(router.handler());
    http_endpoint->serve();
}

void Endpoint::shutdown() {
    http_endpoint->shutdown();
}

void Endpoint::setup_routes() {
    using namespace Rest;

    Routes::Get(router, "/kv/:key", Routes::bind(&Endpoint::handle_get, this));
    Routes::Put(router, "/kv/:key", Routes::bind(&Endpoint::handle_put, this));
    Routes::Delete(router, "/kv/:key", Routes::bind(&Endpoint::handle_delete, this));
    Routes::Post(router, "/control/stop", Routes::bind(&Endpoint::handle_stop, this));

}

void Endpoint::handle_put(const Rest::Request &request, Http::ResponseWriter response) {
    string key = request.param(":key").as<std::string>();

    cout << "PUT: " << key  << " -> " << request.body() << endl;

    store.put(key,request.body());
    response.send(Http::Code::Ok);
}

void Endpoint::handle_get(const Rest::Request &request, Http::ResponseWriter response) {
    string key = request.param(":key").as<std::string>();
    cout << "GET: " << key << endl;

    string value;
    if(store.get(key,value)) {
        response.send(Http::Code::Ok, value);
    } else {
        response.send(Http::Code::Not_Found);
    }
}

void Endpoint::handle_delete(const Rest::Request &request, Http::ResponseWriter response) {
    string key = request.param(":key").as<std::string>();
    cout << "DELETE: " << key << endl;
    store.del(key);
    response.send(Http::Code::Ok);
}

void Endpoint::handle_stop(const Rest::Request &request, Http::ResponseWriter response) {
    store.flush();
    cout << "Shutting down..." << endl;

    response.send(Http::Code::Ok);
    shutdown();
    exit(1);
}
