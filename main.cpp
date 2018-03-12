#include <csignal>
#include "Endpoint.h"


int main() {

    Port port(9080);
    Address address(Ipv4::any(), port);
    Endpoint endpoint(address);

    endpoint.init();
    endpoint.start();

}
