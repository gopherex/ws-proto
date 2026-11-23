#include "service.ws.pb.h"
#include "well_known.ws.pb.h"

#include <concepts>
#include <utility>

using Service = naming::api::KeywordServiceClient;
using Request = naming::v1::Outer::Type;
using Response = naming::v1::Outer::WhenMessage;
static_assert(std::same_as<decltype(std::declval<Service>().call_Class(std::declval<Request>())),
                           ws_proto::Rpc<Request, Response, false, false>>);
static_assert(std::same_as<decltype(std::declval<wiretest::WellKnownServiceClient>().call_Empty({})),
                           ws_proto::Rpc<google::protobuf::Empty, google::protobuf::Empty, false, false>>);
int main() {}
