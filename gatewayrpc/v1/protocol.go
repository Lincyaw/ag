package gatewayv1

// ProtocolVersion fences managed daemons across incompatible ag upgrades.
// v4 adds the durable tool-capability catalog to trajectory settings. The
// settings payload is decoded with unknown fields rejected, so an older v3
// daemon cannot safely serve a v4 frontend and must be replaced by the managed
// gateway lifecycle.
const ProtocolVersion = "gateway.v4"
