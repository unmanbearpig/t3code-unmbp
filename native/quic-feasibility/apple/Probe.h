#include <stdint.h>

// Runs on the caller's thread. Keep both strings alive until this function returns.
int32_t t3_apple_webtransport_probe(const char *url, const char *certificate_sha256);
