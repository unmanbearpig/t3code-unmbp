#ifndef T3_QUIC_H
#define T3_QUIC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*T3QuicCallback)(uint64_t context, uint64_t operation,
                               int32_t status, uint64_t handle,
                               uint8_t *data, size_t length);

uint64_t t3_quic_owner_create(void);

// Input bytes are copied before submit returns. Zero accepts the operation and
// delivers exactly one callback; a negative return does not deliver a callback.
int32_t t3_quic_submit(uint64_t owner, uint64_t operation, uint32_t kind,
                       uint64_t handle, const uint8_t *data, size_t length,
                       uint32_t argument, T3QuicCallback callback,
                       uint64_t context);

void t3_quic_cancel(uint64_t owner, uint64_t operation);
void t3_quic_close(uint64_t owner, uint32_t code);
void t3_quic_reset(uint64_t owner, uint64_t stream, uint32_t code);
void t3_quic_stop(uint64_t owner, uint64_t stream, uint32_t code);

// Callback buffers belong to Rust, including callbacks after owner closure.
void t3_quic_buffer_free(uint8_t *data, size_t length);

#ifdef __cplusplus
}
#endif

#endif
