/*
 * One producer / one consumer queue for compressed video packets.
 * No disk I/O or hardware buffers cross this boundary.
 * Limits include the in-flight write held by the consumer until _done().
 */

#ifndef MIRO_EGO_PACKET_QUEUE_H
#define MIRO_EGO_PACKET_QUEUE_H

#include <pthread.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

typedef struct miro_ego_packet {
    struct miro_ego_packet *next;
    size_t size;
    int key_frame;
    unsigned char data[];
} miro_ego_packet_t;

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t ready;
    miro_ego_packet_t *head;
    miro_ego_packet_t *tail;
    size_t bytes;
    size_t frames;
    size_t max_bytes;
    size_t max_frames;
    size_t peak_bytes;
    size_t peak_frames;
    size_t rejected;
    int closed;
} miro_ego_packet_queue_t;

/* A failed initializer owns no resources; callers must not use/destroy it. */
static inline int miro_ego_packet_queue_init(miro_ego_packet_queue_t *queue,
                                             size_t bytes, size_t frames)
{
    memset(queue, 0, sizeof(*queue));
    int ret = pthread_mutex_init(&queue->lock, NULL);
    if (ret != 0) {
        return ret;
    }
    ret = pthread_cond_init(&queue->ready, NULL);
    if (ret != 0) {
        pthread_mutex_destroy(&queue->lock);
        return ret;
    }
    queue->max_bytes = bytes;
    queue->max_frames = frames;
    return 0;
}

static inline int miro_ego_packet_queue_push(miro_ego_packet_queue_t *queue,
                                             const void *data,
                                             size_t size, int key_frame)
{
    if (size == 0 || data == NULL || size > queue->max_bytes ||
        size > (size_t)-1 - sizeof(miro_ego_packet_t)) {
        pthread_mutex_lock(&queue->lock);
        queue->rejected += 1;
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }
    miro_ego_packet_t *packet = malloc(sizeof(*packet) + size);
    if (packet == NULL) {
        pthread_mutex_lock(&queue->lock);
        queue->rejected += 1;
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }
    packet->next = NULL;
    packet->size = size;
    packet->key_frame = key_frame;
    memcpy(packet->data, data, size);

    pthread_mutex_lock(&queue->lock);
    if (queue->closed || size > queue->max_bytes - queue->bytes ||
        queue->frames >= queue->max_frames) {
        queue->rejected += 1;
        pthread_mutex_unlock(&queue->lock);
        free(packet);
        return -1;
    }
    if (queue->tail != NULL) {
        queue->tail->next = packet;
    } else {
        queue->head = packet;
    }
    queue->tail = packet;
    queue->bytes += size;
    queue->frames += 1;
    if (queue->bytes > queue->peak_bytes) {
        queue->peak_bytes = queue->bytes;
    }
    if (queue->frames > queue->peak_frames) {
        queue->peak_frames = queue->frames;
    }
    pthread_cond_signal(&queue->ready);
    pthread_mutex_unlock(&queue->lock);
    return 0;
}

static inline miro_ego_packet_t *miro_ego_packet_queue_pop(miro_ego_packet_queue_t *queue)
{
    pthread_mutex_lock(&queue->lock);
    while (queue->head == NULL && !queue->closed) {
        pthread_cond_wait(&queue->ready, &queue->lock);
    }
    miro_ego_packet_t *packet = queue->head;
    if (packet != NULL) {
        queue->head = packet->next;
        if (queue->head == NULL) {
            queue->tail = NULL;
        }
    }
    pthread_mutex_unlock(&queue->lock);
    return packet;
}

static inline void miro_ego_packet_queue_done(miro_ego_packet_queue_t *queue,
                                              miro_ego_packet_t *packet)
{
    pthread_mutex_lock(&queue->lock);
    queue->bytes -= packet->size;
    queue->frames -= 1;
    pthread_mutex_unlock(&queue->lock);
    free(packet);
}

static inline void miro_ego_packet_queue_close(miro_ego_packet_queue_t *queue)
{
    pthread_mutex_lock(&queue->lock);
    queue->closed = 1;
    pthread_cond_broadcast(&queue->ready);
    pthread_mutex_unlock(&queue->lock);
}

static inline void miro_ego_packet_queue_destroy(miro_ego_packet_queue_t *queue)
{
    miro_ego_packet_queue_close(queue);
    miro_ego_packet_t *packet;
    while ((packet = miro_ego_packet_queue_pop(queue)) != NULL) {
        miro_ego_packet_queue_done(queue, packet);
    }
    pthread_mutex_destroy(&queue->lock);
    pthread_cond_destroy(&queue->ready);
}

#endif /* MIRO_EGO_PACKET_QUEUE_H */
