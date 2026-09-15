/* One producer / one consumer queue for compressed video packets. No disk I/O
 * or hardware buffers cross this boundary. Limits include the in-flight write. */
#ifndef YLX_PACKET_QUEUE_H
#define YLX_PACKET_QUEUE_H
#include <pthread.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

typedef struct ylx_packet {
    struct ylx_packet *next;
    size_t size;
    int key_frame;
    unsigned char data[];
} ylx_packet_t;

typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t ready;
    ylx_packet_t *head, *tail;
    size_t bytes, frames, max_bytes, max_frames, peak_bytes, peak_frames, rejected;
    int closed;
} ylx_packet_queue_t;

static void ylx_packet_queue_init(ylx_packet_queue_t *queue, size_t bytes, size_t frames)
{
    memset(queue, 0, sizeof(*queue));
    pthread_mutex_init(&queue->lock, NULL);
    pthread_cond_init(&queue->ready, NULL);
    queue->max_bytes = bytes;
    queue->max_frames = frames;
}

static int ylx_packet_queue_push(ylx_packet_queue_t *queue, const void *data,
                                 size_t size, int key_frame)
{
    pthread_mutex_lock(&queue->lock);
    if (queue->closed || size == 0 || size > queue->max_bytes - queue->bytes ||
        queue->frames >= queue->max_frames) {
        queue->rejected++;
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }
    ylx_packet_t *packet = malloc(sizeof(*packet) + size);
    if (packet == NULL) {
        queue->rejected++;
        pthread_mutex_unlock(&queue->lock);
        return -1;
    }
    packet->next = NULL;
    packet->size = size;
    packet->key_frame = key_frame;
    memcpy(packet->data, data, size);
    if (queue->tail) queue->tail->next = packet;
    else queue->head = packet;
    queue->tail = packet;
    queue->bytes += size;
    queue->frames++;
    if (queue->bytes > queue->peak_bytes) queue->peak_bytes = queue->bytes;
    if (queue->frames > queue->peak_frames) queue->peak_frames = queue->frames;
    pthread_cond_signal(&queue->ready);
    pthread_mutex_unlock(&queue->lock);
    return 0;
}

static ylx_packet_t *ylx_packet_queue_pop(ylx_packet_queue_t *queue)
{
    pthread_mutex_lock(&queue->lock);
    while (!queue->head && !queue->closed) pthread_cond_wait(&queue->ready, &queue->lock);
    ylx_packet_t *packet = queue->head;
    if (packet) {
        queue->head = packet->next;
        if (!queue->head) queue->tail = NULL;
    }
    pthread_mutex_unlock(&queue->lock);
    return packet;
}

static void ylx_packet_queue_done(ylx_packet_queue_t *queue, ylx_packet_t *packet)
{
    pthread_mutex_lock(&queue->lock);
    queue->bytes -= packet->size;
    queue->frames--;
    pthread_mutex_unlock(&queue->lock);
    free(packet);
}

static void ylx_packet_queue_close(ylx_packet_queue_t *queue)
{
    pthread_mutex_lock(&queue->lock);
    queue->closed = 1;
    pthread_cond_broadcast(&queue->ready);
    pthread_mutex_unlock(&queue->lock);
}

static void ylx_packet_queue_destroy(ylx_packet_queue_t *queue)
{
    ylx_packet_queue_close(queue);
    ylx_packet_t *packet;
    while ((packet = ylx_packet_queue_pop(queue))) ylx_packet_queue_done(queue, packet);
    pthread_mutex_destroy(&queue->lock);
    pthread_cond_destroy(&queue->ready);
}
#endif
