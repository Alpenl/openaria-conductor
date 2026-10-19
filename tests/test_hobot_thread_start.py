"""Exercise actual pipeline lifetime code with pthread failures and fake codecs.

The product structs, queues and open/finish/close functions are compiled intact.
SDK calls are stubs; workers keep the real wait/close dependencies but use no AV.
"""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class HobotThreadStartTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        compiler = shutil.which("cc")
        if not compiler:
            raise unittest.SkipTest("C compiler unavailable")
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        directory = Path(cls.temporary.name)
        root = Path(__file__).resolve().parents[1] / "src/rp_ylx/hobot"
        source = (root / "ylx_stereo_pipeline.c").read_text()
        structures = source[
            source.index("typedef struct segment {") : source.index(
                "static unsigned long long monotonic_time_ns(void);"
            )
        ]
        closer = source[
            source.index("static void *closer_thread_main(") : source.index(
                "/* -------------------------------------------------------------- threads"
            )
        ]
        lifetime = source[
            source.index("int ylx_pipeline_open(") : source.index("int ylx_pipeline_submit(")
        ] + source[
            source.index("int ylx_pipeline_finish(") : source.index("void ylx_pipeline_stats(")
        ]
        harness = r"""
#define _GNU_SOURCE
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "ylx_stereo_pipeline.h"
static int fail_thread, create_calls, join_calls, live_mutexes, live_conditions;
static int fail_init, init_calls;
static pthread_mutex_t *mutexes[8];
static pthread_cond_t *conditions[8];
static int fail_codec, codec_calls, codecs_started, codecs_stopped, next_encoder;
static atomic_int live_workers;
static pthread_t created[6];
static int checked_create(pthread_t *thread,const pthread_attr_t *attr,
                          void *(*function)(void *),void *argument) {
    int number=++create_calls;
    if(number==fail_thread)return EAGAIN;
    int result=pthread_create(thread,attr,function,argument);
    assert(result==0);created[number-1]=*thread;return result;
}
static int checked_join(pthread_t thread,void **result) {
    int known=0;
    for(int i=0;i<create_calls;i++)if(i+1!=fail_thread && pthread_equal(thread,created[i]))known++;
    assert(known==1);join_calls++;return pthread_join(thread,result);
}
static int checked_mutex_init(pthread_mutex_t *lock,const pthread_mutexattr_t *attr) {
    if(++init_calls==fail_init)return EAGAIN;
    int result=pthread_mutex_init(lock,attr);assert(result==0);
    for(int i=0;i<8;i++)if(!mutexes[i]){mutexes[i]=lock;live_mutexes++;return result;}
    abort();
}
static int mutex_slot(pthread_mutex_t *lock) {
    for(int i=0;i<8;i++)if(mutexes[i]==lock)return i;
    assert(!"using uninitialized mutex");abort();
}
static int cond_slot(pthread_cond_t *condition) {
    for(int i=0;i<8;i++)if(conditions[i]==condition)return i;
    assert(!"using uninitialized condition");abort();
}
static int checked_mutex_destroy(pthread_mutex_t *lock) {
    int slot=mutex_slot(lock);
    int result=pthread_mutex_destroy(lock);assert(result==0);mutexes[slot]=NULL;live_mutexes--;return result;
}
static int checked_cond_init(pthread_cond_t *condition,const pthread_condattr_t *attr) {
    if(++init_calls==fail_init)return EAGAIN;
    int result=pthread_cond_init(condition,attr);assert(result==0);
    for(int i=0;i<8;i++)if(!conditions[i]){conditions[i]=condition;live_conditions++;return result;}
    abort();
}
static int checked_cond_destroy(pthread_cond_t *condition) {
    int slot=cond_slot(condition);
    int result=pthread_cond_destroy(condition);assert(result==0);conditions[slot]=NULL;live_conditions--;return result;
}
static int checked_mutex_lock(pthread_mutex_t *lock) {(void)mutex_slot(lock);return pthread_mutex_lock(lock);}
static int checked_mutex_unlock(pthread_mutex_t *lock) {(void)mutex_slot(lock);return pthread_mutex_unlock(lock);}
static int checked_cond_signal(pthread_cond_t *condition) {(void)cond_slot(condition);return pthread_cond_signal(condition);}
static int checked_cond_broadcast(pthread_cond_t *condition) {(void)cond_slot(condition);return pthread_cond_broadcast(condition);}
static int checked_cond_wait(pthread_cond_t *condition,pthread_mutex_t *lock) {
    (void)cond_slot(condition);(void)mutex_slot(lock);return pthread_cond_wait(condition,lock);
}
#define pthread_create checked_create
#define pthread_join checked_join
#define pthread_mutex_init checked_mutex_init
#define pthread_mutex_destroy checked_mutex_destroy
#define pthread_cond_init checked_cond_init
#define pthread_cond_destroy checked_cond_destroy
#define pthread_mutex_lock checked_mutex_lock
#define pthread_mutex_unlock checked_mutex_unlock
#define pthread_cond_signal checked_cond_signal
#define pthread_cond_broadcast checked_cond_broadcast
#define pthread_cond_wait checked_cond_wait
#include "ylx_packet_queue.h"
typedef struct {int id;} media_codec_context_t;
typedef struct {int unused;} media_muxer_context_t;
#define ERROR_LEN 256
#define PATH_LEN 512
#define MUX_QUEUE_BYTES (32u*1024u*1024u)
#define MUX_QUEUE_SECONDS 16u
"""
        stubs = r"""
static void configure_decoder(media_codec_context_t *codec,const ylx_pipeline_config_t *config) {
    (void)config;codec->id=0;
}
static int configure_encoder(media_codec_context_t *codec,const ylx_pipeline_config_t *config,int width) {
    (void)config;(void)width;codec->id=++next_encoder;return 0;
}
static int codec_start(media_codec_context_t *codec) {
    (void)codec;if(++codec_calls==fail_codec)return -42;codecs_started++;return 0;
}
static void codec_stop(media_codec_context_t *codec) {(void)codec;codecs_stopped++;}
static unsigned long long monotonic_time_ns(void) {return 1;}
static void observe_max(atomic_ullong *value,unsigned long long elapsed) {(void)value;(void)elapsed;}
static void segment_seal(ylx_pipeline_t *pipeline,segment_t *segment) {(void)pipeline;(void)segment;}
static void *split_thread_main(void *argument) {
    ylx_pipeline_t *pipeline=argument;atomic_fetch_add(&live_workers,1);
    while(!atomic_load(&pipeline->stopping))usleep(50);
    atomic_store(&pipeline->split_done,1);atomic_fetch_sub(&live_workers,1);return NULL;
}
static void *encoder_output_thread(void *argument) {
    eye_t *eye=argument;atomic_fetch_add(&live_workers,1);
    while(!atomic_load(&eye->pipeline->split_done))usleep(50);
    ylx_packet_queue_close(&eye->packets);atomic_fetch_sub(&live_workers,1);return NULL;
}
static void *mux_writer_thread(void *argument) {
    eye_t *eye=argument;atomic_fetch_add(&live_workers,1);
    ylx_packet_t *packet;
    while((packet=ylx_packet_queue_pop(&eye->packets)))ylx_packet_queue_done(&eye->packets,packet);
    atomic_fetch_sub(&live_workers,1);return NULL;
}
"""
        main = r"""
int main(int argc,char **argv) {
    assert(argc==2);int test=atoi(argv[1]);
    if(test>0 && test<7)fail_thread=test;
    if(test<0)fail_codec=-test;
    if(test>=101 && test<=108)fail_init=test-100;
    ylx_pipeline_config_t config={.sbs_width=3840,.height=1080,.fps=30,.bitrate_kbps=12288,
        .hevc=1,.rate_control=0,.intra_period=30,.min_qp=18,.max_qp=32,.intra_qp=20,
        .initial_qp=22,.vbv_ms=3000,.segment_frames=150,.out_dir="unused"};
    if(test==99)config.sbs_width=1;
    ylx_pipeline_t *pipeline=(void *)(uintptr_t)1;char error[256]={0};
    int result=ylx_pipeline_open(&config,NULL,NULL,&pipeline,error,sizeof(error));
    if(test==0) {
        assert(result==0 && pipeline && create_calls==6);
        assert(ylx_pipeline_finish(pipeline)==0);
        assert(ylx_pipeline_finish(pipeline)==0);
        ylx_pipeline_close(pipeline);assert(join_calls==6);
    } else {
        assert(result==-1 && pipeline==NULL);
        if(fail_thread) {
            assert(strstr(error,"thread_start_failed") && create_calls==fail_thread);
            assert(join_calls==fail_thread-1);
        }
        if(fail_codec)assert(strstr(error,"unavailable") && create_calls==0);
        if(fail_init) {
            assert(strstr(error,"sync_init_failed") && init_calls==fail_init);
            assert(create_calls==0 && codec_calls==0);
        }
        if(test==99)assert(strstr(error,"invalid_configuration") && create_calls==0);
    }
    assert(codecs_started==codecs_stopped);
    assert(!atomic_load(&live_workers) && live_mutexes==0 && live_conditions==0);
    ylx_pipeline_close(NULL);
    (void)ylx_packet_queue_push;
    puts("lifetime cleanup passed");return 0;
}
"""
        path = directory / "case.c"
        path.write_text(harness + structures + stubs + closer + lifetime + main)
        cls.binary = directory / "case"
        subprocess.run(
            [compiler, "-std=gnu11", "-Wall", "-Wextra", "-Werror", "-pthread", "-I", str(root),
             str(path), "-o", str(cls.binary)],
            check=True, capture_output=True, text=True,
        )

    def run_case(self, number):
        result = subprocess.run(
            [str(self.binary), str(number)], capture_output=True, text=True, timeout=5
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_each_thread_creation_failure_reclaims_started_workers_and_resources(self):
        for number in range(1, 7):
            with self.subTest(thread=number):
                self.run_case(number)

    def test_successful_start_and_repeated_finish_join_once(self):
        self.run_case(0)

    def test_codec_start_failure_releases_only_started_codecs(self):
        for number in range(1, 4):
            with self.subTest(codec=number):
                self.run_case(-number)

    def test_invalid_configuration_clears_output_without_starting_resources(self):
        self.run_case(99)

    def test_each_synchronization_init_failure_avoids_invalid_use_and_leaks(self):
        for number in range(1, 9):
            with self.subTest(initializer=number):
                self.run_case(100 + number)


if __name__ == "__main__":
    unittest.main()
