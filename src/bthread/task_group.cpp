// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// bthread - A M:N threading library to make applications more concurrent.

// Date: Tue Jul 10 17:40:58 CST 2012

#include <sys/types.h>
#include <stddef.h>                         // size_t
#include <gflags/gflags.h>
#include "butil/compat.h"                   // OS_MACOSX
#include "butil/macros.h"                   // ARRAY_SIZE
#include "butil/scoped_lock.h"              // BAIDU_SCOPED_LOCK
#include "butil/fast_rand.h"
#include "butil/unique_ptr.h"
#include "butil/third_party/murmurhash3/murmurhash3.h" // fmix64
#include "bthread/errno.h"                  // ESTOP
#include "bthread/butex.h"                  // butex_*
#include "bthread/sys_futex.h"              // futex_wake_private
#include "bthread/processor.h"              // cpu_relax
#include "bthread/task_control.h"
#include "bthread/task_group.h"
#include "bthread/timer_thread.h"
#include "bthread/errno.h"

namespace bthread {

static const bthread_attr_t BTHREAD_ATTR_TASKGROUP = {
    BTHREAD_STACKTYPE_UNKNOWN, 0, NULL };

static bool pass_bool(const char*, bool) { return true; }

DEFINE_bool(show_bthread_creation_in_vars, false, "When this flags is on, The time "
            "from bthread creation to first run will be recorded and shown "
            "in /vars");
const bool ALLOW_UNUSED dummy_show_bthread_creation_in_vars =
    ::GFLAGS_NS::RegisterFlagValidator(&FLAGS_show_bthread_creation_in_vars,
                                    pass_bool);

DEFINE_bool(show_per_worker_usage_in_vars, false,
            "Show per-worker usage in /vars/bthread_per_worker_usage_<tid>");
const bool ALLOW_UNUSED dummy_show_per_worker_usage_in_vars =
    ::GFLAGS_NS::RegisterFlagValidator(&FLAGS_show_per_worker_usage_in_vars,
                                    pass_bool);

__thread TaskGroup* tls_task_group = NULL; // 线程内全局变量
// Sync with TaskMeta::local_storage when a bthread is created or destroyed.
// During running, the two fields may be inconsistent, use tls_bls as the
// groundtruth.
thread_local LocalStorage tls_bls = BTHREAD_LOCAL_STORAGE_INITIALIZER;

// defined in bthread/key.cpp
extern void return_keytable(bthread_keytable_pool_t*, KeyTable*);

// [Hacky] This is a special TLS set by bthread-rpc privately... to save
// overhead of creation keytable, may be removed later.
BAIDU_THREAD_LOCAL void* tls_unique_user_ptr = NULL;

const TaskStatistics EMPTY_STAT = { 0, 0 };

const size_t OFFSET_TABLE[] = {
#include "bthread/offset_inl.list"
};

int TaskGroup::get_attr(bthread_t tid, bthread_attr_t* out) {
    TaskMeta* const m = address_meta(tid);
    if (m != NULL) {
        const uint32_t given_ver = get_version(tid);
        BAIDU_SCOPED_LOCK(m->version_lock);
        if (given_ver == *m->version_butex) {
            *out = m->attr;
            return 0;
        }
    }
    errno = EINVAL;
    return -1;
}

void TaskGroup::set_stopped(bthread_t tid) {
    TaskMeta* const m = address_meta(tid);
    if (m != NULL) {
        const uint32_t given_ver = get_version(tid);
        BAIDU_SCOPED_LOCK(m->version_lock);
        if (given_ver == *m->version_butex) {
            m->stop = true;
        }
    }
}

bool TaskGroup::is_stopped(bthread_t tid) {
    TaskMeta* const m = address_meta(tid);
    if (m != NULL) {
        const uint32_t given_ver = get_version(tid);
        BAIDU_SCOPED_LOCK(m->version_lock);
        if (given_ver == *m->version_butex) {
            return m->stop;
        }
    }
    // If the tid does not exist or version does not match, it's intuitive
    // to treat the thread as "stopped".
    return true;
}

bool TaskGroup::wait_task(bthread_t* tid) {
    do {
#ifndef BTHREAD_DONT_SAVE_PARKING_STATE
        if (_last_pl_state.stopped()) { // pl已经停了
            return false;
        }
        // 如果和先前的state相同，说明pl.state没有变化，说明pl中的_pending_signal没有发生变化
        // 说明没有新的任务加入(即没有人调用pl.signal),没有任务那还偷啥子嘞，那就wait等人调用pl.signal后被唤醒
        _pl->wait(_last_pl_state);
        if (steal_task(tid)) { // 从remote_rq取task或去其他tg偷task, 并保存pl的state
            return true;
        }
#else
        const ParkingLot::State st = _pl->get_state(); // pl当前的状态
        if (st.stopped()) { // pl已经停了
            return false;
        }
        if (steal_task(tid)) {
            return true;
        }
        _pl->wait(st); // 如果上面偷失败了，就在这里等新加入的任务唤醒
#endif
    } while (true);
}

static double get_cumulated_cputime_from_this(void* arg) {
    return static_cast<TaskGroup*>(arg)->cumulated_cputime_ns() / 1000000000.0;
}

// https://sf-zhou.github.io/brpc/brpc_04_schedule.html
void TaskGroup::run_main_task() {
    bvar::PassiveStatus<double> cumulated_cputime(
        get_cumulated_cputime_from_this, this);
    std::unique_ptr<bvar::PerSecond<bvar::PassiveStatus<double> > > usage_bvar;

    TaskGroup* dummy = this;
    bthread_t tid;
    // 消费自己的remote_rq -> 让tc去偷别人的rq -> 让tc去偷别人的remote_rq -> 消费完自己的rq (前三步都是wait_task，最后一步在task_runner里面)
    // wait_task: 拿任务(pop remote_rq或去其他tg偷)
    // sched_to: 切换任务
    // task_runner:: 跑任务(把当前tg的rq跑完)
    while (wait_task(&tid)) { // tg在这里一直取remote_rq的task，不行就去其他tg偷,偷不到就睡眠&死循环(除非被停下来),也就是说这个while在程序停之前一直是true
        TaskGroup::sched_to(&dummy, tid); // 用新task_meta(tid)替换调tg中的旧task，并执行这个新task
        // sched_to中有jump_stack，stack永远是从task_runner开始的，task_runner中有while循环会执行完tg中的所有tm
        DCHECK_EQ(this, dummy); // sched_to是可能会堵塞的，当它执行完并恢复到当前执行语句时，可能已经不是再调用sched_to的那个线程了,sched_to函数的最后一句就是把最后正在使用的tg保存的dummy，而这里的this也就是也变成了另一个tg（也就是dummy)
        DCHECK_EQ(_cur_meta->stack, _main_stack);
        if (_cur_meta->tid != _main_tid) {
            TaskGroup::task_runner(1/*skip remained*/); // 执行这个tg中的task_meta
        }
        if (FLAGS_show_per_worker_usage_in_vars && !usage_bvar) {
            char name[32];
#if defined(OS_MACOSX)
            snprintf(name, sizeof(name), "bthread_worker_usage_%" PRIu64,
                     pthread_numeric_id());
#else
            snprintf(name, sizeof(name), "bthread_worker_usage_%ld",
                     (long)syscall(SYS_gettid));
#endif
            usage_bvar.reset(new bvar::PerSecond<bvar::PassiveStatus<double> >
                             (name, &cumulated_cputime, 1));
        }
    }
    // Don't forget to add elapse of last wait_task. (不要忘记记录最后一个task的执行时间)
    // 上面while循环退出前的那个任务的累计执行时间, while循环里面的任务累计执行时间在sched_to里切换任务时有记录
    current_task()->stat.cputime_ns += butil::cpuwide_time_ns() - _last_run_ns;
}

TaskGroup::TaskGroup(TaskControl* c)
    :
#ifndef NDEBUG
    _sched_recursive_guard(0),
#endif
    _cur_meta(NULL)
    , _control(c)
    , _num_nosignal(0)
    , _nsignaled(0)
    , _last_run_ns(butil::cpuwide_time_ns())
    , _cumulated_cputime_ns(0)
    , _nswitch(0)
    , _last_context_remained(NULL)
    , _last_context_remained_arg(NULL)
    , _pl(NULL)
    , _main_stack(NULL)
    , _main_tid(0)
    , _remote_num_nosignal(0)
    , _remote_nsignaled(0)
{
    _steal_seed = butil::fast_rand();
    _steal_offset = OFFSET_TABLE[_steal_seed % ARRAY_SIZE(OFFSET_TABLE)];
    _pl = &c->_pl[butil::fmix64(pthread_numeric_id()) % TaskControl::PARKING_LOT_NUM]; // 从tc里面随机一个pl
    CHECK(c);
}

TaskGroup::~TaskGroup() {
    if (_main_tid) {
        TaskMeta* m = address_meta(_main_tid);
        CHECK(_main_stack == m->stack);
        return_stack(m->release_stack());
        return_resource(get_slot(_main_tid));
        _main_tid = 0;
    }
}

int TaskGroup::init(size_t runqueue_capacity) {
    if (_rq.init(runqueue_capacity) != 0) {
        LOG(FATAL) << "Fail to init _rq";
        return -1;
    }
    if (_remote_rq.init(runqueue_capacity / 2) != 0) {
        LOG(FATAL) << "Fail to init _remote_rq";
        return -1;
    }
    ContextualStack* stk = get_stack(STACK_TYPE_MAIN, NULL);
    if (NULL == stk) {
        LOG(FATAL) << "Fail to get main stack container";
        return -1;
    }
    butil::ResourceId<TaskMeta> slot;
    TaskMeta* m = butil::get_resource<TaskMeta>(&slot);
    if (NULL == m) {
        LOG(FATAL) << "Fail to get TaskMeta";
        return -1;
    }
    m->stop = false;
    m->interrupted = false;
    m->about_to_quit = false;
    m->fn = NULL;
    m->arg = NULL;
    m->local_storage = LOCAL_STORAGE_INIT;
    m->cpuwide_start_ns = butil::cpuwide_time_ns();
    m->stat = EMPTY_STAT;
    m->attr = BTHREAD_ATTR_TASKGROUP;
    m->tid = make_tid(*m->version_butex, slot);
    m->set_stack(stk);

    _cur_meta = m;
    _main_tid = m->tid;
    _main_stack = stk;
    _last_run_ns = butil::cpuwide_time_ns();
    return 0;
}

void TaskGroup::task_runner(intptr_t skip_remained) {
    // NOTE: tls_task_group is volatile since tasks are moved around
    //       different groups.
    TaskGroup* g = tls_task_group;

    if (!skip_remained) { // 如果不跳过remained就执行remained
        while (g->_last_context_remained) {
            RemainedFn fn = g->_last_context_remained;
            g->_last_context_remained = NULL;
            fn(g->_last_context_remained_arg);
            g = tls_task_group;
        }

#ifndef NDEBUG
        --g->_sched_recursive_guard;
#endif
    }

    do {
        // A task can be stopped before it gets running, in which case
        // we may skip user function, but that may confuse user:
        // Most tasks have variables to remember running result of the task,
        // which is often initialized to values indicating success. If an
        // user function is never called, the variables will be unchanged
        // however they'd better reflect failures because the task is stopped
        // abnormally.

        // Meta and identifier of the task is persistent in this run.
        TaskMeta* const m = g->_cur_meta;

        if (FLAGS_show_bthread_creation_in_vars) {
            // NOTE: the thread triggering exposure of pending time may spend
            // considerable time because a single bvar::LatencyRecorder
            // contains many bvar.
            g->_control->exposed_pending_time() <<
                (butil::cpuwide_time_ns() - m->cpuwide_start_ns) / 1000L;
        }

        // Not catch exceptions except ExitException which is for implementing
        // bthread_exit(). User code is intended to crash when an exception is
        // not caught explicitly. This is consistent with other threading
        // libraries.
        void* thread_return;
        try {
            thread_return = m->fn(m->arg); // 执行tm中的函数
        } catch (ExitException& e) {
            thread_return = e.value();
        }

        // Group is probably changed
        // 上面执行m->fn后，这个fn函数可能会挂住，比如函数调用了bthread_usleep，那么这个tm会被重新放到tg的队列中等待执行
        // 等到被重新执行完后就会执行这一句, 被重新执行的时候可能是被其他tg偷过去执行的，所以需要重新给g赋值,相当于调用task_runner的参数tg
        // 和执行的这里的tg可能不是同一个了
        // 和
        g = tls_task_group;

        // TODO: Save thread_return
        (void)thread_return;

        // Logging must be done before returning the keytable, since the logging lib
        // use bthread local storage internally, or will cause memory leak.
        // FIXME: the time from quiting fn to here is not counted into cputime
        if (m->attr.flags & BTHREAD_LOG_START_AND_FINISH) {
            LOG(INFO) << "Finished bthread " << m->tid << ", cputime="
                      << m->stat.cputime_ns / 1000000.0 << "ms";
        }

        // Clean tls variables, must be done before changing version_butex
        // otherwise another thread just joined this thread may not see side
        // effects of destructing tls variables.
        KeyTable* kt = tls_bls.keytable; // TODO(xcy): 没看懂，再见
        if (kt != NULL) {
            return_keytable(m->attr.keytable_pool, kt);
            // After deletion: tls may be set during deletion.
            tls_bls.keytable = NULL;
            m->local_storage.keytable = NULL; // optional
        }

        // Increase the version and wake up all joiners, if resulting version
        // is 0, change it to 1 to make bthread_t never be 0. Any access
        // or join to the bthread after changing version will be rejected.
        // The spinlock is for visibility of TaskGroup::get_attr.
        {
            BAIDU_SCOPED_LOCK(m->version_lock);
            if (0 == ++*m->version_butex) {
                ++*m->version_butex;
            }
        }
        butex_wake_except(m->version_butex, 0);

        g->_control->_nbthreads << -1; // 任务执行结束，tg里面任务减1
        g->set_remained(TaskGroup::_release_last_context, m); // 任务执行完了，清理任务的栈. TODO(xcy): 为什么不直接执行而是放到remained里面
        ending_sched(&g); // 在tg的rq里找下一个task

    } while (g->_cur_meta->tid != g->_main_tid); // 上面的ending_sched会一直寻找并执行下一个任务直到没有任务后回到main_task

    // Was called from a pthread and we don't have BTHREAD_STACKTYPE_PTHREAD
    // tasks to run, quit for more tasks.
}

void TaskGroup::_release_last_context(void* arg) {
    TaskMeta* m = static_cast<TaskMeta*>(arg);
    if (m->stack_type() != STACK_TYPE_PTHREAD) {
        return_stack(m->release_stack()/*may be NULL*/);
    } else {
        // it's _main_stack, don't return.
        m->set_stack(NULL);
    }
    return_resource(get_slot(m->tid));
}

int TaskGroup::start_foreground(TaskGroup** pg,
                                bthread_t* __restrict th,
                                const bthread_attr_t* __restrict attr,
                                void * (*fn)(void*),
                                void* __restrict arg) {
    if (__builtin_expect(!fn, 0)) {
        return EINVAL;
    }
    const int64_t start_ns = butil::cpuwide_time_ns();
    const bthread_attr_t using_attr = (attr ? *attr : BTHREAD_ATTR_NORMAL);
    butil::ResourceId<TaskMeta> slot;
    TaskMeta* m = butil::get_resource(&slot); // 新建tm
    if (__builtin_expect(!m, 0)) {
        return ENOMEM;
    }
    CHECK(m->current_waiter.load(butil::memory_order_relaxed) == NULL);
    m->stop = false;
    m->interrupted = false;
    m->about_to_quit = false;
    m->fn = fn;
    m->arg = arg;
    CHECK(m->stack == NULL);
    m->attr = using_attr;
    m->local_storage = LOCAL_STORAGE_INIT;
    m->cpuwide_start_ns = start_ns; // tm开始时间
    m->stat = EMPTY_STAT;
    m->tid = make_tid(*m->version_butex, slot); // TODO(xcy): 看看version_butex标识啥子
    *th = m->tid;
    if (using_attr.flags & BTHREAD_LOG_START_AND_FINISH) {
        LOG(INFO) << "Started bthread " << m->tid;
    }

    TaskGroup* g = *pg;
    g->_control->_nbthreads << 1; // 任务数+1
    if (g->is_current_pthread_task()) { // 如果当前正在main_task，那就直接准备执行tm(就是把tm放到rq里面)
        // never create foreground task in pthread.
        g->ready_to_run(m->tid, (using_attr.flags & BTHREAD_NOSIGNAL));
    } else {
        // NOSIGNAL affects current task, not the new task.
        RemainedFn fn = NULL;
        if (g->current_task()->about_to_quit) { // TODO(xcy): bthread_about_to_quit，在rpc代码下有调用
            fn = ready_to_run_in_worker_ignoresignal;
        } else {
            fn = ready_to_run_in_worker;
        }
        ReadyToRunArgs args = {
            g->current_tid(),
            (bool)(using_attr.flags & BTHREAD_NOSIGNAL)
        };
        g->set_remained(fn, &args); // 当前任务靠边站,放到remained里面
        TaskGroup::sched_to(pg, m->tid); // 执行这个任务，因为要start_foreground
    }
    return 0;
}

template <bool REMOTE>
int TaskGroup::start_background(bthread_t* __restrict th,
                                const bthread_attr_t* __restrict attr,
                                void * (*fn)(void*),
                                void* __restrict arg) {
    if (__builtin_expect(!fn, 0)) {
        return EINVAL;
    }
    const int64_t start_ns = butil::cpuwide_time_ns();
    const bthread_attr_t using_attr = (attr ? *attr : BTHREAD_ATTR_NORMAL);
    butil::ResourceId<TaskMeta> slot;
    TaskMeta* m = butil::get_resource(&slot);
    if (__builtin_expect(!m, 0)) {
        return ENOMEM;
    }
    CHECK(m->current_waiter.load(butil::memory_order_relaxed) == NULL);
    m->stop = false;
    m->interrupted = false;
    m->about_to_quit = false;
    m->fn = fn;
    m->arg = arg;
    CHECK(m->stack == NULL); // 新建的tm是没有stack的
    m->attr = using_attr;
    m->local_storage = LOCAL_STORAGE_INIT;
    m->cpuwide_start_ns = start_ns;
    m->stat = EMPTY_STAT;
    m->tid = make_tid(*m->version_butex, slot);
    *th = m->tid;
    if (using_attr.flags & BTHREAD_LOG_START_AND_FINISH) {
        LOG(INFO) << "Started bthread " << m->tid;
    }
    _control->_nbthreads << 1;
    if (REMOTE) {
        ready_to_run_remote(m->tid, (using_attr.flags & BTHREAD_NOSIGNAL));
    } else {
        ready_to_run(m->tid, (using_attr.flags & BTHREAD_NOSIGNAL));
    }
    return 0;
}

// Explicit instantiations.
template int
TaskGroup::start_background<true>(bthread_t* __restrict th,
                                  const bthread_attr_t* __restrict attr,
                                  void * (*fn)(void*),
                                  void* __restrict arg);
template int
TaskGroup::start_background<false>(bthread_t* __restrict th,
                                   const bthread_attr_t* __restrict attr,
                                   void * (*fn)(void*),
                                   void* __restrict arg);

int TaskGroup::join(bthread_t tid, void** return_value) {
    if (__builtin_expect(!tid, 0)) {  // tid of bthread is never 0.
        return EINVAL;
    }
    TaskMeta* m = address_meta(tid);
    if (__builtin_expect(!m, 0)) {
        // The bthread is not created yet, this join is definitely wrong.
        return EINVAL;
    }
    TaskGroup* g = tls_task_group;
    if (g != NULL && g->current_tid() == tid) { // 自己不能join自己
        // joining self causes indefinite waiting.
        return EINVAL;
    }
    const uint32_t expected_version = get_version(tid);
    while (*m->version_butex == expected_version) { // TODO(xuechengyun)
        if (butex_wait(m->version_butex, expected_version, NULL) < 0 &&
            errno != EWOULDBLOCK && errno != EINTR) {
            return errno;
        }
    }
    if (return_value) {
        *return_value = NULL;
    }
    return 0;
}

bool TaskGroup::exists(bthread_t tid) {
    if (tid != 0) {  // tid of bthread is never 0.
        TaskMeta* m = address_meta(tid);
        if (m != NULL) {
            return (*m->version_butex == get_version(tid));
        }
    }
    return false;
}

TaskStatistics TaskGroup::main_stat() const {
    TaskMeta* m = address_meta(_main_tid);
    return m ? m->stat : EMPTY_STAT;
}

void TaskGroup::ending_sched(TaskGroup** pg) {
    TaskGroup* g = *pg;
    bthread_t next_tid = 0;
    // Find next task to run, if none, switch to idle thread of the group.
#ifndef BTHREAD_FAIR_WSQ
    // When BTHREAD_FAIR_WSQ is defined, profiling shows that cpu cost of
    // WSQ::steal() in example/multi_threaded_echo_c++ changes from 1.9%
    // to 2.9%
    const bool popped = g->_rq.pop(&next_tid);
#else
    const bool popped = g->_rq.steal(&next_tid);
#endif
    if (!popped && !g->steal_task(&next_tid)) {
        // Jump to main task if there's no task to run.
        next_tid = g->_main_tid; // 当走到这里的时候，说明没有tm需要执行了, 回到main_tid
    }

    TaskMeta* const cur_meta = g->_cur_meta;
    TaskMeta* next_meta = address_meta(next_tid);
    if (next_meta->stack == NULL) {
        if (next_meta->stack_type() == cur_meta->stack_type()) {
            // also works with pthread_task scheduling to pthread_task, the
            // transfered stack is just _main_stack.
            // 将stack交接给next_meta，这是cur_meta中需要执行的任务就已经结束了(就是用户传入fn、arg)
            // 也就是说第一个tm从task_runner开始跑,函数执行完后将stack交给后面的tm继续执行，而不需要为新的tm建一个从task_runner开始stack
            next_meta->set_stack(cur_meta->release_stack());
        } else {
            ContextualStack* stk = get_stack(next_meta->stack_type(), task_runner); // TODO(xcy)
            if (stk) {
                next_meta->set_stack(stk);
            } else {
                // stack_type is BTHREAD_STACKTYPE_PTHREAD or out of memory,
                // In latter case, attr is forced to be BTHREAD_STACKTYPE_PTHREAD.
                // This basically means that if we can't allocate stack, run
                // the task in pthread directly.
                next_meta->attr.stack_type = BTHREAD_STACKTYPE_PTHREAD;
                next_meta->set_stack(g->_main_stack);
            }
        }
    }
    sched_to(pg, next_meta); // 切换task,可能会回到main_task
}

void TaskGroup::sched(TaskGroup** pg) {
    TaskGroup* g = *pg;
    bthread_t next_tid = 0;
    // Find next task to run, if none, switch to idle thread of the group.
#ifndef BTHREAD_FAIR_WSQ
    const bool popped = g->_rq.pop(&next_tid); // 从队列尾取
#else
    const bool popped = g->_rq.steal(&next_tid); // 从队列头取
#endif
    if (!popped && !g->steal_task(&next_tid)) { // 取失败了就去偷，偷失败了就回到main_tid
        // Jump to main task if there's no task to run.
        next_tid = g->_main_tid;
    }
    sched_to(pg, next_tid); // 切换到任务next_tid
}

void TaskGroup::sched_to(TaskGroup** pg, TaskMeta* next_meta) {
    TaskGroup* g = *pg;
#ifndef NDEBUG
    if ((++g->_sched_recursive_guard) > 1) {
        LOG(FATAL) << "Recursively(" << g->_sched_recursive_guard - 1
                   << ") call sched_to(" << g << ")";
    }
#endif
    // Save errno so that errno is bthread-specific.
    const int saved_errno = errno;
    void* saved_unique_user_ptr = tls_unique_user_ptr;

    TaskMeta* const cur_meta = g->_cur_meta; // 当前的task_meta
    const int64_t now = butil::cpuwide_time_ns(); // 当前时间
    const int64_t elp_ns = now - g->_last_run_ns; // task_meta执行的时间
    g->_last_run_ns = now;
    cur_meta->stat.cputime_ns += elp_ns; // 当前task_meta累计执行时间
    if (cur_meta->tid != g->main_tid()) {
        g->_cumulated_cputime_ns += elp_ns; // task_group中task_meta的累计执行时间
    }
    ++cur_meta->stat.nswitch; // task_meta在task_group中被切换走的次数+1
    ++ g->_nswitch; // task_group中切换task_meta的次数+1
    // Switch to the task
    if (__builtin_expect(next_meta != cur_meta, 1)) { // 开始切换执行next_meta
        g->_cur_meta = next_meta; // 将next_meta做为当前task_group需要执行的task_meta
        // Switch tls_bls
        cur_meta->local_storage = tls_bls;
        tls_bls = next_meta->local_storage;

        // Logging must be done after switching the local storage, since the logging lib
        // use bthread local storage internally, or will cause memory leak.
        if ((cur_meta->attr.flags & BTHREAD_LOG_CONTEXT_SWITCH) ||
            (next_meta->attr.flags & BTHREAD_LOG_CONTEXT_SWITCH)) { // TODO(xuechengyun):没看懂
            LOG(INFO) << "Switch bthread: " << cur_meta->tid << " -> "
                      << next_meta->tid;
        }

        if (cur_meta->stack != NULL) {
            if (next_meta->stack != cur_meta->stack) {
                jump_stack(cur_meta->stack, next_meta->stack); // 汇编实现的用户态任务栈切换
                // cur_meta(主协程或其他协程)在此处挂起，然后进入next_meta bthread 任务入口(新任务从task_runner开始执行,旧任务从上次自己yield的地方开始)
                // 上面jump_stack后，汇编代码里面会jmp到next_meta的task下一步需要执行的地方,

                // probably went to another group, need to assign g again.
                g = tls_task_group; // jump_stack后程序从next_meta的任务开始执行，如果这个任务被挂起(比如调用了bthread_usleep),那么这个tm会被重新放到tg中
                // 等到tm再次被执行时，可能是被别的tg偷取执行了，也就是tls_task_group可能是偷next_meta的tg，而不是sched_to这个函数的参数tg
            }
#ifndef NDEBUG
            else { // 两个bthread任务的stack不可能相同，除非两个都是pthread任务，是main_stack
                // else pthread_task is switching to another pthread_task, sc
                // can only equal when they're both _main_stack
                CHECK(cur_meta->stack == g->_main_stack);
            }
#endif
        }
        // else because of ending_sched(including pthread_task->pthread_task)
        // 因为在ending_sched里面cur_meta的stack交给了next_meta,所以cur_meta的stack=NULL
        // 而next_meta继续在这个stack上运行，所以也不要jump_stack
    } else {
        LOG(FATAL) << "bthread=" << g->current_tid() << " sched_to itself!";
    }

    while (g->_last_context_remained) { // 上面已经切换完任务，这里如果还有remained，让remained先执行完
        RemainedFn fn = g->_last_context_remained;
        g->_last_context_remained = NULL;
        fn(g->_last_context_remained_arg);
        g = tls_task_group;
    }

    // Restore errno
    errno = saved_errno;
    tls_unique_user_ptr = saved_unique_user_ptr;

#ifndef NDEBUG
    --g->_sched_recursive_guard;
#endif
    *pg = g;
}

void TaskGroup::destroy_self() {
    if (_control) {
        _control->_destroy_group(this);
        _control = NULL;
    } else {
        CHECK(false);
    }
}

void TaskGroup::ready_to_run(bthread_t tid, bool nosignal) {
    push_rq(tid); // 往_rq里push任务
    if (nosignal) { // TODO(xcy): 什么时候需要nosignal=true
        ++_num_nosignal; // nosignal 数量加1
    } else {
        const int additional_signal = _num_nosignal;
        _num_nosignal = 0; // nosignal 归0
        _nsignaled += 1 + additional_signal; // signal数量加1(上面push_rq的这个)加所有nosignal
        _control->signal_task(1 + additional_signal); // 把没被signal的全部signal
    }
}

void TaskGroup::flush_nosignal_tasks() { // 唤醒所有nosignal的
    const int val = _num_nosignal;
    if (val) {
        _num_nosignal = 0;
        _nsignaled += val;
        _control->signal_task(val);
    }
}

void TaskGroup::ready_to_run_remote(bthread_t tid, bool nosignal) {
    _remote_rq._mutex.lock();
    while (!_remote_rq.push_locked(tid)) { // push不成功就一直push(remote_rq的push和pop用的同一个mutex)
        flush_nosignal_tasks_remote_locked(_remote_rq._mutex); // TODO(xuechengyun)
        LOG_EVERY_SECOND(ERROR) << "_remote_rq is full, capacity="
                                << _remote_rq.capacity();
        ::usleep(1000);
        _remote_rq._mutex.lock();
    }
    if (nosignal) {
        ++_remote_num_nosignal;
        _remote_rq._mutex.unlock();
    } else {
        const int additional_signal = _remote_num_nosignal;
        _remote_num_nosignal = 0;
        _remote_nsignaled += 1 + additional_signal;
        _remote_rq._mutex.unlock();
        _control->signal_task(1 + additional_signal);
    }
}

void TaskGroup::flush_nosignal_tasks_remote_locked(butil::Mutex& locked_mutex) {
    const int val = _remote_num_nosignal;
    if (!val) {
        locked_mutex.unlock();
        return;
    }
    _remote_num_nosignal = 0;
    _remote_nsignaled += val;
    locked_mutex.unlock();
    _control->signal_task(val);
}

void TaskGroup::ready_to_run_general(bthread_t tid, bool nosignal) {
    // 如果自己调自己,那就把任务放到rq, 如果别的tg调自己,那就把任务放到remote_rq
    if (tls_task_group == this) {
        return ready_to_run(tid, nosignal);
    }
    return ready_to_run_remote(tid, nosignal);
}

void TaskGroup::flush_nosignal_tasks_general() {
    if (tls_task_group == this) {
        return flush_nosignal_tasks();
    }
    return flush_nosignal_tasks_remote();
}

void TaskGroup::ready_to_run_in_worker(void* args_in) {
    ReadyToRunArgs* args = static_cast<ReadyToRunArgs*>(args_in);
    return tls_task_group->ready_to_run(args->tid, args->nosignal);
}

void TaskGroup::ready_to_run_in_worker_ignoresignal(void* args_in) {
    ReadyToRunArgs* args = static_cast<ReadyToRunArgs*>(args_in);
    return tls_task_group->push_rq(args->tid); //上面的ready_to_run就比这里多一个处理nosignal,这里不需要处理就直接push_rq
}

struct SleepArgs {
    uint64_t timeout_us;
    bthread_t tid;
    TaskMeta* meta;
    TaskGroup* group;
};

static void ready_to_run_from_timer_thread(void* arg) {
    CHECK(tls_task_group == NULL);
    const SleepArgs* e = static_cast<const SleepArgs*>(arg);
    e->group->control()->choose_one_group()->ready_to_run_remote(e->tid); // TODO(xcy): 为什么要从tc里面选一个tg，而不是直接用这个tg
}

void TaskGroup::_add_sleep_event(void* void_args) {
    // Must copy SleepArgs. After calling TimerThread::schedule(), previous
    // thread may be stolen by a worker immediately and the on-stack SleepArgs
    // will be gone.
    // 下面这行意思是，必须完全拷贝SleepArgs, 因为再下面的timer_thread会schedule这个任务(也就是放到timer_thread里面sleep一会)
    // 那么，等到timeout的时候，void_args可能已经析构了,看看_add_sleep_event的调用处usleep函数, 传入的参数是局部变量SleepArg e,
    // 也就是上面这段原英文注释说的 the on-stack SleepArgs will be gone (栈上的SleepArgs会被析构掉)
    SleepArgs e = *static_cast<SleepArgs*>(void_args);
    TaskGroup* g = e.group;

    TimerThread::TaskId sleep_id;
    sleep_id = get_global_timer_thread()->schedule(
        ready_to_run_from_timer_thread, void_args,
        butil::microseconds_from_now(e.timeout_us));

    if (!sleep_id) {
        // fail to schedule timer, go back to previous thread.
        g->ready_to_run(e.tid); // sleep失败了，继续放到tg里面等着被run
        return;
    }

    // Set TaskMeta::current_sleep which is for interruption.
    const uint32_t given_ver = get_version(e.tid);
    {
        BAIDU_SCOPED_LOCK(e.meta->version_lock);
        if (given_ver == *e.meta->version_butex && !e.meta->interrupted) {
            e.meta->current_sleep = sleep_id; // 如果没有被interrupted，那就记录下sleep_id，然后return
            return;
        }
    }
    // The thread is stopped or interrupted.
    // interrupt() always sees that current_sleep == 0. It will not schedule
    // the calling thread. The race is between current thread and timer thread.
    if (get_global_timer_thread()->unschedule(sleep_id) == 0) { // 走到这里说明已经interrupted了，从timer_thread里面拿出来就行了
        // added to timer, previous thread may be already woken up by timer and
        // even stopped. It's safe to schedule previous thread when unschedule()
        // returns 0 which means "the not-run-yet sleep_id is removed". If the
        // sleep_id is running(returns 1), ready_to_run_in_worker() will
        // schedule previous thread as well. If sleep_id does not exist,
        // previous thread is scheduled by timer thread before and we don't
        // have to do it again.
        g->ready_to_run(e.tid);
    }
}

// To be consistent with sys_usleep, set errno and return -1 on error.
int TaskGroup::usleep(TaskGroup** pg, uint64_t timeout_us) {
    if (0 == timeout_us) {
        yield(pg); // todo
        return 0;
    }
    TaskGroup* g = *pg;
    // We have to schedule timer after we switched to next bthread otherwise
    // the timer may wake up(jump to) current still-running context.
    SleepArgs e = { timeout_us, g->current_tid(), g->current_task(), g };
    g->set_remained(_add_sleep_event, &e);
    sched(pg); // sched执行完后，上面set_remained里的函数也执行完了
    g = *pg;
    e.meta->current_sleep = 0; // TODO(xcy): 为什么要归0
    if (e.meta->interrupted) {
        // Race with set and may consume multiple interruptions, which are OK.
        e.meta->interrupted = false;
        // NOTE: setting errno to ESTOP is not necessary from bthread's
        // pespective, however many RPC code expects bthread_usleep to set
        // errno to ESTOP when the thread is stopping, and print FATAL
        // otherwise. To make smooth transitions, ESTOP is still set instead
        // of EINTR when the thread is stopping.
        errno = (e.meta->stop ? ESTOP : EINTR);
        return -1;
    }
    return 0;
}

// Defined in butex.cpp
bool erase_from_butex_because_of_interruption(ButexWaiter* bw);

static int interrupt_and_consume_waiters(
    bthread_t tid, ButexWaiter** pw, uint64_t* sleep_id) {
    TaskMeta* const m = TaskGroup::address_meta(tid);
    if (m == NULL) {
        return EINVAL;
    }
    const uint32_t given_ver = get_version(tid);
    BAIDU_SCOPED_LOCK(m->version_lock);
    if (given_ver == *m->version_butex) {
        *pw = m->current_waiter.exchange(NULL, butil::memory_order_acquire);
        *sleep_id = m->current_sleep;
        m->current_sleep = 0;  // only one stopper gets the sleep_id
        m->interrupted = true;
        return 0;
    }
    return EINVAL;
}

static int set_butex_waiter(bthread_t tid, ButexWaiter* w) {
    TaskMeta* const m = TaskGroup::address_meta(tid);
    if (m != NULL) {
        const uint32_t given_ver = get_version(tid);
        BAIDU_SCOPED_LOCK(m->version_lock);
        if (given_ver == *m->version_butex) {
            // Release fence makes m->interrupted visible to butex_wait
            m->current_waiter.store(w, butil::memory_order_release);
            return 0;
        }
    }
    return EINVAL;
}

// The interruption is "persistent" compared to the ones caused by signals,
// namely if a bthread is interrupted when it's not blocked, the interruption
// is still remembered and will be checked at next blocking. This designing
// choice simplifies the implementation and reduces notification loss caused
// by race conditions.
// TODO: bthreads created by BTHREAD_ATTR_PTHREAD blocking on bthread_usleep()
// can't be interrupted.
int TaskGroup::interrupt(bthread_t tid, TaskControl* c) {
    // Consume current_waiter in the TaskMeta, wake it up then set it back.
    ButexWaiter* w = NULL;
    uint64_t sleep_id = 0;
    int rc = interrupt_and_consume_waiters(tid, &w, &sleep_id);
    if (rc) {
        return rc;
    }
    // a bthread cannot wait on a butex and be sleepy at the same time.
    CHECK(!sleep_id || !w);
    if (w != NULL) {
        erase_from_butex_because_of_interruption(w);
        // If butex_wait() already wakes up before we set current_waiter back,
        // the function will spin until current_waiter becomes non-NULL.
        rc = set_butex_waiter(tid, w);
        if (rc) {
            LOG(FATAL) << "butex_wait should spin until setting back waiter";
            return rc;
        }
    } else if (sleep_id != 0) {
        if (get_global_timer_thread()->unschedule(sleep_id) == 0) {
            bthread::TaskGroup* g = bthread::tls_task_group;
            if (g) {
                g->ready_to_run(tid);
            } else {
                if (!c) {
                    return EINVAL;
                }
                c->choose_one_group()->ready_to_run_remote(tid);
            }
        }
    }
    return 0;
}

void TaskGroup::yield(TaskGroup** pg) { // 切换到下一个tm，把当前tm重新放到队列
    TaskGroup* g = *pg;
    ReadyToRunArgs args = { g->current_tid(), false };
    g->set_remained(ready_to_run_in_worker, &args);
    sched(pg);
}

void print_task(std::ostream& os, bthread_t tid) {
    TaskMeta* const m = TaskGroup::address_meta(tid);
    if (m == NULL) {
        os << "bthread=" << tid << " : never existed";
        return;
    }
    const uint32_t given_ver = get_version(tid);
    bool matched = false;
    bool stop = false;
    bool interrupted = false;
    bool about_to_quit = false;
    void* (*fn)(void*) = NULL;
    void* arg = NULL;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bool has_tls = false;
    int64_t cpuwide_start_ns = 0;
    TaskStatistics stat = {0, 0};
    {
        BAIDU_SCOPED_LOCK(m->version_lock);
        if (given_ver == *m->version_butex) {
            matched = true;
            stop = m->stop;
            interrupted = m->interrupted;
            about_to_quit = m->about_to_quit;
            fn = m->fn;
            arg = m->arg;
            attr = m->attr;
            has_tls = m->local_storage.keytable;
            cpuwide_start_ns = m->cpuwide_start_ns;
            stat = m->stat;
        }
    }
    if (!matched) {
        os << "bthread=" << tid << " : not exist now";
    } else {
        os << "bthread=" << tid << " :\nstop=" << stop
           << "\ninterrupted=" << interrupted
           << "\nabout_to_quit=" << about_to_quit
           << "\nfn=" << (void*)fn
           << "\narg=" << (void*)arg
           << "\nattr={stack_type=" << attr.stack_type
           << " flags=" << attr.flags
           << " keytable_pool=" << attr.keytable_pool
           << "}\nhas_tls=" << has_tls
           << "\nuptime_ns=" << butil::cpuwide_time_ns() - cpuwide_start_ns
           << "\ncputime_ns=" << stat.cputime_ns
           << "\nnswitch=" << stat.nswitch;
    }
}

}  // namespace bthread
