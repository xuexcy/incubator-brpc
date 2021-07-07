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

#ifndef BTHREAD_TASK_GROUP_H
#define BTHREAD_TASK_GROUP_H

#include "butil/time.h"                             // cpuwide_time_ns
#include "bthread/task_control.h"
#include "bthread/task_meta.h"                     // bthread_t, TaskMeta
#include "bthread/work_stealing_queue.h"           // WorkStealingQueue
#include "bthread/remote_task_queue.h"             // RemoteTaskQueue
#include "butil/resource_pool.h"                    // ResourceId
#include "bthread/parking_lot.h"

namespace bthread {

// For exiting a bthread.
class ExitException : public std::exception {
public:
    explicit ExitException(void* value) : _value(value) {}
    ~ExitException() throw() {}
    const char* what() const throw() override {
        return "ExitException";
    }
    void* value() const {
        return _value;
    }
private:
    void* _value;
};

// Thread-local group of tasks.
// Notice that most methods involving context switching are static otherwise
// pointer `this' may change after wakeup. The **pg parameters in following
// function are updated before returning.
class TaskGroup {
public:
    // Create task `fn(arg)' with attributes `attr' in TaskGroup *pg and put
    // the identifier into `tid'. Switch to the new task and schedule old task
    // to run.
    // Return 0 on success, errno otherwise.
    // xcy_done:执行这个任务，如果有其他正在执行的tm，让它靠边站
    static int start_foreground(TaskGroup** pg,
                                bthread_t* __restrict tid,
                                const bthread_attr_t* __restrict attr,
                                void * (*fn)(void*),
                                void* __restrict arg);

    // Create task `fn(arg)' with attributes `attr' in this TaskGroup, put the
    // identifier into `tid'. Schedule the new thread to run.
    //   Called from worker: start_background<false>
    //   Called from non-worker: start_background<true>
    // Return 0 on success, errno otherwise.
    template <bool REMOTE>
    int start_background(bthread_t* __restrict tid,
                         const bthread_attr_t* __restrict attr,
                         void * (*fn)(void*),
                         void* __restrict arg);

    // Suspend caller and run next bthread in TaskGroup *pg.
    static void sched(TaskGroup** pg); // xcy_done: 找任务再切任务(切换调用下面的sched_to(next_tid))
    // xcy_done: 切到下一个任务(找到任务后会调用sched_to(next_meta)), 它比sched多的一段代码[基本就是]sched_to(next_tid)里面的代码
    // 相当于都是先sched_to(next_tid) 再 sched_to(next_meta), 不同的地方在于调用ending_sched的地方一定是刚刚执行完了另一个task,
    // 前面说[基本就是],也就是说还有不一样的地方，那就是在ending_sched里面判断了切换后的任务与刚执行的任务是否是同一个stack_type
    // 如果是的话,那就复用上一个tm的stack,免得又重新申请
    static void ending_sched(TaskGroup** pg);

    // Suspend caller and run bthread `next_tid' in TaskGroup *pg.
    // Purpose of this function is to avoid pushing `next_tid' to _rq and
    // then being popped by sched(pg), which is not necessary.
    static void sched_to(TaskGroup** pg, TaskMeta* next_meta);
    static void sched_to(TaskGroup** pg, bthread_t next_tid); // xcy_done 切换成任务next_tid
    static void exchange(TaskGroup** pg, bthread_t next_tid); // xcy_done 把正在执行的任务放到remaind，并开始执行参数里的任务(TODO(xcy): 谁在调)

    // The callback will be run in the beginning of next-run bthread.
    // Can't be called by current bthread directly because it often needs
    // the target to be suspended already.
    typedef void (*RemainedFn)(void*);
    void set_remained(RemainedFn cb, void* arg) { // TODO(xcy):什么时候会需要remained，会不会并发
        _last_context_remained = cb;
        _last_context_remained_arg = arg;
    }

    // Suspend caller for at least |timeout_us| microseconds.
    // If |timeout_us| is 0, this function does nothing.
    // If |group| is NULL or current thread is non-bthread, call usleep(3)
    // instead. This function does not create thread-local TaskGroup.
    // Returns: 0 on success, -1 otherwise and errno is set.
    static int usleep(TaskGroup** pg, uint64_t timeout_us); // 把正在执行的task放到timer_thread中去sleep

    // Suspend caller and run another bthread. When the caller will resume
    // is undefined.
    static void yield(TaskGroup** pg); // xcy_done:挂起正在运行的任务并去执行下一个

    // Suspend caller until bthread `tid' terminates.
    static int join(bthread_t tid, void** return_value);

    // Returns true iff the bthread `tid' still exists. Notice that it is
    // just the result at this very moment which may change soon.
    // Don't use this function unless you have to. Never write code like this:
    //    if (exists(tid)) {
    //        Wait for events of the thread.   // Racy, may block indefinitely.
    //    }
    static bool exists(bthread_t tid); xcy_done

    // Put attribute associated with `tid' into `*attr'.
    // Returns 0 on success, -1 otherwise and errno is set.
    static int get_attr(bthread_t tid, bthread_attr_t* attr);

    // Get/set TaskMeta.stop of the tid.
    static void set_stopped(bthread_t tid);
    static bool is_stopped(bthread_t tid);

    // The bthread running run_main_task();
    bthread_t main_tid() const { return _main_tid; }
    TaskStatistics main_stat() const;
    // Routine of the main task which should be called from a dedicated pthread.
    void run_main_task(); // xcy_done

    // current_task is a function in macOS 10.0+
#ifdef current_task
#undef current_task
#endif
    // Meta/Identifier of current task in this group.
    TaskMeta* current_task() const { return _cur_meta; } // xcy_done
    bthread_t current_tid() const { return _cur_meta->tid; } // xcy_done
    // Uptime of current task in nanoseconds.
    int64_t current_uptime_ns() const // xcy_done 当前任务的运行时间
    { return butil::cpuwide_time_ns() - _cur_meta->cpuwide_start_ns; }

    // True iff current task is the one running run_main_task()
    bool is_current_main_task() const { return current_tid() == _main_tid; } // xcy_done: 是否是main_task
    // True iff current task is in pthread-mode.
    bool is_current_pthread_task() const // xcy_done:当前任务时候是pthread_task(只有main_task才是在pthread并且main_task没有任何要执行的东西,它的fn是NULL)
    { return _cur_meta->stack == _main_stack; }

    // Active time in nanoseconds spent by this TaskGroup.
    int64_t cumulated_cputime_ns() const { return _cumulated_cputime_ns; } // tg的累计执行时间(就是在这个tg里面tm的执行时间之和)

    // Push a bthread into the runqueue
    void ready_to_run(bthread_t tid, bool nosignal = false); // 把任务放到rq里面
    // Flush tasks pushed to rq but signalled.
    void flush_nosignal_tasks();

    // Push a bthread into the runqueue from another non-worker thread.
    void ready_to_run_remote(bthread_t tid, bool nosignal = false); // xcy_done 把任务放到remote_rq执行
    void flush_nosignal_tasks_remote_locked(butil::Mutex& locked_mutex);
    void flush_nosignal_tasks_remote();

    // Automatically decide the caller is remote or local, and call
    // the corresponding function.
    void ready_to_run_general(bthread_t tid, bool nosignal = false); // xcy_done
    void flush_nosignal_tasks_general();

    // The TaskControl that this TaskGroup belongs to.
    TaskControl* control() const { return _control; }

    // Call this instead of delete.
    void destroy_self(); // 由tc来删掉这个tg

    // Wake up blocking ops in the thread.
    // Returns 0 on success, errno otherwise.
    static int interrupt(bthread_t tid, TaskControl* c);

    // Get the meta associate with the task.
    static TaskMeta* address_meta(bthread_t tid); // 根据tid拿到tm

    // Push a task into _rq, if _rq is full, retry after some time. This
    // process make go on indefinitely.
    void push_rq(bthread_t tid); // 把任务放到rq里面

private:
friend class TaskControl;

    // You shall use TaskControl::create_group to create new instance.
    explicit TaskGroup(TaskControl*);

    int init(size_t runqueue_capacity);

    // You shall call destroy_self() instead of destructor because deletion
    // of groups are postponed to avoid race.
    ~TaskGroup();

    static void task_runner(intptr_t skip_remained); // xcy_done:执行当前任务，然后把rq中的任务执行完

    // Callbacks for set_remained()
    static void _release_last_context(void*);
    static void _add_sleep_event(void*); // 把tm放到timer_thread中
    struct ReadyToRunArgs {
        bthread_t tid;
        bool nosignal;
    };
    static void ready_to_run_in_worker(void*);
    static void ready_to_run_in_worker_ignoresignal(void*); // TODO(xcy)看看啥时候用

    // Wait for a task to run.
    // Returns true on success, false is treated as permanent error and the
    // loop calling this function should end.
    bool wait_task(bthread_t* tid); // xcy_done 取任务，没有就wait

    bool steal_task(bthread_t* tid) { // xcy_done 先消费remote_rq再去其他tg偷
        if (_remote_rq.pop(tid)) {
            return true;
        }
#ifndef BTHREAD_DONT_SAVE_PARKING_STATE
        _last_pl_state = _pl->get_state(); // 保存pl状态
#endif
        return _control->steal_task(tid, &_steal_seed, _steal_offset); // 通过tc统一偷task
    }

#ifndef NDEBUG
    int _sched_recursive_guard;
#endif

    TaskMeta* _cur_meta; // 当前任务

    // the control that this group belongs to
    TaskControl* _control; // tc，全局唯一，所有tg里的tc都一样
    int _num_nosignal;
    int _nsignaled;
    // last scheduling time
    int64_t _last_run_ns; // 上一个任务最后的运行时间/开始切换新任务的时间
    int64_t _cumulated_cputime_ns; // tg中所有tm总计执行时间

    size_t _nswitch; // 切换tm的次数
    RemainedFn _last_context_remained; // 在当前tm执行完后会被执行的函数,一般是刚刚被临时挂起的tm
    void* _last_context_remained_arg;

    ParkingLot* _pl; // tc中的某个pl
#ifndef BTHREAD_DONT_SAVE_PARKING_STATE
    ParkingLot::State _last_pl_state; // pl上一次的状态
#endif
    size_t _steal_seed;
    size_t _steal_offset;
    ContextualStack* _main_stack;
    bthread_t _main_tid; // TODO(xuechengyun):这玩意到底是干嘛的,又没有可执行任务
    WorkStealingQueue<bthread_t> _rq; // 双向队列
    RemoteTaskQueue _remote_rq; // 单向有锁队列
    int _remote_num_nosignal;
    int _remote_nsignaled;
};

}  // namespace bthread

#include "task_group_inl.h"

#endif  // BTHREAD_TASK_GROUP_H
