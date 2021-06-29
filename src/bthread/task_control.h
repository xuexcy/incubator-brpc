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

#ifndef BTHREAD_TASK_CONTROL_H
#define BTHREAD_TASK_CONTROL_H

#ifndef NDEBUG
#include <iostream>                             // std::ostream
#endif
#include <stddef.h>                             // size_t
#include "butil/atomicops.h"                     // butil::atomic
#include "bvar/bvar.h"                          // bvar::PassiveStatus
#include "bthread/task_meta.h"                  // TaskMeta
#include "butil/resource_pool.h"                 // ResourcePool
#include "bthread/work_stealing_queue.h"        // WorkStealingQueue
#include "bthread/parking_lot.h"

namespace bthread {

class TaskGroup;

// Control all task groups
class TaskControl {
    friend class TaskGroup;

public:
    TaskControl();
    ~TaskControl(); // TODO(xuechengyun)

    // Must be called before using. `nconcurrency' is # of worker pthreads.
    int init(int nconcurrency); // xcy_done: 根据参数创建worker

    // Create a TaskGroup in this control.
    TaskGroup* create_group(); // xcy_done

    // Steal a task from a "random" group.
    bool steal_task(bthread_t* tid, size_t* seed, size_t offset); // xcy_done:由某个tg调用，偷取其他tg的_rq或_remote_rq中的task

    // Tell other groups that `n' tasks was just added to caller's runqueue
    void signal_task(int num_task); // TODO

    // Stop and join worker threads in TaskControl.
    void stop_and_join(); // xcy_done

    // Get # of worker threads.
    int concurrency() const
    { return _concurrency.load(butil::memory_order_acquire); }

    void print_rq_sizes(std::ostream& os);

    double get_cumulated_worker_time(); // xcy_done: 所以worker/tg的执行时间之和
    int64_t get_cumulated_switch_count();
    int64_t get_cumulated_signal_count();

    // [Not thread safe] Add more worker threads.
    // Return the number of workers actually added, which may be less than |num|
    int add_workers(int num); // xcy_done:新建worker,和init中创建的worker一个意思

    // Choose one TaskGroup (randomly right now).
    // If this method is called after init(), it never returns NULL.
    TaskGroup* choose_one_group(); // 随机选一个tg

private:
    // Add/Remove a TaskGroup.
    // Returns 0 on success, -1 otherwise.
    int _add_group(TaskGroup*); // xcy_done: 记录新的tg
    int _destroy_group(TaskGroup*); // xcy_done: 删除对这个tg的记录

    static void delete_task_group(void* arg); // xcy_done: delete arg

    static void* worker_thread(void* task_control); // xcy_done:创建tg并执行tg中的task(死循环)

    bvar::LatencyRecorder& exposed_pending_time();
    bvar::LatencyRecorder* create_exposed_pending_time();

    // 下面的group、worker的数量等价于创建的task_group的数量
    butil::atomic<size_t> _ngroup; // 已经创建的worker数量
    TaskGroup** _groups; // tg数组
    butil::Mutex _modify_group_mutex; // 修改_groups需要的mutex

    bool _stop; // tc是否停止的标记
    butil::atomic<int> _concurrency; // 需要创建的worker的数量,用户传入的参数
    std::vector<pthread_t> _workers; // worker中运行着tg

    bvar::Adder<int64_t> _nworkers; // 创建的worker数量，新建就<<1, 删除就>>1
    butil::Mutex _pending_time_mutex;
    butil::atomic<bvar::LatencyRecorder*> _pending_time;
    bvar::PassiveStatus<double> _cumulated_worker_time; // TODO(xuechengyun)
    bvar::PerSecond<bvar::PassiveStatus<double> > _worker_usage_second;
    bvar::PassiveStatus<int64_t> _cumulated_switch_count;
    bvar::PerSecond<bvar::PassiveStatus<int64_t> > _switch_per_second;
    bvar::PassiveStatus<int64_t> _cumulated_signal_count;
    bvar::PerSecond<bvar::PassiveStatus<int64_t> > _signal_per_second;
    bvar::PassiveStatus<std::string> _status;
    bvar::Adder<int64_t> _nbthreads;

    static const int PARKING_LOT_NUM = 4;
    ParkingLot _pl[PARKING_LOT_NUM];
};

inline bvar::LatencyRecorder& TaskControl::exposed_pending_time() {
    bvar::LatencyRecorder* pt = _pending_time.load(butil::memory_order_consume);
    if (!pt) {
        pt = create_exposed_pending_time();
    }
    return *pt;
}

}  // namespace bthread

#endif  // BTHREAD_TASK_CONTROL_H
