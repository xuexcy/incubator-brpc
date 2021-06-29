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

#ifndef BTHREAD_WORK_STEALING_QUEUE_H
#define BTHREAD_WORK_STEALING_QUEUE_H

#include "butil/macros.h"
#include "butil/atomicops.h"
#include "butil/logging.h"

namespace bthread {

template <typename T> // 参考:https://www.pianshen.com/article/12341222944/
class WorkStealingQueue { // 双端队列, push和pop都从bottom
public:
    WorkStealingQueue()
        : _bottom(1) // 初始化时第一次放在下标1（其实好像初始化为0也可以)
        , _capacity(0)
        , _buffer(NULL)
        , _top(1) { // _bottom - _top = 队列中元素的个数
    }

    ~WorkStealingQueue() { // xcy_done
        delete [] _buffer;
        _buffer = NULL;
    }

    int init(size_t capacity) { // xcy_done
        if (_capacity != 0) {
            LOG(ERROR) << "Already initialized";
            return -1;
        }
        if (capacity == 0) {
            LOG(ERROR) << "Invalid capacity=" << capacity;
            return -1;
        }
        if (capacity & (capacity - 1)) {
            LOG(ERROR) << "Invalid capacity=" << capacity
                       << " which must be power of 2";
            return -1;
        }
        _buffer = new(std::nothrow) T[capacity]; // 用数组实现的队列
        if (NULL == _buffer) {
            return -1;
        }
        _capacity = capacity;
        return 0;
    }
    // 某个worker的push和pop不可能同时并发，但是都可能和该worker的steal并发
    // Push an item into the queue.
    // Returns true on pushed.
    // May run in parallel with steal().
    // Never run in parallel with pop() or another push().
    bool push(const T& x) { // xcy_done
        const size_t b = _bottom.load(butil::memory_order_relaxed);
        const size_t t = _top.load(butil::memory_order_acquire);
        if (b >= t + _capacity) { // Full queue.
            return false;
        }
        _buffer[b & (_capacity - 1)] = x; // 通过&(capacity - 1)来循环数组下标
        _bottom.store(b + 1, butil::memory_order_release); // 队列尾序号+1
        return true;
    }

    // Pop an item from the queue.
    // Returns true on popped and the item is written to `val'.
    // May run in parallel with steal().
    // Never run in parallel with push() or another pop().
    bool pop(T* val) { // xcy_done
        const size_t b = _bottom.load(butil::memory_order_relaxed);
        size_t t = _top.load(butil::memory_order_relaxed);
        if (t >= b) {
            // fast check since we call pop() in each sched.
            // Stale _top which is smaller should not enter this branch.
            return false;
        }
        const size_t newb = b - 1;
        _bottom.store(newb, butil::memory_order_relaxed); // 队列尾序号-1
        butil::atomic_thread_fence(butil::memory_order_seq_cst);
        t = _top.load(butil::memory_order_relaxed);
        if (t > newb) {
            _bottom.store(b, butil::memory_order_relaxed); // 重置回之前的序号
            return false;
        }
        *val = _buffer[newb & (_capacity - 1)]; // 取出队列尾的值
        if (t != newb) { // 如果相等，说明取出了队列中的最后一个元素
            return true;
        }
        // Single last element, compete with steal() 队列只剩最后一个元素，和下面的steal进行竞争
        // 1. 如果竞争成功，说明steal()里的top还是一开始的t,则将top加1
        // 2. 如果竞争失败，top会在steal()里加1,那这里保持原值t即可
        const bool popped = _top.compare_exchange_strong(
            t, t + 1, butil::memory_order_seq_cst, butil::memory_order_relaxed);
        // 3. 不论竞争成功与否，队列都空了，top会在此处或steal()里加1，这里bottom维持原值b不变。
        // 也就是当出现竞争时，就不要管bottom的变化了，搞定top的变化就行了
        _bottom.store(b, butil::memory_order_relaxed);
        return popped;
    }

    // Steal one item from the queue.
    // Returns true on stolen.
    // May run in parallel with push() pop() or another steal().
    bool steal(T* val) {
        size_t t = _top.load(butil::memory_order_acquire);
        size_t b = _bottom.load(butil::memory_order_acquire);
        if (t >= b) {
            // Permit false negative for performance considerations.
            return false;
        }
        do {
            butil::atomic_thread_fence(butil::memory_order_seq_cst);
            b = _bottom.load(butil::memory_order_acquire);
            if (t >= b) {
                return false;
            }
            *val = _buffer[t & (_capacity - 1)];
        // 当队列只剩最后一个元素且与上面pop竞争失败时,那就没有steal到task, while里面的条件会为false
        // TODO(xuechengyun): 为什么steal失败后要while而不是直接return false
        } while (!_top.compare_exchange_strong(t, t + 1,
                                               butil::memory_order_seq_cst,
                                               butil::memory_order_relaxed));
        return true;
    }

    size_t volatile_size() const { // xcy_done: 队列中元素的个数
        const size_t b = _bottom.load(butil::memory_order_relaxed);
        const size_t t = _top.load(butil::memory_order_relaxed);
        return (b <= t ? 0 : (b - t));
    }

    size_t capacity() const { return _capacity; } // xcy_done: 数组大小

private:
    // Copying a concurrent structure makes no sense.
    DISALLOW_COPY_AND_ASSIGN(WorkStealingQueue);

    butil::atomic<size_t> _bottom; // 当前可以放置元素的地方(队列尾是_bottom - 1)
    size_t _capacity; // 数组大小(2的幂)
    T* _buffer; // 数组队列
    butil::atomic<size_t> BAIDU_CACHELINE_ALIGNMENT _top; // 队列头
};

}  // namespace bthread

#endif  // BTHREAD_WORK_STEALING_QUEUE_H
