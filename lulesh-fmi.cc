// FMI-backed implementation of the MPI shim declared in lulesh-fmi.h.
//
// FMI (extern/fmi) only offers BLOCKING send/recv and is not safe to drive
// concurrently on one Communicator, so LULESH's non-blocking 26-neighbour halo
// exchange (Irecv/Isend ... Wait/Waitall) is emulated by RECORDING each request
// and executing the whole batch at the first Wait/Waitall, in a deadlock-free
// order.
//
// Deadlock freedom: every send has a matching recv on the peer (MPI semantics).
// We iterate the peers we exchange with in ascending rank order; for peer P, the
// lower-ranked endpoint of the pair sends first, the higher-ranked one receives
// first. The globally-lowest unfinished rank therefore never blocks on a send
// before it reaches a recv that drains a higher peer, so progress is guaranteed
// up the ranks. FMI's Direct backend uses separate directional sockets for A->B
// and B->A, so a send and recv to/from the same peer never self-deadlock.
//
// Everything runs on the master thread only (LULESH uses MPI_THREAD_FUNNELED),
// which also keeps FMI operation boundaries clean for the future CRIU migration.

#include "lulesh-fmi.h"

#include <fmi.h>
#include <comm/Data.h>
#include <utils/Function.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>
#include <thread>
#include <vector>

namespace {

FMI::Communicator* g_comm = nullptr;
int g_rank = 0;
int g_size = 1;

// One recorded, not-yet-executed point-to-point request.
struct PendingOp {
    bool   is_send;
    int    peer;
    char*  buf;
    size_t bytes;
};

std::vector<PendingOp> g_pending;
bool g_flushed = false; // the current batch has already been executed

[[noreturn]] void fail(const std::string& what) {
    std::fprintf(stderr, "[FMI rank %d] fatal: %s\n", g_rank, what.c_str());
    std::exit(1);
}

const char* require_env(const char* name) {
    const char* v = std::getenv(name);
    if (v == nullptr || v[0] == '\0') {
        fail(std::string("environment variable ") + name +
             " must be set (FMI_RANK, FMI_WORLD_SIZE, FMI_CONFIG, FMI_COMM_NAME)");
    }
    return v;
}

// A new exchange phase always starts with Irecv/Isend; clear the just-finished
// batch lazily when the first request of the next phase is recorded.
void reset_batch_if_flushed() {
    if (g_flushed) {
        g_pending.clear();
        g_flushed = false;
    }
}

void send_one(const PendingOp& op) {
    FMI::Comm::Data<void*> d(static_cast<void*>(op.buf), op.bytes);
    g_comm->send(d, static_cast<FMI::Utils::peer_num>(op.peer));
}

void recv_one(const PendingOp& op) {
    FMI::Comm::Data<void*> d(static_cast<void*>(op.buf), op.bytes);
    g_comm->recv(d, static_cast<FMI::Utils::peer_num>(op.peer));
}

void flush_batch() {
    if (g_flushed) return;

    // Distinct peers we exchange with this phase, in ascending rank order.
    std::vector<int> peers;
    for (const PendingOp& op : g_pending) {
        if (std::find(peers.begin(), peers.end(), op.peer) == peers.end()) {
            peers.push_back(op.peer);
        }
    }
    std::sort(peers.begin(), peers.end());

    try {
        for (int p : peers) {
            const bool send_first = (g_rank < p);
            if (send_first) {
                for (const PendingOp& op : g_pending)
                    if (op.peer == p && op.is_send) send_one(op);
                for (const PendingOp& op : g_pending)
                    if (op.peer == p && !op.is_send) recv_one(op);
            } else {
                for (const PendingOp& op : g_pending)
                    if (op.peer == p && !op.is_send) recv_one(op);
                for (const PendingOp& op : g_pending)
                    if (op.peer == p && op.is_send) send_one(op);
            }
        }
    } catch (const std::exception& e) {
        fail(std::string("halo exchange failed: ") + e.what());
    } catch (...) {
        fail("halo exchange failed: unknown exception");
    }

    g_flushed = true;
}

// Collectives must never interleave with a half-flushed P2P batch. LULESH never
// does this, but fail loudly if a future change ever violates it.
void assert_no_pending(const char* who) {
    if (!g_pending.empty() && !g_flushed) {
        fail(std::string(who) + " called with an unflushed point-to-point batch");
    }
}

template <typename T>
FMI::Utils::Function<T> make_reduce_fn(MPI_Op op) {
    switch (op) {
        case MPI_MIN:
            return FMI::Utils::Function<T>([](T a, T b) { return a < b ? a : b; }, true, true);
        case MPI_MAX:
            return FMI::Utils::Function<T>([](T a, T b) { return a > b ? a : b; }, true, true);
        case MPI_SUM:
            return FMI::Utils::Function<T>([](T a, T b) { return a + b; }, true, true);
        default:
            fail("unsupported MPI_Op in reduction");
    }
}

// Scalar (count==1) allreduce/reduce for double or float, the only forms LULESH
// uses. `do_reduce` runs the matching FMI collective; allreduce passes root=-1.
template <typename T>
void scalar_collective(const void* sendbuf, void* recvbuf, MPI_Op op, int root, bool allreduce) {
    T s = *static_cast<const T*>(sendbuf);
    FMI::Comm::Data<T> sd(s), rd;
    auto fn = make_reduce_fn<T>(op);
    if (allreduce) {
        g_comm->allreduce(sd, rd, fn);
        *static_cast<T*>(recvbuf) = rd.get();
    } else {
        g_comm->reduce(sd, rd, static_cast<FMI::Utils::peer_num>(root), fn);
        if (g_rank == root) *static_cast<T*>(recvbuf) = rd.get();
    }
}

// Demo support for the CRIU rank-migration demo (no effect on normal runs).
//
// LULESH calls MPI_Allreduce exactly once per cycle (the top-of-cycle `dt` MIN
// reduction in TimeIncrement). That point is a clean migration quiesce boundary:
// the previous cycle's point-to-point halo flushes have all completed (no
// in-flight messages to strand) and every rank meets here collectively. When
// FMI_MIGRATE_AT_CYCLE=C is set, the C-th allreduce call prints a marker and
// holds for FMI_MIGRATE_WINDOW_MS *before* entering the collective, giving the
// demo driver a deterministic window to mark a pending CRIU migration. FMI's
// transparent-migration runtime then quiesces the targeted rank inside this very
// allreduce (at the OperationGuard boundary). With the env unset this is a no-op,
// so ordinary FMI runs are unchanged.
void maybe_open_migration_window(long call_no) {
    const char* at = std::getenv("FMI_MIGRATE_AT_CYCLE");
    if (at == nullptr || at[0] == '\0') return;
    if (call_no != std::atol(at)) return;

    long window_ms = 8000;
    if (const char* w = std::getenv("FMI_MIGRATE_WINDOW_MS")) {
        if (w[0] != '\0') window_ms = std::atol(w);
    }
    std::fprintf(stderr,
                 "FMI_MIGRATE: window open rank=%d allreduce_call=%ld holding_ms=%ld "
                 "(quiesce point reached; mark a pending migration now)\n",
                 g_rank, call_no, window_ms);
    std::fflush(stderr);
    std::this_thread::sleep_for(std::chrono::milliseconds(window_ms));
    std::fprintf(stderr, "FMI_MIGRATE: window closed rank=%d entering allreduce\n", g_rank);
    std::fflush(stderr);
}

} // namespace

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

int MPI_Init(int* /*argc*/, char*** /*argv*/) {
    g_rank = std::atoi(require_env("FMI_RANK"));
    g_size = std::atoi(require_env("FMI_WORLD_SIZE"));
    const std::string config = require_env("FMI_CONFIG");
    const std::string name   = require_env("FMI_COMM_NAME");
    try {
        g_comm = new FMI::Communicator(static_cast<FMI::Utils::peer_num>(g_rank),
                                       static_cast<FMI::Utils::peer_num>(g_size),
                                       config, name);
    } catch (const std::exception& e) {
        fail(std::string("FMI::Communicator construction failed: ") + e.what());
    } catch (...) {
        fail("FMI::Communicator construction failed: unknown exception");
    }
    return MPI_SUCCESS;
}

int MPI_Init_thread(int* argc, char*** argv, int /*required*/, int* provided) {
    MPI_Init(argc, argv);
    if (provided) *provided = MPI_THREAD_FUNNELED;
    return MPI_SUCCESS;
}

int MPI_Finalize() {
    delete g_comm;
    g_comm = nullptr;
    return MPI_SUCCESS;
}

int MPI_Comm_rank(MPI_Comm /*comm*/, int* rank) { *rank = g_rank; return MPI_SUCCESS; }
int MPI_Comm_size(MPI_Comm /*comm*/, int* size) { *size = g_size; return MPI_SUCCESS; }

int MPI_Abort(MPI_Comm /*comm*/, int errorcode) {
    std::fprintf(stderr, "[FMI rank %d] MPI_Abort(code=%d)\n", g_rank, errorcode);
    std::exit(errorcode);
}

double MPI_Wtime() {
    using clock = std::chrono::steady_clock;
    return std::chrono::duration<double>(clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Collectives
// ---------------------------------------------------------------------------

int MPI_Barrier(MPI_Comm /*comm*/) {
    assert_no_pending("MPI_Barrier");
    try {
        g_comm->barrier();
    } catch (const std::exception& e) {
        fail(std::string("barrier failed: ") + e.what());
    } catch (...) {
        fail("barrier failed: unknown exception");
    }
    return MPI_SUCCESS;
}

int MPI_Allreduce(const void* sendbuf, void* recvbuf, int count,
                  MPI_Datatype datatype, MPI_Op op, MPI_Comm /*comm*/) {
    if (count != 1) fail("MPI_Allreduce shim supports count==1 only");
    assert_no_pending("MPI_Allreduce");
    static long allreduce_calls = 0;
    maybe_open_migration_window(++allreduce_calls);
    try {
        if (datatype == MPI_DOUBLE)
            scalar_collective<double>(sendbuf, recvbuf, op, /*root*/ 0, /*allreduce*/ true);
        else
            scalar_collective<float>(sendbuf, recvbuf, op, /*root*/ 0, /*allreduce*/ true);
    } catch (const std::exception& e) {
        fail(std::string("allreduce failed: ") + e.what());
    } catch (...) {
        fail("allreduce failed: unknown exception");
    }
    return MPI_SUCCESS;
}

int MPI_Reduce(const void* sendbuf, void* recvbuf, int count,
               MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm /*comm*/) {
    if (count != 1) fail("MPI_Reduce shim supports count==1 only");
    assert_no_pending("MPI_Reduce");
    try {
        if (datatype == MPI_DOUBLE)
            scalar_collective<double>(sendbuf, recvbuf, op, root, /*allreduce*/ false);
        else
            scalar_collective<float>(sendbuf, recvbuf, op, root, /*allreduce*/ false);
    } catch (const std::exception& e) {
        fail(std::string("reduce failed: ") + e.what());
    } catch (...) {
        fail("reduce failed: unknown exception");
    }
    return MPI_SUCCESS;
}

// ---------------------------------------------------------------------------
// Point-to-point (recorded now, executed at the first Wait/Waitall)
// ---------------------------------------------------------------------------

int MPI_Irecv(void* buf, int count, MPI_Datatype datatype, int src, int /*tag*/,
              MPI_Comm /*comm*/, MPI_Request* request) {
    reset_batch_if_flushed();
    g_pending.push_back(PendingOp{false, src, static_cast<char*>(buf),
                                  static_cast<size_t>(count) * static_cast<size_t>(datatype)});
    if (request) *request = 1;
    return MPI_SUCCESS;
}

int MPI_Isend(const void* buf, int count, MPI_Datatype datatype, int dest, int /*tag*/,
              MPI_Comm /*comm*/, MPI_Request* request) {
    reset_batch_if_flushed();
    g_pending.push_back(PendingOp{true, dest,
                                  static_cast<char*>(const_cast<void*>(buf)),
                                  static_cast<size_t>(count) * static_cast<size_t>(datatype)});
    if (request) *request = 1;
    return MPI_SUCCESS;
}

int MPI_Wait(MPI_Request* request, MPI_Status* /*status*/) {
    flush_batch();
    if (request) *request = MPI_REQUEST_NULL;
    return MPI_SUCCESS;
}

int MPI_Waitall(int count, MPI_Request* requests, MPI_Status* /*statuses*/) {
    flush_batch();
    if (requests)
        for (int i = 0; i < count; ++i) requests[i] = MPI_REQUEST_NULL;
    return MPI_SUCCESS;
}
