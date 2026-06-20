#ifndef LULESH_FMI_H
#define LULESH_FMI_H

// MPI-compatible shim backed by a single global FMI::Communicator.
//
// This header is pulled in by lulesh.h when the build is configured with
// -DUSE_FMI=1 (in place of <mpi.h>). It declares just the subset of the MPI
// API that LULESH uses, so the rest of the sources compile unchanged. The
// implementation lives in lulesh-fmi.cc and is written against the FMI library
// (extern/fmi). See that file for the deferred, deadlock-free emulation of the
// non-blocking point-to-point calls on top of FMI's blocking send/recv.

#include <cstddef>

// ---- Opaque handle types ---------------------------------------------------
// LULESH only ever passes MPI_COMM_WORLD around and never inspects a status, so
// these can be trivial. MPI_Datatype encodes the element size in its value so
// that count * datatype == message size in bytes.
typedef int MPI_Comm;
typedef int MPI_Request;
typedef int MPI_Status;

enum MPI_Datatype { MPI_DATATYPE_NULL = 0, MPI_FLOAT = 4, MPI_DOUBLE = 8 };
enum MPI_Op { MPI_OP_NULL = 0, MPI_MIN, MPI_MAX, MPI_SUM };

// ---- Constants -------------------------------------------------------------
inline constexpr MPI_Comm    MPI_COMM_WORLD     = 0;
inline constexpr MPI_Request MPI_REQUEST_NULL   = 0;
inline constexpr int         MPI_SUCCESS        = 0;
inline constexpr int         MPI_THREAD_SINGLE  = 0;
inline constexpr int         MPI_THREAD_FUNNELED = 1;

// ---- Lifecycle / environment ----------------------------------------------
int    MPI_Init(int* argc, char*** argv);
int    MPI_Init_thread(int* argc, char*** argv, int required, int* provided);
int    MPI_Finalize();
int    MPI_Comm_rank(MPI_Comm comm, int* rank);
int    MPI_Comm_size(MPI_Comm comm, int* size);
int    MPI_Abort(MPI_Comm comm, int errorcode);
double MPI_Wtime();

// ---- Collectives -----------------------------------------------------------
int MPI_Barrier(MPI_Comm comm);
int MPI_Allreduce(const void* sendbuf, void* recvbuf, int count,
                  MPI_Datatype datatype, MPI_Op op, MPI_Comm comm);
int MPI_Reduce(const void* sendbuf, void* recvbuf, int count,
               MPI_Datatype datatype, MPI_Op op, int root, MPI_Comm comm);

// ---- Point-to-point (non-blocking emulated via deferred flush) -------------
int MPI_Irecv(void* buf, int count, MPI_Datatype datatype, int src, int tag,
              MPI_Comm comm, MPI_Request* request);
int MPI_Isend(const void* buf, int count, MPI_Datatype datatype, int dest, int tag,
              MPI_Comm comm, MPI_Request* request);
int MPI_Wait(MPI_Request* request, MPI_Status* status);
int MPI_Waitall(int count, MPI_Request* requests, MPI_Status* statuses);

#endif // LULESH_FMI_H
