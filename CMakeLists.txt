cmake_minimum_required(VERSION 3.10)

project(LULESH CXX)

option(WITH_MPI      "Build LULESH with MPI"          TRUE)
option(WITH_FMI      "Build LULESH with FMI"          FALSE)
option(WITH_FMI_CRIU "Build LULESH with FMI + CRIU rank-migration support (implies WITH_FMI)" FALSE)
option(WITH_OPENMP   "Build LULESH with OpenMP"       TRUE)
option(WITH_SILO     "Build LULESH with silo support" FALSE)

# CRIU rank migration is no longer a compile-time feature of FMI: the epoch-era
# control plane (FMI_ENABLE_CRIU, FMI_BUILD_TOOLS, fmi-rank-agent) was removed
# upstream and replaced by the neighborhood-drain protocol, which lives entirely
# in the DrainTCP backend and is turned on by the JSON config ("drain": true),
# not by a CMake option. All that is left of the distinction here is that the
# migration path needs the Redis-backed transports; keep the option as an alias
# for the FMI backend so the existing build-dir names still work.
if (WITH_FMI_CRIU)
  set(WITH_FMI ON)
endif()

# MPI and FMI are mutually exclusive communication backends. FMI wins if both
# are requested, so `cmake -DWITH_FMI=ON` works without also setting -DWITH_MPI=OFF.
if (WITH_FMI AND WITH_MPI)
  message(STATUS "WITH_FMI=ON: disabling WITH_MPI (mutually exclusive backends)")
  set(WITH_MPI OFF)
endif()

if (WITH_MPI)
  find_package(MPI REQUIRED)
  include_directories(${MPI_C_INCLUDE_PATH} ${MPI_CXX_INCLUDE_PATH})
  add_definitions("-DUSE_MPI=1")
  list(APPEND LULESH_EXTERNAL_LIBS ${MPI_C_LIBRARIES} ${MPI_CXX_LIBRARIES})
elseif (WITH_FMI)
  # The new FMI exposes exactly four options (FMI_ENABLE_S3 / _REDIS / _TCPUNCH /
  # FMI_BUILD_TESTS); FMI_ENABLE_CRIU and FMI_BUILD_TOOLS were deleted with the
  # epoch control plane and setting them here would silently do nothing.
  #   * S3      -- pulls in the AWS SDK, never needed by LULESH.
  #   * TCPunch -- the NAT rendezvous the Direct backend needs; the ranks here can
  #                reach each other directly, so DirectTCP/DrainTCP are used instead
  #                and the extern/TCPunch submodule need not even be checked out.
  #   * Redis   -- required: it is the peer registry both TCP-direct transports
  #                discover through, and DrainTCP's coordinator event stream.
  set(FMI_ENABLE_S3      OFF CACHE BOOL "" FORCE)
  set(FMI_ENABLE_TCPUNCH OFF CACHE BOOL "" FORCE)
  set(FMI_ENABLE_REDIS   ON  CACHE BOOL "" FORCE)
  set(FMI_BUILD_TESTS    OFF CACHE BOOL "" FORCE)
  add_subdirectory(extern/fmi)
  add_definitions("-DUSE_MPI=0" "-DUSE_FMI=1")
  list(APPEND LULESH_EXTERNAL_LIBS FMI::FMI)
else()
  add_definitions("-DUSE_MPI=0")
endif()

if (WITH_OPENMP)
  find_package(OpenMP REQUIRED)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()

if (WITH_SILO)
  find_path(SILO_INCLUDE_DIR silo.h
    HINTS ${SILO_DIR}/include)
  find_library(SILO_LIBRARY
    NAMES siloh5
    HINTS ${SILO_DIR}/lib)

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(SILO DEFAULT_MSG
    SILO_LIBRARY
    SILO_INCLUDE_DIR)

  if (SILO_FOUND)
    add_definitions("-DVIZ_MESH")
    include_directories(${SILO_INCLUDE_DIR})
    #   Note: silo needs to be built as a dynamic lib, otherwise 
    # there are additional dependencies (hdf5) which we don't know.
    # This would be fixed by silo providing a CMake package.
    list(APPEND LULESH_EXTERNAL_LIBS ${SILO_LIBRARY})
  endif()
endif()

set(LULESH_SOURCES
  lulesh-comm.cc
  lulesh-init.cc
  lulesh-util.cc
  lulesh-viz.cc
  lulesh.cc)

if (WITH_FMI)
  list(APPEND LULESH_SOURCES lulesh-fmi.cc)
endif()

set(LULESH_EXEC lulesh2.0)

add_executable(${LULESH_EXEC} ${LULESH_SOURCES})
target_link_libraries(${LULESH_EXEC} ${LULESH_EXTERNAL_LIBS})

if (WITH_FMI)
  # FMI headers require C++17; Boost is a private dep of FMI so its include path
  # is not propagated by FMI::FMI and must be added explicitly here.
  target_compile_features(${LULESH_EXEC} PRIVATE cxx_std_17)
  target_include_directories(${LULESH_EXEC} PRIVATE ${FMI_INCLUDE_DIRS} ${Boost_INCLUDE_DIRS})

  # The same program as one translation unit: single/lulesh.{cpp,hpp}, generated
  # from the sources above by single/generate.sh. Same flags, same golden energy.
  add_executable(lulesh2.0-single single/lulesh.cpp)
  target_link_libraries(lulesh2.0-single ${LULESH_EXTERNAL_LIBS})
  target_compile_features(lulesh2.0-single PRIVATE cxx_std_17)
  target_include_directories(lulesh2.0-single PRIVATE ${FMI_INCLUDE_DIRS} ${Boost_INCLUDE_DIRS})
endif()
