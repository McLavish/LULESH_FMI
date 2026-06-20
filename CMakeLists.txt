cmake_minimum_required(VERSION 3.10)

project(LULESH CXX)

option(WITH_MPI    "Build LULESH with MPI"          TRUE)
option(WITH_FMI    "Build LULESH with FMI"          FALSE)
option(WITH_OPENMP "Build LULESH with OpenMP"       TRUE)
option(WITH_SILO   "Build LULESH with silo support" FALSE)

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
  # Minimal FMI: Direct (TCP) backend only, no S3/Redis/CRIU, so the only extra
  # deps are Boost + ZLIB + TCPunch. The migration demo will re-enable Redis/CRIU later.
  set(FMI_ENABLE_S3    OFF CACHE BOOL "" FORCE)
  set(FMI_ENABLE_REDIS OFF CACHE BOOL "" FORCE)
  set(FMI_ENABLE_CRIU  OFF CACHE BOOL "" FORCE)
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
endif()
