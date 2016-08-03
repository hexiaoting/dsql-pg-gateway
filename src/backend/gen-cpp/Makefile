#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for gen-cpp
#
# IDENTIFICATION
#    src/backend/gen-cpp/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/gen-cpp
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS += -O0 -g -I$(dsql_home)/toolchain/thrift-0.9.0-p2/include -I$(dsql_home)/toolchain/boost-1.57.0/include -DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H
OBJS= TCLIService_types.o ErrorCodes_types.o  ExecStats_types.o\
			ImpalaService.o ImpalaService_types.o beeswax_constants.o fb303_constants.o BeeswaxService.o fb303_types.o \
      hive_metastore_constants.o beeswax_types.o hive_metastore_types.o \
      FacebookService.o ThriftHiveMetastore.o client.o
ifneq (,$(filter $(PORTNAME),cygwin win32))
override CPPFLAGS += -DWIN32_STACK_RLIMIT=$(WIN32_STACK_RLIMIT)
endif

include $(top_srcdir)/src/backend/common.mk
