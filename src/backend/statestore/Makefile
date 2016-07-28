#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for statestore
#
# IDENTIFICATION
#    src/backend/statestore/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/statestore
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS += -O0 -g -I$(top_builddir)/src/interfaces/libpq/  -I$(dsql_home)/toolchain/thrift-0.9.0-p2/include -I$(dsql_home)/toolchain/boost-1.57.0/include -DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H

OBJS= Exprs_types.o CatalogObjects_types.o Status_types.o Types_types.o thrift-server.o \
			StatestoreService.o StatestoreService_types.o StatestoreSubscriber.o \
			StatestoreClient.o
ifneq (,$(filter $(PORTNAME),cygwin win32))
override CPPFLAGS += -DWIN32_STACK_RLIMIT=$(WIN32_STACK_RLIMIT) -std=c++0x
endif

include $(top_srcdir)/src/backend/common.mk
