/* -------------------------------------------------------------------------
 *
 * worker_spi.c
 *		Sample background worker code that demonstrates various coding
 *		patterns: establishing a database connection; starting and committing
 *		transactions; using GUC variables, and heeding SIGHUP to reread
 *		the configuration file; reporting to pg_stat_activity; using the
 *		process latch to sleep and exit in case of postmaster death.
 *
 * This code connects to a database, creates a schema and table, and summarizes
 * the numbers contained therein.  To see it working, insert an initial value
 * with "total" type and some initial value; then insert some other rows with
 * "delta" type.  Delta rows will be deleted by this worker and their values
 * aggregated into the total.
 *
 * Copyright (C) 2013-2014, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/worker_spi/worker_spi.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "access/xact.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "tcop/utility.h"
#include <stdlib.h>

void start_subscribe(int pg_port, int ss_port, int hb_port, char *ss_host);

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(worker_spi_launch);

void		_PG_init(void);
void		worker_spi_main(Datum) pg_attribute_noreturn();

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int	worker_spi_naptime = 10;

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
worker_spi_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
  exit(1);
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
worker_spi_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Initialize workspace for a worker process: create the schema if it doesn't
 * already exist.
 */
static void
initialize_worker_spi()
{
	int			ret;
	int			ntup;
	bool		isnull;
	StringInfoData buf;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());
	pgstat_report_activity(STATE_RUNNING, "initializing spi_worker schema");

	/* XXX could we use CREATE SCHEMA IF NOT EXISTS? */
	initStringInfo(&buf);
	appendStringInfo(&buf, "select count(*) from pg_namespace ");

	ret = SPI_execute(buf.data, true, 0);
	if (ret != SPI_OK_SELECT)
		elog(FATAL, "SPI_execute failed: error code %d", ret);

	if (SPI_processed != 1)
		elog(FATAL, "not a singleton result");

	ntup = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
									   SPI_tuptable->tupdesc,
									   1, &isnull));
	if (isnull)
		elog(FATAL, "null result");

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	pgstat_report_activity(STATE_IDLE, NULL);
}

void
worker_spi_main(Datum main_arg)
{
	StringInfoData buf;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, worker_spi_sighup);
	pqsignal(SIGTERM, worker_spi_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	initialize_worker_spi();

	initStringInfo(&buf);

  int pg_port = atoi((char *)GetConfigOption("port", false, false));
  int statestored_port = atoi((char *)GetConfigOption("statestored_port", false, false));
  int heartbeat_port = atoi((char *)GetConfigOption("heartbeat_port", false, false));
  char *ss_host = GetConfigOption("statestored_host", false, false);
  start_subscribe(pg_port, statestored_port, heartbeat_port, ss_host);

	while (!got_sigterm)
	{
		int			rc;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   worker_spi_naptime * 1000L);
		ResetLatch(MyLatch);

    if (rc & WL_POSTMASTER_DEATH) {
      proc_exit(1);
    }

    if (got_sighup)
    {
      got_sighup = false;
      ProcessConfigFile(PGC_SIGHUP);
    }

	}

	proc_exit(1);
}

/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	/* set up common data for all our workers */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = worker_spi_main;
	worker.bgw_notify_pid = 0;

	/*
	 * Now fill in worker-specific data, and do the actual registrations.
	 */
  snprintf(worker.bgw_name, BGW_MAXLEN, "worker meta sync");
  worker.bgw_main_arg = NULL;

  RegisterBackgroundWorker(&worker);
}

/*
 * Dynamically launch an SPI worker.
 */
Datum
worker_spi_launch(PG_FUNCTION_ARGS)
{
	int32		i = PG_GETARG_INT32(0);
	BackgroundWorker worker;
	BackgroundWorkerHandle *handle;
	BgwHandleStatus status;
	pid_t		pid;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = NULL;		/* new worker might not have library loaded */
	sprintf(worker.bgw_library_name, "worker_spi");
	sprintf(worker.bgw_function_name, "worker_spi_main");
	snprintf(worker.bgw_name, BGW_MAXLEN, "worker %d", i);
	worker.bgw_main_arg = Int32GetDatum(i);
	/* set bgw_notify_pid so that we can use WaitForBackgroundWorkerStartup */
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		PG_RETURN_NULL();

	status = WaitForBackgroundWorkerStartup(handle, &pid);

	if (status == BGWH_STOPPED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not start background process"),
			   errhint("More details may be available in the server log.")));
	if (status == BGWH_POSTMASTER_DIED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
			  errmsg("cannot start background processes without postmaster"),
				 errhint("Kill all remaining database processes and restart the database.")));
	Assert(status == BGWH_STARTED);

	PG_RETURN_INT32(pid);
}
