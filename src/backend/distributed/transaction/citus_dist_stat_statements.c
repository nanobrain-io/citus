/*-------------------------------------------------------------------------
 *
 * citus_dist_stat_statements.c
 *
 *	This file contains functions for monitoring the distributed transactions
 *	accross the cluster.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "postmaster/postmaster.h"

#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "distributed/backend_data.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/task_tracker.h"
#include "distributed/transaction_identifier.h"
#include "nodes/execnodes.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "storage/s_lock.h"
#include "utils/builtins.h"
#if PG_VERSION_NUM >= 100000
#include "utils/fmgrprotos.h"
#endif
#include "utils/inet.h"
#include "utils/timestamp.h"


/*
 *  citus_dist_stat_activity and citus_worker_stat_activity is similar to
 *  pg_stat_statements. Those functions are basically returns join of
 *  pg_stat_statements and get_all_active_transactions() on each node
 *  in the cluster.
 *
 *  citus_dist_stat_activity returns only the queries that are the distributed
 *  queries. citus_worker_stat_activity returns only the queries that are worker
 *  queries initiated by the distributed queries.
 *
 *  An important note on this views is that they only show the activity
 *  that are inside distributed transactions. Distributed transactions
 *  cover the following:
 *     - All multi-shard modifications (DDLs, COPY, UPDATE, DELETE, INSERT .. SELECT)
 *     - All multi-shard queries with CTEs (modifying CTEs, read-only CTEs)
 *     - All recursively planned subqueries
 *     - All queries within transaction blocks (BEGIN; query; COMMMIT;)
 *
 *  In other words, the following types of queries won't be observed in these
 *  views:
 *      - Router queries that are not inside transaction blocks
 *      - Real-time queries that are not inside transaction blocks
 *      - Task-tracker queries
 *
 *
 *  The following information for all the distributed transactions:
 *	query_host_name					text
 *	query_host_port					int
 *	database_id						oid
 *	databaese_name					name
 *	process_id						integer
 *  initiator_node_host				text
 *  initiator_node_port				int
 *	distributed_transaction_number	bigint
 *	distributed_transaction_stamp	timestamp with time zone
 *	usesysid						oid
 *	usename							name
 *	application_name                text
 *	client_addr                     inet
 *	client_hostname                 text
 *	client_port                     integer
 *	backend_start                   timestamp with time zone
 *	xact_start                      timestamp with time zone
 *	query_start                     timestamp with time zone
 *	state_change                    timestamp with time zone
 *	wait_event_type                 text
 *	wait_event                      text
 *	state                           text
 *	backend_xid                     xid
 *	backend_xmin                    xid
 *	query                           text
 *	backend_type                    text
 */

/*
 * We get CITUS_DIST_STAT_ACTIVITY_QUERY_COLS from workers and manually add
 * CITUS_DIST_STAT_ADDITIONAL_COLS for hostname and hostport. Also, instead of
 * showing the initiator_node_id we expand it to initiator_node_host and
 * initiator_node_port.
 */
#define CITUS_DIST_STAT_ACTIVITY_QUERY_COLS 23
#define CITUS_DIST_STAT_ADDITIONAL_COLS 3
#define CITUS_DIST_STAT_ACTIVITY_COLS \
	CITUS_DIST_STAT_ACTIVITY_QUERY_COLS + CITUS_DIST_STAT_ADDITIONAL_COLS


/*
 * We get the query_host_name and query_host_port while opening the connection to
 * the node. We also replace initiator_node_identifier with initiator_node_host
 * and initiator_node_port. Thus, they are not in the query below.
 *
 * Also, backend_type introduced with pg 10+.
 */

#if PG_VERSION_NUM >= 100000

	#define CITUS_DIST_STAT_ACTIVITY_QUERY \
	"\
	SELECT \
		dist_txs.database_id, \
		pg_stat_activity.datname, \
		dist_txs.process_id, \
		dist_txs.initiator_node_identifier, \
		dist_txs.transaction_number, \
		dist_txs.transaction_stamp, \
		pg_stat_activity.usesysid, \
		pg_stat_activity.usename, \
		pg_stat_activity.application_name, \
		pg_stat_activity.client_addr, \
		pg_stat_activity.client_hostname, \
		pg_stat_activity.client_port, \
		pg_stat_activity.backend_start, \
		pg_stat_activity.xact_start, \
		pg_stat_activity.query_start, \
		pg_stat_activity.state_change, \
		pg_stat_activity.wait_event_type, \
		pg_stat_activity.wait_event, \
		pg_stat_activity.state, \
		pg_stat_activity.backend_xid, \
		pg_stat_activity.backend_xmin, \
		pg_stat_activity.query, \
		pg_stat_activity.backend_type \
	FROM \
		get_all_active_transactions() AS dist_txs(database_id, process_id, initiator_node_identifier, transaction_originator, transaction_number, transaction_stamp), \
		pg_stat_activity \
	WHERE \
		pg_stat_activity.pid = dist_txs.process_id \
			AND \
		dist_txs.transaction_originator = %s; "
#else
		#define CITUS_DIST_STAT_ACTIVITY_QUERY \
	"\
	SELECT \
		dist_txs.database_id, \
		pg_stat_activity.datname, \
		dist_txs.process_id, \
		dist_txs.initiator_node_identifier, \
		dist_txs.transaction_number, \
		dist_txs.transaction_stamp, \
		pg_stat_activity.usesysid, \
		pg_stat_activity.usename, \
		pg_stat_activity.application_name, \
		pg_stat_activity.client_addr, \
		pg_stat_activity.client_hostname, \
		pg_stat_activity.client_port, \
		pg_stat_activity.backend_start, \
		pg_stat_activity.xact_start, \
		pg_stat_activity.query_start, \
		pg_stat_activity.state_change, \
		pg_stat_activity.wait_event_type, \
		pg_stat_activity.wait_event, \
		pg_stat_activity.state, \
		pg_stat_activity.backend_xid, \
		pg_stat_activity.backend_xmin, \
		pg_stat_activity.query, \
		null \
	FROM \
		get_all_active_transactions() AS dist_txs(database_id, process_id, initiator_node_identifier, transaction_originator, transaction_number, transaction_stamp), \
		pg_stat_activity \
	WHERE \
		pg_stat_activity.pid = dist_txs.process_id \
			AND \
		dist_txs.transaction_originator = %s; "

#endif

typedef struct CitusDistStat
{
	text *query_host_name;
	int query_host_port;

	/* fields from get_all_active_transactions */
	Oid database_id;
	Name databaese_name;
	int process_id;
	text *initiator_node_hostname;
	int initiator_node_port;
	uint64 distributed_transaction_number;
	TimestampTz distributed_transaction_stamp;

	/* fields from pg_stat_statement */
	Oid usesysid;
	Name usename;
	text *application_name;
	inet *client_addr;
	text *client_hostname;
	int client_port;
	TimestampTz backend_start;
	TimestampTz xact_start;
	TimestampTz query_start;
	TimestampTz state_change;
	text *wait_event_type;
	text *wait_event;
	text *state;
	TransactionId backend_xid;
	TransactionId backend_xmin;
	text *query;
	text *backend_type;
} CitusDistStat;


/* local forward declarations */
static List * CitusDistStatActivity(const char *statQuery);
static void ReturnCitusDistStats(List *citusStatsList, FunctionCallInfo fcinfo);
static CitusDistStat * ParseCitusDistStat(PGresult *result, int64 rowIndex);

/* utlity functions to parse the fields */
static text * ParseTextField(PGresult *result, int rowIndex, int colIndex);
static Name ParseNameField(PGresult *result, int rowIndex, int colIndex);
static inet * ParseInetField(PGresult *result, int rowIndex, int colIndex);
static TransactionId ParseXIDField(PGresult *result, int rowIndex, int colIndex);


PG_FUNCTION_INFO_V1(citus_dist_stat_activity);
PG_FUNCTION_INFO_V1(citus_worker_stat_activity);


/*
 * citus_dist_stat_activity connects to all nodes in the cluster and returns
 * pg_stat_activity like result set but only consisting of queries that are
 * on the distributed tables and inside distributed transactions.
 */
Datum
citus_dist_stat_activity(PG_FUNCTION_ARGS)
{
	List *citusDistStatStatements = NIL;
	StringInfo citusDistStatQuery = NULL;
	const char *transactionOriginator = "true";

	CheckCitusVersion(ERROR);

	/* set the transactionOriginator to true in the query */
	citusDistStatQuery = makeStringInfo();
	appendStringInfo(citusDistStatQuery, CITUS_DIST_STAT_ACTIVITY_QUERY,
					 transactionOriginator);

	citusDistStatStatements = CitusDistStatActivity(citusDistStatQuery->data);

	ReturnCitusDistStats(citusDistStatStatements, fcinfo);

	PG_RETURN_VOID();
}


/*
 * citus_worker_stat_activity connects to all nodes in the cluster and returns
 * pg_stat_activity like result set but only consisting of queries that are
 * on the shards of distributed tables and inside distributed transactions.
 */
Datum
citus_worker_stat_activity(PG_FUNCTION_ARGS)
{
	List *citusWorkerStatStatements = NIL;
	StringInfo cituWorkerStatQuery = NULL;
	const char *transactionOriginator = "false";

	CheckCitusVersion(ERROR);

	/* set the transactionOriginator to true in the query */
	cituWorkerStatQuery = makeStringInfo();
	appendStringInfo(cituWorkerStatQuery, CITUS_DIST_STAT_ACTIVITY_QUERY,
					 transactionOriginator);

	citusWorkerStatStatements = CitusDistStatActivity(cituWorkerStatQuery->data);

	ReturnCitusDistStats(citusWorkerStatStatements, fcinfo);

	PG_RETURN_VOID();
}


/*
 * CitusDistStatActivity gets the stats query, connects to each node in the
 * cluster, executes the query and parses the results. The function returns
 * list of CitusDistStat struct for further processing.
 *
 * The function connects to each active primary node in the pg_dist_node. Plus,
 * if the query is being executed on the coordinator, the function connects to
 * localhost as well. The implication of this is that whenever the query is executed
 * from a MX worker node, it wouldn't be able to get information from the queries
 * executed on the coordinator given that there is not metadata information about that.
 */
static List *
CitusDistStatActivity(const char *statQuery)
{
	List *citusStatsList = NIL;

	List *workerNodeList = ActivePrimaryNodeList();
	ListCell *workerNodeCell = NULL;
	char *nodeUser = NULL;
	List *connectionList = NIL;
	ListCell *connectionCell = NULL;

	/*
	 * We prefer to connect with the current user. This will
	 * ensure that we have the same privilage restrictions
	 * that pg_stat_activity enforces.
	 */
	nodeUser = CurrentUserName();

	/* open connections in parallel */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		MultiConnection *connection = NULL;
		int connectionFlags = 0;

		connection = StartNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
													 nodeUser, NULL);

		connectionList = lappend(connectionList, connection);
	}

	/*
	 * Coordinator's nodename and nodeport doesn't show-up in the metadata,
	 * so connect locally when executing from the coordinator.
	 */
	if (IsCoordinator())
	{
		char *nodeName = LOCAL_HOST_NAME;
		int nodePort = PostPortNumber;
		MultiConnection *connection = NULL;
		int connectionFlags = 0;

		connection = StartNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
													 nodeUser, NULL);

		connectionList = lappend(connectionList, connection);
	}

	FinishConnectionListEstablishment(connectionList);


	/* send commands in parallel */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		int querySent = false;

		querySent = SendRemoteCommand(connection, statQuery);
		if (querySent == 0)
		{
			ReportConnectionError(connection, WARNING);
		}
	}

	/* receive query results */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		PGresult *result = NULL;
		bool raiseInterrupts = true;
		int64 rowIndex = 0;
		int64 rowCount = 0;
		int64 colCount = 0;

		result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, WARNING);
			continue;
		}

		rowCount = PQntuples(result);
		colCount = PQnfields(result);

		if (colCount != CITUS_DIST_STAT_ACTIVITY_QUERY_COLS)
		{
			/*
			 * We don't expect to hit this error, but keep it here in case there
			 * is a version mistmatch.
			 */
			ereport(WARNING, (errmsg("unexpected number of columns from "
									 "citus stat query")));
			continue;
		}

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			CitusDistStat *citusDistStat = ParseCitusDistStat(result, rowIndex);

			/*
			 * Add the query_host_name and query_host_port which denote where
			 * the query is being running.
			 */
			citusDistStat->query_host_name = cstring_to_text(connection->hostname);
			citusDistStat->query_host_port = connection->port;

			citusStatsList = lappend(citusStatsList, citusDistStat);
		}

		PQclear(result);
		ForgetResults(connection);
	}

	return citusStatsList;
}


/*
 * ParseCitusDistStat is a helper function which basically gets a PGresult
 * and parses the results for rowIndex. Finally, returns CitusDistStat for
 * further processing of the data retrieved.
 */
static CitusDistStat *
ParseCitusDistStat(PGresult *result, int64 rowIndex)
{
	CitusDistStat *citusDistStat = (CitusDistStat *) palloc0(sizeof(CitusDistStat));
	int initiator_node_identifier = 0;
	WorkerNode *initiatorWorkerNode = NULL;

	/* fields from get_all_active_transactions */
	citusDistStat->database_id = ParseIntField(result, rowIndex, 0);
	citusDistStat->databaese_name = ParseNameField(result, rowIndex, 1);
	citusDistStat->process_id = ParseIntField(result, rowIndex, 2);

	/*
	 * Replace initiator_node_identifier with initiator_node_hostname
	 * and initiator_node_port given that those are a lot more useful.
	 *
	 * The rules are following:
	 *    - If initiator_node_identifier belongs to a worker, simply get it
	 *      from the metadata
	 *   - If the initiator_node_identifier belongs to the coordinator and
	 *     we're executing the function on the coordinator, get the localhost
	 *     and port
	 *   - If the initiator_node_identifier belongs to the coordinator and
	 *     we're executing the function on the coordinator, manually mark it
	 *     as "coordinator_host" given that we cannot know the host and port
	 */
	initiator_node_identifier = ParseIntField(result, rowIndex, 3);
	if (initiator_node_identifier != 0)
	{
		bool nodeExists = false;

		initiatorWorkerNode = PrimaryNodeForGroup(initiator_node_identifier, &nodeExists);

		/* a query should run on an existing node */
		Assert(nodeExists);
		citusDistStat->initiator_node_hostname =
			cstring_to_text(initiatorWorkerNode->workerName);
		citusDistStat->initiator_node_port = initiatorWorkerNode->workerPort;
	}
	else if (initiator_node_identifier == 0 && IsCoordinator())
	{
		citusDistStat->initiator_node_hostname = cstring_to_text(LOCAL_HOST_NAME);
		citusDistStat->initiator_node_port = PostPortNumber;
	}
	else
	{
		/*
		 * We could only get here if the function is called from metadata workers and
		 * the query is initiated from the coordinator.
		 */
		citusDistStat->initiator_node_hostname = cstring_to_text("coordinator_host");
		citusDistStat->initiator_node_port = 0;
	}

	citusDistStat->distributed_transaction_number = ParseIntField(result, rowIndex, 4);
	citusDistStat->distributed_transaction_stamp =
		ParseTimestampTzField(result, rowIndex, 5);

	/* fields from pg_stat_statement */
	citusDistStat->usesysid = ParseIntField(result, rowIndex, 6);
	citusDistStat->usename = ParseNameField(result, rowIndex, 7);
	citusDistStat->application_name = ParseTextField(result, rowIndex, 8);
	citusDistStat->client_addr = ParseInetField(result, rowIndex, 9);
	citusDistStat->client_hostname = ParseTextField(result, rowIndex, 10);
	citusDistStat->client_port = ParseIntField(result, rowIndex, 11);
	citusDistStat->backend_start = ParseTimestampTzField(result, rowIndex, 12);
	citusDistStat->xact_start = ParseTimestampTzField(result, rowIndex, 13);
	citusDistStat->query_start = ParseTimestampTzField(result, rowIndex, 14);
	citusDistStat->state_change = ParseTimestampTzField(result, rowIndex, 15);
	citusDistStat->wait_event_type = ParseTextField(result, rowIndex, 16);
	citusDistStat->wait_event = ParseTextField(result, rowIndex, 17);
	citusDistStat->state = ParseTextField(result, rowIndex, 18);
	citusDistStat->backend_xid = ParseXIDField(result, rowIndex, 19);
	citusDistStat->backend_xmin = ParseXIDField(result, rowIndex, 20);
	citusDistStat->query = ParseTextField(result, rowIndex, 21);
	citusDistStat->backend_type = ParseTextField(result, rowIndex, 22);

	return citusDistStat;
}


/*
 * ParseTextField parses a text from a remote result or returns NULL if the
 * result is NULL.
 */
static text *
ParseTextField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;
	Datum resultStringDatum = 0;
	Datum textDatum = 0;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return NULL;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);
	resultStringDatum = CStringGetDatum(resultString);
	textDatum = DirectFunctionCall1(textin, resultStringDatum);

	return (text *) DatumGetPointer(textDatum);
}


/*
 * ParseNameField parses a name from a remote result or returns NULL if the
 * result is NULL.
 */
static Name
ParseNameField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;
	Datum resultStringDatum = 0;
	Datum nameDatum = 0;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return (Name) nameDatum;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);
	resultStringDatum = CStringGetDatum(resultString);
	nameDatum = DirectFunctionCall1(namein, resultStringDatum);

	return (Name) DatumGetPointer(nameDatum);
}


/*
 * ParseInetField parses an inet from a remote result or returns NULL if the
 * result is NULL.
 */
static inet *
ParseInetField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;
	Datum resultStringDatum = 0;
	Datum inetDatum = 0;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return NULL;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);
	resultStringDatum = CStringGetDatum(resultString);
	inetDatum = DirectFunctionCall1(inet_in, resultStringDatum);

	return DatumGetInetP(inetDatum);
}


/*
 * ParseXIDField parses an XID from a remote result or returns 0 if the
 * result is NULL.
 */
static TransactionId
ParseXIDField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;
	Datum resultStringDatum = 0;
	Datum XIDDatum = 0;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return 0;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);
	resultStringDatum = CStringGetDatum(resultString);
	XIDDatum = DirectFunctionCall1(xidin, resultStringDatum);

	return DatumGetTransactionId(XIDDatum);
}


/*
 * ReturnCitusDistStats returns the stats for a set returning function.
 */
static void
ReturnCitusDistStats(List *citusStatsList, FunctionCallInfo fcinfo)
{
	ReturnSetInfo *resultInfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc tupleDesc = NULL;
	Tuplestorestate *tupleStore = NULL;
	MemoryContext per_query_ctx = NULL;
	MemoryContext oldContext = NULL;

	ListCell *citusStatsCell = NULL;

	/* check to see if caller supports us returning a tuplestore */
	if (resultInfo == NULL || !IsA(resultInfo, ReturnSetInfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "set-valued function called in context that cannot accept a set")));
	}
	if (!(resultInfo->allowedModes & SFRM_Materialize))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));
	}

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
	{
		elog(ERROR, "return type must be a row type");
	}

	per_query_ctx = resultInfo->econtext->ecxt_per_query_memory;
	oldContext = MemoryContextSwitchTo(per_query_ctx);

	tupleStore = tuplestore_begin_heap(true, false, work_mem);
	resultInfo->returnMode = SFRM_Materialize;
	resultInfo->setResult = tupleStore;
	resultInfo->setDesc = tupleDesc;
	MemoryContextSwitchTo(oldContext);

	foreach(citusStatsCell, citusStatsList)
	{
		CitusDistStat *citusDistStat = (CitusDistStat *) lfirst(citusStatsCell);

		Datum values[CITUS_DIST_STAT_ACTIVITY_COLS];
		bool nulls[CITUS_DIST_STAT_ACTIVITY_COLS];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = PointerGetDatum(citusDistStat->query_host_name);
		values[1] = Int32GetDatum(citusDistStat->query_host_port);
		values[2] = ObjectIdGetDatum(citusDistStat->database_id);

		if (citusDistStat->databaese_name != NULL)
		{
			values[3] = CStringGetDatum(NameStr(*citusDistStat->databaese_name));
		}
		else
		{
			nulls[3] = true;
		}

		values[4] = Int32GetDatum(citusDistStat->process_id);

		if (citusDistStat->initiator_node_hostname != NULL)
		{
			values[5] = PointerGetDatum(citusDistStat->initiator_node_hostname);
		}
		else
		{
			nulls[5] = true;
		}

		values[6] = Int32GetDatum(citusDistStat->initiator_node_port);
		values[7] = UInt64GetDatum(citusDistStat->distributed_transaction_number);
		values[8] = TimestampTzGetDatum(citusDistStat->distributed_transaction_stamp);
		values[9] = ObjectIdGetDatum(citusDistStat->usesysid);

		if (citusDistStat->usename != NULL)
		{
			values[10] = CStringGetDatum(NameStr(*citusDistStat->usename));
		}
		else
		{
			nulls[10] = true;
		}

		if (citusDistStat->application_name != NULL)
		{
			values[11] = PointerGetDatum(citusDistStat->application_name);
		}
		else
		{
			nulls[11] = true;
		}

		if (citusDistStat->client_addr != NULL)
		{
			values[12] = InetPGetDatum(citusDistStat->client_addr);
		}
		else
		{
			nulls[12] = true;
		}

		if (citusDistStat->client_hostname != NULL)
		{
			values[13] = PointerGetDatum(citusDistStat->client_hostname);
		}
		else
		{
			nulls[13] = true;
		}

		values[14] = Int32GetDatum(citusDistStat->client_port);
		values[15] = TimestampTzGetDatum(citusDistStat->backend_start);
		values[16] = TimestampTzGetDatum(citusDistStat->xact_start);
		values[17] = TimestampTzGetDatum(citusDistStat->query_start);
		values[18] = TimestampTzGetDatum(citusDistStat->state_change);

		if (citusDistStat->wait_event_type != NULL)
		{
			values[19] = PointerGetDatum(citusDistStat->wait_event_type);
		}
		else
		{
			nulls[19] = true;
		}

		if (citusDistStat->wait_event != NULL)
		{
			values[20] = PointerGetDatum(citusDistStat->wait_event);
		}
		else
		{
			nulls[20] = true;
		}

		if (citusDistStat->state != NULL)
		{
			values[21] = PointerGetDatum(citusDistStat->state);
		}
		else
		{
			nulls[21] = true;
		}

		values[22] = TransactionIdGetDatum(citusDistStat->backend_xid);
		values[23] = TransactionIdGetDatum(citusDistStat->backend_xmin);

		if (citusDistStat->query != NULL)
		{
			values[24] = PointerGetDatum(citusDistStat->query);
		}
		else
		{
			nulls[24] = true;
		}

		if (citusDistStat->backend_type != NULL)
		{
			values[25] = PointerGetDatum(citusDistStat->backend_type);
		}
		else
		{
			nulls[25] = true;
		}

		tuplestore_putvalues(tupleStore, tupleDesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupleStore);
}
