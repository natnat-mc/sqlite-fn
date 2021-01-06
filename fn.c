#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <alloca.h>

#ifndef DEBUG
#define DEBUG 0
#endif
#define debug DEBUG

#define fail(ctx, msg) do{ sqlite3_result_error(ctx, msg, -1); if(debug) fprintf(stderr, "%s, %d: %s: %s\n", __func__, __LINE__, msg, sqlite3_errmsg(sqlite3_context_db_handle(ctx))); return; } while(0)
#define typecheck(ctx, arg, type, msg) do { if(sqlite3_value_type(arg) != type) fail(ctx, msg); } while(0)

/**
 * global utilities
 */
static int _getflags(const char* str) {
	int flags = SQLITE_UTF8;
	for(char const* c = str; *c; c++) switch(*c) {
		case 'd': flags |= SQLITE_DETERMINISTIC; break;
		case 'D': flags |= SQLITE_DIRECTONLY; break;
		case 'i': flags |= SQLITE_INNOCUOUS; break;
	}
	return flags;
}
static char* strdup(const char* str) {
	if(!str) return NULL;
	int len = strlen(str);
	char* dup = malloc(len+1);
	if(!dup) return NULL;
	strcpy(dup, str);
	return dup;
}

/**
 * normal functions
 */
typedef struct sql_fn {
	/* function itself */
	char* code;
	int argc;
	
	/* compiled cache */
	int cachec; /* cache entries */
	int cacher; /* cache capacity */
	sqlite3_stmt** cachev; /* compiled statement */
	char* cacheu; /* used? */
} sql_fn;

/**
 * utility functions
 */
static sql_fn* fn_create(const char* code, int argc) { /* create function */
	sql_fn* fn = malloc(sizeof(sql_fn));
	if(!fn) return NULL;
	char* code_dup = strdup(code);
	if(!code_dup) {
		free(fn);
		return NULL;
	}
	fn->code = code_dup;
	fn->argc = argc;
	fn->cachec = 0;
	fn->cacher = 0;
	fn->cachev = NULL;
	return fn;
}

static void fn_delete(sql_fn* fn) { /* delete (free) function */
	free(fn->code);
	fn->code = NULL;
	for(int i=0; i<fn->cachec; i++) sqlite3_finalize(fn->cachev[i]);
	fn->cachec = 0;
	fn->cacher = 0;
	free(fn->cachev);
	fn->cachev = NULL;
	free(fn);
}

static sqlite3_stmt* fn_stmt(sql_fn* fn, sqlite3* db) { /* get statement for function */
	if(!fn || !fn->code) return NULL;
	if(fn->cacher == 0) {
		fn->cachev = malloc(sizeof(sqlite3_stmt*));
		if(!fn->cachev) return NULL;
		fn->cacher = 1;
	}
	for(int i=0; i<fn->cachec; i++) {
		if(!sqlite3_stmt_busy(fn->cachev[i])) return fn->cachev[i];
	}
	if(fn->cachec == fn->cacher) {
		void* _cachev = realloc(fn->cachev, sizeof(sqlite3_stmt*)*2*fn->cacher);
		if(!_cachev) return NULL;
		fn->cachev = _cachev;
		fn->cacher *= 2;
	}
	sqlite3_stmt* stmt;
	if(sqlite3_prepare_v2(db, fn->code, -1, &stmt, NULL) != SQLITE_OK) return NULL;
	fn->cachev[fn->cachec++] = stmt;
	return stmt;
}

/**
 * created function body
 */
static void sql_function(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
	/* get stuff */
	sqlite3* db = sqlite3_context_db_handle(ctx);
	sql_fn* fn = sqlite3_user_data(ctx);
	if(argc != fn->argc) fail(ctx, "Wrong number of arguments");
	sqlite3_stmt* stmt = fn_stmt(fn, db);
	if(!stmt) fail(ctx, "Failed to allocate a statement");

	/* do stuff */
	for(int i=1; i<=fn->argc; i++) if(sqlite3_bind_value(stmt, i, argv[i-1]) != SQLITE_OK) fail(ctx, "Failed to bind argument");
	if(sqlite3_step(stmt) != SQLITE_ROW) {
		sqlite3_reset(stmt);
		fail(ctx, "Did not return a row");
	}
	sqlite3_value* rst = sqlite3_column_value(stmt, 0);
	if(!rst) fail(ctx, "No result value");
	if(debug) switch(sqlite3_column_type(stmt, 0)) {
		case SQLITE_INTEGER: printf("->int\n"); break;
		case SQLITE_FLOAT: printf("->float\n"); break;
		case SQLITE_TEXT: printf("->text\n"); break;
		case SQLITE_BLOB: printf("->blob\n"); break;
		case SQLITE_NULL: printf("->null\n"); break;
		default: printf("->what?\n");
	}
	sqlite3_result_value(ctx, rst);
	sqlite3_reset(stmt);
}

/**
 * created vararg
 */
static void sql_vararg(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
	/* special cases */
	if(argc == 0) {
		sqlite3_result_null(ctx);
		return;
	} else if(argc == 1) {
		sqlite3_result_value(ctx, argv[0]);
		return;
	}

	/* get stuff */
	sqlite3* db = sqlite3_context_db_handle(ctx);
	sql_fn* fn = sqlite3_user_data(ctx);
	sqlite3_stmt* stmt = fn_stmt(fn, db);
	if(!stmt) fail(ctx, "Failed to allocate a statement");

	/* do stuff */
	sqlite3_value* acc = sqlite3_value_dup(argv[0]);
	if(!acc) fail(ctx, "Failed to duplicate value");
	for(int i=1; i<argc; i++) {
		if(sqlite3_bind_value(stmt, 1, acc) != SQLITE_OK) {
			sqlite3_value_free(acc);
			fail(ctx, "Failed to bind accumulator");
		}
		if(sqlite3_bind_value(stmt, 2, argv[i]) != SQLITE_OK) {
			sqlite3_value_free(acc);
			fail(ctx, "Failed to bind current");
		}
		if(sqlite3_step(stmt) != SQLITE_ROW) {
			sqlite3_value_free(acc);
			sqlite3_reset(stmt);
			fail(ctx, "Did not return a row");
		}
		sqlite3_value_free(acc);
		acc = sqlite3_value_dup(sqlite3_column_value(stmt, 0));
		sqlite3_reset(stmt);
		if(!acc) fail(ctx, "Failed to duplicate value");
	}
	sqlite3_result_value(ctx, acc);
	sqlite3_value_free(acc);
}

/**
 * created reducer step
 */
static void sql_reducer_step(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
	/* validation */
	if(argc != 1) fail(ctx, "Accumulator reducer step takes one argument");

	/* get stuff */
	sqlite3* db = sqlite3_context_db_handle(ctx);
	sql_fn* fn = sqlite3_user_data(ctx);
	sqlite3_value** accp = sqlite3_aggregate_context(ctx, sizeof(sqlite3_value*));
	if(!accp) fail(ctx, "Failed to allocate aggregate context");
	
	/* special case */
	if(!*accp) {
		*accp = sqlite3_value_dup(argv[0]);
		if(!*accp) fail(ctx, "Failed to duplicate value");
	}

	/* do stuff */
	sqlite3_stmt* stmt = fn_stmt(fn, db);
	if(!stmt) fail(ctx, "Failed to allocate a statement");
	if(sqlite3_bind_value(stmt, 1, *accp) != SQLITE_OK) fail(ctx, "Failed to bind accumulator");
	sqlite3_value_free(*accp);
	*accp = NULL;
	if(sqlite3_bind_value(stmt, 2, argv[0]) != SQLITE_OK) fail(ctx, "Failed to bind current");
	if(sqlite3_step(stmt) != SQLITE_ROW) {
		sqlite3_reset(stmt);
		fail(ctx, "Did not return a row");
	}
	*accp = sqlite3_value_dup(sqlite3_column_value(stmt, 0));
	if(!*accp) fail(ctx, "Failed to duplicate value");
}

/**
 * created reducer final
 */
static void sql_reducer_final(sqlite3_context* ctx) {
	sqlite3_value** accp = sqlite3_aggregate_context(ctx, 0);
	if(!accp || !*accp) {
		sqlite3_result_null(ctx);
	} else {
		sqlite3_result_value(ctx, *accp);
		sqlite3_value_free(*accp);
	}
}

/**
 * CREATE_FUNCTION(name, nargs, flags, code)
 */
static void create_function(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
	/* get arguments */
	if(argc != 4) fail(ctx, "Wrong number of arguments");
	typecheck(ctx, argv[0], SQLITE_TEXT, "Function name must be TEXT");
	typecheck(ctx, argv[1], SQLITE_INTEGER, "Function argc must be TEXT");
	typecheck(ctx, argv[2], SQLITE_TEXT, "Function flags must be TEXT");
	typecheck(ctx, argv[3], SQLITE_TEXT, "Function code must be TEXT");
	sqlite3* fn_db = sqlite3_context_db_handle(ctx);
	const char* fn_name = (const char*) sqlite3_value_text(argv[0]);
	const int fn_argc = sqlite3_value_int(argv[1]);
	const char* fn_flags_s = (const char*) sqlite3_value_text(argv[2]);
	const char* fn_code_s = (const char*) sqlite3_value_text(argv[3]);
	if(fn_argc < 0 || fn_argc > 127) fail(ctx, "Invalid number of arguments for user function");

	/* get flags */
	int fn_flags = _getflags(fn_flags_s);

	/* create argstr */
	char* argstr = alloca(5*fn_argc); /* a tad less in practice */
	{
		char* _argstr = argstr;
		for(int i=1; i<=fn_argc; i++) {
			if(i!=1) _argstr += sprintf(_argstr, ",");
			_argstr += sprintf(_argstr, "a%d", i);
		}
	}

	/* create valuestr */
	char* valuestr = alloca(2*fn_argc);
	for(int i=0; i<2*fn_argc-1; i++) valuestr[i] = (i%2) ? ',' : '?';
	valuestr[2*fn_argc-1] = '\0';

	/* create function code */
	char* fn_code = alloca(strlen(fn_code_s) + strlen(argstr) + strlen(valuestr) + 46);
	sprintf(fn_code, "WITH a(%s) AS (VALUES(%s)) SELECT (%s) AS r FROM a", argstr, valuestr, fn_code_s);

	if(debug) printf("Creating function %s with body %s\n", fn_name, fn_code);

	/* create function struct */
	sql_fn* fn = fn_create(fn_code, fn_argc);
	if(!fn) fail(ctx, "Out of memory");

	/* create function */
	if(sqlite3_create_function_v2(fn_db, fn_name, fn_argc, fn_flags, fn, sql_function, NULL, NULL, (void (*)(void*)) fn_delete) != SQLITE_OK) fail(ctx, "Failed to create function");
	sqlite3_result_null(ctx);
}

/**
 * CREATE_FUNCTION_V2(name, flags, code, ...args)
 */
static void create_function_v2(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
	/* get arguments */
	if(argc < 3) fail(ctx, "Not enough arguments");
	typecheck(ctx, argv[0], SQLITE_TEXT, "Function name must be TEXT");
	typecheck(ctx, argv[1], SQLITE_TEXT, "Function flags must be TEXT");
	typecheck(ctx, argv[2], SQLITE_TEXT, "Function code must be TEXT");
	for(int i=3; i<argc; i++) typecheck(ctx, argv[i], SQLITE_TEXT, "Function argument names must be TEXT");
	sqlite3* fn_db = sqlite3_context_db_handle(ctx);
	const char* fn_name = (const char*) sqlite3_value_text(argv[0]);
	const int fn_argc = argc-3;
	const char* fn_flags_s = (const char*) sqlite3_value_text(argv[1]);
	const char* fn_code_s = (const char*) sqlite3_value_text(argv[2]);

	/* get flags */
	int fn_flags = _getflags(fn_flags_s);

	/* create argstr */
	int argstrlen = 0;
	for(int i=3; i<argc; i++) argstrlen += 1 + strlen((const char*) sqlite3_value_text(argv[i]));
	char* argstr = alloca(argstrlen);
	{
		char* _argstr = argstr;
		for(int i=3; i<argc; i++) {
			if(i!=3) _argstr += sprintf(_argstr, ",");
			_argstr += sprintf(_argstr, "%s", sqlite3_value_text(argv[i]));
		}
	}

	/* create valuestr */
	char* valuestr = alloca(2*fn_argc);
	for(int i=0; i<2*fn_argc-1; i++) valuestr[i] = (i%2) ? ',' : '?';
	valuestr[2*fn_argc-1] = '\0';

	/* create function code */
	char* fn_code = alloca(strlen(fn_code_s) + strlen(argstr) + strlen(valuestr) + 46);
	sprintf(fn_code, "WITH a(%s) AS (VALUES(%s)) SELECT (%s) AS r FROM a", argstr, valuestr, fn_code_s);
	if(debug) printf("Creating function %s with body %s\n", fn_name, fn_code);

	/* create function struct */
	sql_fn* fn = fn_create(fn_code, fn_argc);
	if(!fn) fail(ctx, "Out of memory");

	/* create function */
	if(sqlite3_create_function_v2(fn_db, fn_name, fn_argc, fn_flags, fn, sql_function, NULL, NULL, (void (*)(void*)) fn_delete) != SQLITE_OK) fail(ctx, "Failed to create function");
	sqlite3_result_null(ctx);
}

/**
 * CREATE_REDUCER(name, flags, code[, accname, currname])
 */
static void create_reducer(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
	/* get arguments */
	if(argc != 3 && argc != 5) fail(ctx, "Must use either 3 or 5 arguments");
	typecheck(ctx, argv[0], SQLITE_TEXT, "Reducer name must be TEXT");
	typecheck(ctx, argv[1], SQLITE_TEXT, "Reducer flags must be TEXT");
	typecheck(ctx, argv[2], SQLITE_TEXT, "Reducer code must be TEXT");
	if(argc == 5) {
		typecheck(ctx, argv[3], SQLITE_TEXT, "Reducer accumulator name must be TEXT");
		typecheck(ctx, argv[4], SQLITE_TEXT, "Reducer current name must be TEXT");
	}
	sqlite3* red_db = sqlite3_context_db_handle(ctx);
	const char* red_name = (const char*) sqlite3_value_text(argv[0]);
	const char* red_flags_s = (const char*) sqlite3_value_text(argv[1]);
	const char* red_code_s = (const char*) sqlite3_value_text(argv[2]);
	const char* red_accname = (argc == 5) ? (const char*) sqlite3_value_text(argv[3]) : "acc";
	const char* red_currname = (argc == 5) ? (const char*) sqlite3_value_text(argv[4]) : "curr";

	/* get flags */
	int red_flags = _getflags(red_flags_s);

	/* create function code */
	char* red_code = alloca(strlen(red_code_s) + 54);
	sprintf(red_code, "WITH a(%s,%s) AS (VALUES(?,?)) SELECT (%s) AS r FROM a", red_accname, red_currname, red_code_s);
	if(debug) printf("Creating reducer %s with body %s\n", red_name, red_code);

	/* create function struct */
	sql_fn* fn = fn_create(red_code, 2);
	if(!fn) fail(ctx, "Out of memory");

	/* create vararg */
	if(sqlite3_create_function_v2(red_db, red_name, -1, red_flags, fn, sql_vararg, NULL, NULL, (void (*)(void*)) fn_delete) != SQLITE_OK) fail(ctx, "Failed to create vararg");
	if(sqlite3_create_function_v2(red_db, red_name, 1, red_flags, fn, NULL, sql_reducer_step, sql_reducer_final, (void (*)(void*)) fn_delete) != SQLITE_OK) fail(ctx, "Failed to create aggregate");
	sqlite3_result_null(ctx);
}

/**
 * entry point
 */
#define try(x) do{ int ret = x; if(ret != SQLITE_OK) return ret; } while(0)

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_fn_init(sqlite3 *db, char** errMsg, const sqlite3_api_routines *api) {
	(void)(errMsg); /* useless param */
	SQLITE_EXTENSION_INIT2(api);

	try(sqlite3_create_function_v2(db, "create_function", 4, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, create_function, NULL, NULL, NULL));
	try(sqlite3_create_function_v2(db, "create_function_v2", -1, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, create_function_v2, NULL, NULL, NULL));
	try(sqlite3_create_function_v2(db, "create_reducer", -1, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, create_reducer, NULL, NULL, NULL));

	return SQLITE_OK;
}
