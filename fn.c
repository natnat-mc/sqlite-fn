#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#ifndef DEBUG
#define DEBUG 0
#endif
#define debug DEBUG

#define fail(ctx, msg) do{ sqlite3_result_error(ctx, msg, -1); if(debug) fprintf(stderr, "%s, %d: %s: %s\n", __func__, __LINE__, msg, sqlite3_errmsg(sqlite3_context_db_handle(ctx))); return; } while(0)

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
	sqlite3* db = sqlite3_context_db_handle(ctx);
	sql_fn* fn = sqlite3_user_data(ctx);
	if(argc != fn->argc) fail(ctx, "Wrong number of arguments");
	sqlite3_stmt* stmt = fn_stmt(fn, db);
	if(!stmt) fail(ctx, "Failed to allocate a statement");
	for(int i=1; i<=fn->argc; i++) if(sqlite3_bind_value(stmt, i, argv[i-1]) != SQLITE_OK) fail(ctx, "Failed to bind argument");
	if(sqlite3_step(stmt) != SQLITE_ROW) {
		sqlite3_reset(stmt);
		fail(ctx, "Did not return a row");
	}
	sqlite3_value* rst = sqlite3_column_value(stmt, 0);
	if(!rst) fail(ctx, "No result value");
	if(debug) switch(sqlite3_column_type(stmt, 1)) {
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
 * CREATE_FUNCTION(name, nargs, flags, code)
 */
static void create_function(sqlite3_context* ctx, int argc, sqlite3_value** argv) {
	/* get arguments */
	if(argc != 4) fail(ctx, "Wrong number of arguments");
	if(sqlite3_value_type(argv[0]) != SQLITE_TEXT) fail(ctx, "Function name must be TEXT");
	if(sqlite3_value_type(argv[1]) != SQLITE_INTEGER) fail(ctx, "Function argc must be INTEGER");
	if(sqlite3_value_type(argv[2]) != SQLITE_TEXT) fail(ctx, "Function flags must be TEXT");
	if(sqlite3_value_type(argv[3]) != SQLITE_TEXT) fail(ctx, "Function code must be TEXT");
	sqlite3* fn_db = sqlite3_context_db_handle(ctx);
	const char* fn_name = (const char*) sqlite3_value_text(argv[0]);
	const int fn_argc = sqlite3_value_int(argv[1]);
	const char* fn_flags_s = (const char*) sqlite3_value_text(argv[2]);
	const char* fn_code_s = (const char*) sqlite3_value_text(argv[3]);

	/* get flags */
	int fn_flags = SQLITE_UTF8;
	for(char const* c = fn_flags_s; *c; c++) switch(*c) {
		case 'd': fn_flags |= SQLITE_DETERMINISTIC; break;
		case 'D': fn_flags |= SQLITE_DIRECTONLY; break;
		case 'i': fn_flags |= SQLITE_INNOCUOUS; break;
	}

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

	return SQLITE_OK;
}