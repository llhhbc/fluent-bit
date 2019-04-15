
#ifndef FLB_FILTER_CGO_LIB_H
#define FLB_FILTER_CGO_LIB_H

#include "cgo.h"

// three func define in go libs


#define MAX_PARAMETERS 50
#define MAX_FIELD  100

#define GOLIB_SO_KEY_NAME "golib_so"

#define INIT_FUNC_NAME "GoLibInit"
#define FILTER_FUNC_NAME "GoLibFilter"
#define EXIT_FUNC_NAME "GoLibExit"


struct cgolib_conf {
    void * handler;
    GoInt (*init_lib_func)(GoSlice p0, GoSlice p1);
    GoInt (*filter_lib_func)(GoSlice p0, GoSlice p1);
    GoInt (*exit_lib_func)();
};

#endif /* FLB_FILTER_CGO_LIB_H */