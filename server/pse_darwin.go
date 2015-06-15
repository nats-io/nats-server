// Copyright 2015 Apcera Inc. All rights reserved.

package server

/*
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <mach/task.h>
#include <mach/mach_init.h>
#include <mach/mach_port.h>
#include <mach/thread_act.h>
#include <mach/vm_map.h>
#include <libproc.h>

#define TH_USAGE_SCALE 1000

int getusage(double *pcpu, unsigned int *rss, unsigned int *vss)
{
    struct task_basic_info t_info;
    mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;
    task_t task = MACH_PORT_NULL;
    int ret;

    *rss = *vss = *pcpu = 0;
    ret = task_for_pid(mach_task_self(), getpid(), &task);
    if (ret != KERN_SUCCESS) {
        return ret;
    }
    task_info(task, TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);
    *rss = t_info.resident_size;
    *vss = t_info.virtual_size;

    // Percent CPU Calculations

    thread_port_array_t thread_list;
    unsigned int        thread_count;

	ret = task_threads(task, &thread_list, &thread_count);
	if (ret != KERN_SUCCESS) {
        mach_port_deallocate(mach_task_self(), task);
		return ret;
	}

    int j;
    int cpu_usage;
    unsigned int thread_info_count = THREAD_BASIC_INFO_COUNT;

    cpu_usage = 0;

	for (j = 0; j < thread_count; j++) {
		int tstate;
        int error;
	    struct thread_basic_info tb;

        thread_info_count = THREAD_BASIC_INFO_COUNT;
		error = thread_info(thread_list[j], THREAD_BASIC_INFO,
			 (thread_info_t)&tb, &thread_info_count);
		if (error != KERN_SUCCESS) {
			ret=1;
		}
		cpu_usage += tb.cpu_usage;
    }
	vm_deallocate(mach_task_self(), (vm_address_t)(thread_list),
		 sizeof(thread_list) * thread_count);
    mach_port_deallocate(mach_task_self(), task);

    *pcpu = ((double)cpu_usage) * 100.0 / ((double)TH_USAGE_SCALE);

    return 0;
}

*/
import "C"

import (
	"syscall"
)

func procUsage(pcpu *float64, rss, vss *int64) error {
	var r, v C.uint
	var c C.double

	if ret := C.getusage(&c, &r, &v); ret != 0 {
		return syscall.Errno(ret)
	}
	*rss = int64(r)
	*vss = int64(v)
	*pcpu = float64(c)
	return nil
}
