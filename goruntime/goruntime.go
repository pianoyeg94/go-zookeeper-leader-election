package goruntime

// Nanotime gets the Linux CLOCK_MONOTIC time in nanoseconds
// (time passed since the system was booted).
//
// Switches to g0's operating system stack because
// on Linux kernel versions >= 2.6 it performs a vDSO call to the
// `__vdso_clock_gettime` symbol from the dynamically linked
// linux-vdso.so.1 shared object, which avoids switching from user-space
// to kernel-land and back again (requires registers and stack adjustments
// for the C ABI).
//
// On kernel versions < 2.6 falls back to performing a real system call.
//
// https://github.com/golang/go/blob/go1.21.0/src/runtime/sys_linux_amd64.s#L223
func Nanotime() int64 {
	return nanotime()
}

// Procyield executes the PAUSE assembly instruction provided
// by the x86-64 ISA (instruction set architecture) - Intel and AMD processors.
// This instruction is mainly used in spin-wait loops
// to help the branch predictor avoid filling the processor
// pipeline with the same uncommited speculative CMP instructions -
// this reduces the power consumed by the CPU and greatly improves
// its performance.
// Basically achieves this by avoiding memory order violations, which
// lead to unnecessary pipeline flushes.
// It's also recommened by the Intel manual that a PAUSE instruction
// should be placed in all spin-wait loops.
//
// The instruction may be called serveral times in an assembly loop when `cycles`
// is greater than 1.
//
// https://github.com/golang/go/blob/go1.21.0/src/runtime/asm_amd64.s#L775
func Procyield(cycles uint32) {
	procyield(cycles)
}
