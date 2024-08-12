const std = @import("std");
const builtin = @import("builtin");

var timer: std.time.Timer = undefined;
pub fn timer_start() !void {
    timer = try std.time.Timer.start();
}

pub fn timer_end(msg: []const u8) !void {
    std.debug.print("{d} s elapsed.{s}\n", .{ @as(f32, @floatFromInt(timer.read())) / 1000000000.0, msg });
    timer.reset();
}

const Calculation = struct { sum: f32 = undefined, min: f32 = undefined, max: f32 = undefined, num: u32 = undefined };

pub fn parse_and_collect_measurement(buffer: []u8, calculations: *std.StringHashMap(Calculation), lock: *std.Thread.Mutex, allocator: std.mem.Allocator) !void {
    var local_calculations: std.StringHashMap(Calculation) = std.StringHashMap(Calculation).init(allocator);
    try local_calculations.ensureTotalCapacity(2048);
    defer local_calculations.deinit();
    var start: usize = 0;
    while (start < buffer.len) {
        const end: usize = std.mem.indexOfScalarPos(u8, buffer, start, '\n') orelse buffer.len;
        var line = buffer[start..end];
        const line_split = std.mem.indexOfScalar(u8, line, ';') orelse continue;
        const station_name = line[0..line_split];
        const value = try std.fmt.parseFloat(f32, line[line_split + 1 .. line.len - 1]);

        const station = try local_calculations.getOrPut(station_name);
        if (station.found_existing) {
            station.value_ptr.*.max = @max(value, station.value_ptr.*.max);
            station.value_ptr.*.min = @min(value, station.value_ptr.*.min);
            station.value_ptr.*.sum += value;
            station.value_ptr.*.num += 1;
        } else {
            station.value_ptr.* = Calculation{ .min = value, .max = value, .sum = 0.0, .num = 1 };
        }

        start = end + 1;
    }
    var key_iter = local_calculations.iterator();
    lock.lock();
    while (key_iter.next()) |local_calc| {
        const station_name = local_calc.key_ptr.*;
        const station = local_calc.value_ptr.*;
        const calc = try calculations.getOrPut(station_name);
        if (calc.found_existing) {
            calc.value_ptr.*.max = @max(calc.value_ptr.*.max, station.max);
            calc.value_ptr.*.min = @min(calc.value_ptr.*.min, station.min);
            calc.value_ptr.*.sum += station.sum;
            calc.value_ptr.*.num += station.num;
        } else {
            calc.value_ptr.* = station;
        }
    }
    lock.unlock();
}

pub fn thread_run(buffer: []u8, calculations: *std.StringHashMap(Calculation), lock: *std.Thread.Mutex, allocator: std.mem.Allocator) !void {
    try parse_and_collect_measurement(buffer, calculations, lock, allocator);
}

const BufferType = if (builtin.os.tag == .windows) []u8 else []align(std.mem.page_size) u8;

pub extern "kernel32" fn CreateFileMappingA(hFile: std.os.windows.HANDLE, lpFileMappingAttributes: std.os.windows.DWORD, flProtect: std.os.windows.DWORD, dwMaximumSizeHigh: std.os.windows.DWORD, dwMaximumSizeLow: std.os.windows.DWORD, lpName: std.os.windows.DWORD) callconv(std.os.windows.WINAPI) ?std.os.windows.HANDLE;
pub extern "kernel32" fn MapViewOfFile(hFileMappingObject: std.os.windows.HANDLE, dwDesiredAccess: std.os.windows.DWORD, dwFileOffsetHigh: std.os.windows.DWORD, dwFileOffsetLow: std.os.windows.DWORD, dwNumberOfBytesToMap: std.os.windows.SIZE_T) callconv(std.os.windows.WINAPI) ?std.os.windows.LPVOID;

pub fn main() !void {
    const stdout_file = std.io.getStdOut().writer();
    var bw = std.io.bufferedWriter(stdout_file);
    const stdout = bw.writer();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    try timer_start();
    const file = try std.fs.cwd().openFile("measurements.txt", .{});
    const size_limit = try file.getEndPos();
    var buffer: BufferType = undefined;
    if (builtin.os.tag == .windows) {
        const hMap = CreateFileMappingA(file.handle, 0, std.os.windows.PAGE_READONLY, 0, 0, 0);
        const lpBasePtr = MapViewOfFile(hMap.?, 4, 0, 0, 0);
        buffer = @as([*]u8, @ptrCast(lpBasePtr.?))[0..size_limit];
        //buffer = try file.readToEndAlloc(allocator, size_limit);
    } else {
        buffer = try std.posix.mmap(
            null,
            size_limit,
            std.posix.PROT.READ,
            .{ .TYPE = .PRIVATE },
            file.handle,
            0,
        );
        try std.posix.madvise(buffer.ptr, size_limit, std.posix.MADV.HUGEPAGE);
    }
    try timer_end(" For input");
    var lock = std.Thread.Mutex{};
    const NUM_THREADS = try std.Thread.getCpuCount() - 1;
    var calculations: std.StringHashMap(Calculation) = std.StringHashMap(Calculation).init(allocator);
    try calculations.ensureTotalCapacity(2048);
    var threads: []std.Thread = try allocator.alloc(std.Thread, NUM_THREADS);
    const data_split = buffer.len / NUM_THREADS;
    var start: usize = 0;
    for (0..NUM_THREADS) |i| {
        const start_end: usize = (i + 1) * data_split;
        const end: usize = std.mem.indexOfScalarPos(u8, buffer, start_end, '\n') orelse buffer.len;
        threads[i] = try std.Thread.spawn(.{}, thread_run, .{ buffer[start..end], &calculations, &lock, allocator });
        start = end + 1;
        if (start >= buffer.len) break;
    }
    for (threads) |thread| {
        thread.join();
    }
    allocator.free(threads);

    try timer_end(" For calculations");
    try stdout.print("{{", .{});
    var key_iter = calculations.keyIterator();
    var next_key = key_iter.next();
    while (next_key != null) : (next_key = key_iter.next()) {
        const station = calculations.get(next_key.?.*);
        try stdout.print("{s}={d}/{d}/{d}, ", .{ next_key.?.*, station.?.min, station.?.sum / @as(f32, @floatFromInt(station.?.num)), station.?.max });
    }
    try stdout.print("}}\n", .{});
    try bw.flush();
    try timer_end(" For output");
    file.close();
    calculations.deinit();
    if (builtin.os.tag == .windows) {
        allocator.free(buffer);
    } else {
        std.posix.munmap(buffer);
    }
    if (gpa.deinit() == .leak) {
        std.debug.print("Leaked!\n", .{});
    }
}
