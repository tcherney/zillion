const std = @import("std");
const ztracy = @import("ztracy");

var timer: std.time.Timer = undefined;
pub fn timer_start() !void {
    timer = try std.time.Timer.start();
}

pub fn timer_end(msg: []const u8) !void {
    std.debug.print("{d} s elapsed.{s}\n", .{ @as(f32, @floatFromInt(timer.read())) / 1000000000.0, msg });
    timer.reset();
}

const Calculation = struct { sum: f32 = undefined, min: f32 = undefined, max: f32 = undefined, num: u32 = undefined };

pub fn parse_and_collect_measurement(buffer: []u8, calculations: *std.StringHashMap(Calculation), lock: *std.Thread.Mutex) !void {
    var start: usize = 0;
    while (start < buffer.len) {
        const end: usize = std.mem.indexOfScalarPos(u8, buffer, start, '\n') orelse buffer.len;
        var line = buffer[start..end];
        const line_split = std.mem.indexOfScalar(u8, line, ';') orelse continue;
        const station_name = line[0..line_split];
        const value = try std.fmt.parseFloat(f32, line[line_split + 1 .. line.len - 1]);
        lock.lock();
        const station = try calculations.getOrPut(station_name);
        if (station.found_existing) {
            station.value_ptr.*.max = if (value > station.value_ptr.*.max) value else station.value_ptr.*.max;
            station.value_ptr.*.min = if (value < station.value_ptr.*.min) value else station.value_ptr.*.min;
            station.value_ptr.*.sum += value;
            station.value_ptr.*.num += 1;
        } else {
            station.value_ptr.* = Calculation{ .min = value, .max = value, .sum = 0.0, .num = 1 };
        }
        lock.unlock();
        start = end + 1;
    }
}

pub fn thread_run(buffer: []u8, calculations: *std.StringHashMap(Calculation), lock: *std.Thread.Mutex) !void {
    try parse_and_collect_measurement(buffer, calculations, lock);
}

pub fn main() !void {
    const tracy_zone = ztracy.ZoneNC(@src(), "Compute Magic", 0x00_ff_00_00);
    defer tracy_zone.End();
    const stdout_file = std.io.getStdOut().writer();
    var bw = std.io.bufferedWriter(stdout_file);
    const stdout = bw.writer();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    try timer_start();
    const file = try std.fs.cwd().openFile("measurements.txt", .{});
    const size_limit = std.math.maxInt(usize);
    const buffer = try file.readToEndAlloc(allocator, size_limit);

    try timer_end(" For input");
    var lock = std.Thread.Mutex{};
    const NUM_THREADS = 8;
    var calculations: std.StringHashMap(Calculation) = std.StringHashMap(Calculation).init(allocator);
    var threads: []std.Thread = try allocator.alloc(std.Thread, NUM_THREADS);
    const data_split = buffer.len / NUM_THREADS;
    var start: usize = 0;
    for (0..NUM_THREADS) |i| {
        const start_end: usize = (i + 1) * data_split;
        const end: usize = std.mem.indexOfScalarPos(u8, buffer, start_end, '\n') orelse buffer.len;
        threads[i] = try std.Thread.spawn(.{}, thread_run, .{ buffer[start..end], &calculations, &lock });
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
    allocator.free(buffer);
    if (gpa.deinit() == .leak) {
        std.debug.print("Leaked!\n", .{});
    }
}
