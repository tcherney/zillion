const std = @import("std");

const Measurement = struct {
    value: f32 = undefined,
    station: std.ArrayList(u8) = undefined,
    pub fn print(self: *Measurement) void {
        std.debug.print("Station: {s}, Measurement: {d}\n", .{ self.station.items, self.value });
    }
    pub fn deinit(self: *Measurement) void {
        self.station.deinit();
    }
};

const Calculation = struct {
    station: []const u8 = undefined,
    mean: f32 = undefined,
    min: f32 = undefined,
    max: f32 = undefined,
    num: u32 = undefined,
    pub fn calc_mean(self: *Calculation) void {
        self.mean /= @as(f32, @floatFromInt(self.num));
    }
    pub fn print(self: *Calculation) !void {
        const stdout_file = std.io.getStdOut().writer();
        var bw = std.io.bufferedWriter(stdout_file);
        const stdout = bw.writer();
        try stdout.print("{s}={d}/{d}/{d}:Total{d}", .{ self.station, self.min, self.mean, self.max, self.num });
        try bw.flush();
    }
};

pub fn read_measurements(file_name: []const u8, allocator: std.mem.Allocator) !std.ArrayList(Measurement) {
    var measurements: std.ArrayList(Measurement) = std.ArrayList(Measurement).init(allocator);
    const file = try std.fs.cwd().openFile(file_name, .{});
    defer file.close();
    const size_limit = std.math.maxInt(u32);
    const buffer = try file.readToEndAlloc(allocator, size_limit);
    defer allocator.free(buffer);
    var lines: std.mem.SplitIterator(u8, std.mem.DelimiterType.any) = std.mem.splitAny(u8, buffer, "\n");
    var line = lines.next();
    while (line != null) : (line = lines.next()) {
        var measurement_iter: std.mem.SplitIterator(u8, std.mem.DelimiterType.any) = std.mem.splitAny(u8, line.?, ";");
        const station = measurement_iter.next() orelse break;
        const value_str = measurement_iter.next() orelse break;
        var station_list = std.ArrayList(u8).init(allocator);
        _ = try station_list.writer().write(station);
        try measurements.append(Measurement{ .station = station_list, .value = try std.fmt.parseFloat(f32, value_str[0 .. value_str.len - 1]) });
    }
    return measurements;
}

pub fn collect_measurements(meansurements: []Measurement, allocator: std.mem.Allocator) !std.StringHashMap(Calculation) {
    var calculations = std.StringHashMap(Calculation).init(allocator);
    for (meansurements) |measurement| {
        var mapped_calc = calculations.get(measurement.station.items) orelse Calculation{ .station = measurement.station.items, .min = measurement.value, .max = measurement.value, .mean = 0.0, .num = 0 };
        mapped_calc.max = if (measurement.value > mapped_calc.max) measurement.value else mapped_calc.max;
        mapped_calc.min = if (measurement.value < mapped_calc.min) measurement.value else mapped_calc.min;
        mapped_calc.mean += measurement.value;
        mapped_calc.num += 1;
        try calculations.put(mapped_calc.station, mapped_calc);
    }
    return calculations;
}

pub fn process_calculations(calculations: std.StringHashMap(Calculation), allocator: std.mem.Allocator) !std.ArrayList(Calculation) {
    var proccessed_calculations = std.ArrayList(Calculation).init(allocator);
    var key_iter = calculations.keyIterator();
    var next_key = key_iter.next();
    while (next_key != null) : (next_key = key_iter.next()) {
        var mapped_calc = calculations.get(next_key.?.*);
        mapped_calc.?.calc_mean();
        try proccessed_calculations.append(mapped_calc.?);
    }
    return proccessed_calculations;
}

pub fn main() !void {
    const stdout_file = std.io.getStdOut().writer();
    var bw = std.io.bufferedWriter(stdout_file);
    const stdout = bw.writer();
    try stdout.print("Run `zig build test` to run the tests.\n", .{});
    try bw.flush(); // Don't forget to flush!
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    var measurements: std.ArrayList(Measurement) = try read_measurements("measurements.txt", allocator);
    var calculations = try collect_measurements(measurements.items, allocator);
    const processed_calculations = try process_calculations(calculations, allocator);
    calculations.deinit();
    try stdout.print("{{", .{});
    for (0..processed_calculations.items.len) |i| {
        try processed_calculations.items[i].print();
        try stdout.print(", ", .{});
        try bw.flush();
    }
    try stdout.print("}}\n", .{});
    try bw.flush();
    for (0..measurements.items.len) |i| {
        //measurements.items[i].print();
        measurements.items[i].deinit();
    }
    processed_calculations.deinit();

    measurements.deinit();
    if (gpa.deinit() == .leak) {
        std.debug.print("Leaked!\n", .{});
    }
}

test "simple test" {
    var list = std.ArrayList(i32).init(std.testing.allocator);
    defer list.deinit(); // Try commenting this out and see if zig detects the memory leak!
    try list.append(42);
    try std.testing.expectEqual(@as(i32, 42), list.pop());
}

test "fuzz example" {
    // Try passing `--fuzz` to `zig build` and see if it manages to fail this test case!
    const input_bytes = std.testing.fuzzInput(.{});
    try std.testing.expect(!std.mem.eql(u8, "canyoufindme", input_bytes));
}
