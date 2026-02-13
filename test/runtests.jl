using RollupTree
using Test
using DataFrames
using Graphs

@testset "RollupTree.jl" begin

    wbs_table = DataFrame(
        id = ["top", "1", "2", "3", "1.1", "1.2", "2.1", "2.2", "3.1", "3.2"],
        pid = [missing, "top", "top", "top", "1", "1", "2", "2", "3", "3"],
        name = ["Construction of a House", "Internal", "Foundation", "External",
           "Electrical", "Plumbing", "Excavate", "Steel Erection", "Masonry Work", "Building Finishes"],
        work = [missing, missing, missing, missing, 11.80, 33.80, 18.20, 5.80, 16.20, 14.20],
        budget = [missing, missing, missing, missing, 25000, 61000, 37000, 9000, 62000, 21500]
    )

    wbs_table_rollup = deepcopy(wbs_table)
    wbs_table_rollup[wbs_table[!, :id] .== "1", :work] .= 45.6
    wbs_table_rollup[wbs_table[!, :id] .== "2", :work] .= 24.0
    wbs_table_rollup[wbs_table[!, :id] .== "3", :work] .= 30.4
    wbs_table_rollup[wbs_table[!, :id] .== "top", :work] .= 100.0
    wbs_table_rollup[wbs_table[!, :id] .== "1", :budget] .= 86000
    wbs_table_rollup[wbs_table[!, :id] .== "2", :budget] .= 46000
    wbs_table_rollup[wbs_table[!, :id] .== "3", :budget] .= 83500
    wbs_table_rollup[wbs_table[!, :id] .== "top", :budget] .= 215500

    wbs_tree = Graphs.SimpleDiGraph(nrow(wbs_table))
    for row in eachrow(wbs_table)
        if !ismissing(row.pid)
            parent_idx = findfirst(wbs_table[!, :id] .== row.pid)
            child_idx = findfirst(wbs_table[!, :id] .== row.id)
            add_edge!(wbs_tree, child_idx, parent_idx)
        end
    end

    @testset "test rollup()" begin
        @test isequal(wbs_table_rollup, RollupTree.rollup(wbs_table_rollup, wbs_tree))
    end

    @testset "test update_prop()" begin
        expected1 = deepcopy(wbs_table)
        expected1[findfirst(expected1[!, :id] .== "1"), :work] = 11.8 + 33.8
        result1 = RollupTree.update_prop(
            wbs_table, "1", ["1.1", "1.2"],
            (d, k, v) -> begin d[findfirst(d[!, :id] .== k), :work] = v; d end,
            (d, k) -> d[findfirst(d[!, :id] .== k), :work],
            (av) -> reduce(+, av),
            (ds, target, v) -> v
        )
        @test isequal(result1, expected1)

        expected2 = deepcopy(expected1)
        expected2[findfirst(expected2[!, :id] .== "1"), [:work, :budget]] .= [11.8 + 33.8, 25000 + 61000]
        result2 = RollupTree.update_prop(
            wbs_table, "1", ["1.1", "1.2"],
            (d, k, v) -> begin d[findfirst(d[!, :id] .== k), [:work, :budget]] = v; d end,
            (d, k) -> d[findfirst(d[!, :id] .== k), [:work, :budget]],
            (av) -> mapreduce(x -> Vector(x), +, av),
            (ds, target, v) -> v
        )
        @test isequal(result2, expected2)

      result3 = RollupTree.update_prop(
            wbs_table, "1.1", [],
            (d, k, v) -> begin d[findfirst(d[!, :id] .== k), :work] = v; d end,
            (d, k) -> d[findfirst(d[!, :id] .== k), :work],
            (av) -> reduce(+, av),
            (ds, target, v) -> v
        )
        @test isequal(result3, wbs_table)
    end
end
