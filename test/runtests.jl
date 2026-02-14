using TestItems

@testsnippet Setup begin

    using RollupTree
    using DataFrames
    using Graphs
    using MetaGraphsNext
 
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

    wbs_tree = MetaGraphsNext.MetaGraph(Graphs.SimpleDiGraph(), label_type = String)
    for i in 1:nrow(wbs_table)
        id = wbs_table[i, :id]
        pid = wbs_table[i, :pid]
        if !ismissing(pid)
            add_edge!(wbs_tree, pid, id)
        else
            add_vertex!(wbs_tree, id)
        end
    end
end

@testitem "test rollup()" setup = [Setup] begin
    @test isequal(wbs_table_rollup, RollupTree.rollup(wbs_table_rollup, wbs_tree))
end

@testitem "test update_prop()" setup = [Setup] begin
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

@testitem "test df_get_keys() and df_get_ids()" setup = [Setup] begin
    expected = ["top", "1", "2", "3", "1.1", "1.2", "2.1", "2.2", "3.1", "3.2"]
    @test isequal(RollupTree.df_get_keys(wbs_table, :id), expected)
    @test isequal(RollupTree.df_get_ids(wbs_table), expected)
end

@testitem "test df_get_row_by_key() and df_get_row_by_id()" setup = [Setup] begin
    expected = (id = "1.1", pid = "1", name = "Electrical", work = 11.8, budget = 25000)
    @test isequal(RollupTree.df_get_row_by_key(wbs_table, :id, "1.1"), expected)
    @test isequal(RollupTree.df_get_row_by_id(wbs_table, "1.1"), expected)
end

@testitem "test df_get_by_key() and df_get_by_id()" setup = [Setup] begin
  @test RollupTree.df_get_by_key(wbs_table, :id, "1.1", :work) == 11.8
  @test RollupTree.df_get_by_key(wbs_table, :id, "1.1", :budget) == 25000
  @test RollupTree.df_get_by_id(wbs_table, "1.1", :work) == 11.8
  @test RollupTree.df_get_by_id(wbs_table, "1.1", :budget) == 25000
end

@testitem "test df_set_row_by_key() and df_set_row_by_id()" setup = [Setup] begin
  expected = (id = "1.1", pid = "2", name = "Thermal", work = 11.9, budget = 25001)
  shuffled = expected[(:pid, :name, :id, :budget, :work)]
  result1 = RollupTree.df_set_row_by_key(wbs_table, :id, "1.1", shuffled)
  @test isequal(result1[findfirst(result1[!, :id] .== "1.1"), :], expected)

  result2 = RollupTree.df_set_row_by_id(wbs_table, "1.1", shuffled)
  @test isequal(result2[findfirst(result2[!, :id] .== "1.1"), :], expected)
end

@testitem "test df_set_by_key() and df_set_by_id()" setup = [Setup] begin
  expected = deepcopy(wbs_table)
  expected[findfirst(expected[!, :id] .== "1.1"), :work] = 11.9
  expected[findfirst(expected[!, :id] .== "1.1"), :budget] = 25001

  result1 = RollupTree.df_set_by_key(wbs_table, :id, "1.1", :work, 11.9)
  result2 = RollupTree.df_set_by_key(result1, :id, "1.1", :budget, 25001)
  @test isequal(result2, expected)

  result3 = RollupTree.df_set_by_id(wbs_table, "1.1", :work, 11.9)
  result4 = RollupTree.df_set_by_id(result3, "1.1", :budget, 25001)
  @test isequal(result4, expected)
end

@testitem "update_df_prop_by_key() and update_df_prop_by_id()" setup = [Setup] begin
    expected = deepcopy(wbs_table)
    expected[findfirst(expected[!, :id] .== "1"), :work] = 11.8 + 33.8
    result1 = RollupTree.update_df_prop_by_key(wbs_table, :id, "1", ["1.1", "1.2"], :work)
    @test isequal(result1, expected)

    result2 = RollupTree.update_df_prop_by_id(wbs_table, "1", ["1.1", "1.2"], :work)
    @test isequal(result2, expected)
end