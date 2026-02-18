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
    wbs_table_rollup[wbs_table[!, :id] .== "1", :work] .= 11.8 + 33.8
    wbs_table_rollup[wbs_table[!, :id] .== "2", :work] .= 18.2 + 5.8
    wbs_table_rollup[wbs_table[!, :id] .== "3", :work] .= 16.2 + 14.2
    wbs_table_rollup[wbs_table[!, :id] .== "top", :work] .= 11.8 + 33.8 + 18.2 + 5.8 + 16.2 + 14.2
    wbs_table_rollup[wbs_table[!, :id] .== "1", :budget] .= 25000 + 61000
    wbs_table_rollup[wbs_table[!, :id] .== "2", :budget] .= 37000 + 9000
    wbs_table_rollup[wbs_table[!, :id] .== "3", :budget] .= 62000 + 21500
    wbs_table_rollup[wbs_table[!, :id] .== "top", :budget] .= 25000 + 61000 + 37000 + 9000 + 62000 + 21500

    wbs_tree = MetaGraphsNext.MetaGraph(Graphs.SimpleDiGraph(), label_type = String)
    for i in 1:nrow(wbs_table)
        id = wbs_table[i, :id]
        wbs_tree[id] = nothing
        pid = wbs_table[i, :pid]
        if !ismissing(pid)
            wbs_tree[pid] = nothing
            wbs_tree[id, pid] = nothing
        end
    end
end

@testitem "rollup()" setup = [Setup] begin
    result = RollupTree.rollup(
        wbs_tree,
        wbs_table,
        (d, p, c) -> begin
            if length(c) == 0
                d
            else
                idx = findfirst(d[!, :id] .== p)
                if isnothing(idx)
                    error("Parent ID not found in DataFrame")
                end
                d[idx, :work] = sum(d[in(c).(d.id), :work])
                d[idx, :budget] = sum(d[in(c).(d.id), :budget])
                d
            end
        end,
        (t, d) -> true
    )
   @test isequal(wbs_table_rollup, result)
end

@testitem "update_rollup()" setup = [Setup] begin
    input = deepcopy(wbs_table_rollup)
    input[findfirst(input[!, :id] .== "3.2"), :budget] = 22000
    input[findfirst(input[!, :id] .== "2.2"), :work] = missing
    expected = deepcopy(wbs_table_rollup)
    expected[findfirst(input[!, :id] .== "3.2"), :budget] = 22000
    expected[findfirst(input[!, :id] .== "2.2"), :work] = missing
    expected[findfirst(expected[!, :id] .== "3"), :budget] = 62000 + 22000
    expected[findfirst(expected[!, :id] .== "2"), :work] = 18.2 + 5.8
    expected[findfirst(expected[!, :id] .== "top"), :budget] = 25000 + 61000 + 37000 + 9000 + 62000 + 22000
    expected[findfirst(expected[!, :id] .== "top"), :work] = 11.8 + 33.8 + 18.2 + 5.8 + 16.2 + 14.2
    result = RollupTree.update_rollup(
        wbs_tree, input, "3.2",
        (d, v, s) -> begin
            idx = findfirst(d[!, :id] .== v)
            if isnothing(idx)
                error("Vertex ID not found in DataFrame")
            end
            d[idx, :work] = sum(d[in(s).(d.id), :work])
            d[idx, :budget] = sum(d[in(s).(d.id), :budget])
            d
        end
    )
    @test isequal(result, expected)
end

@testitem "update_prop()" setup = [Setup] begin
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

@testitem "df_get_keys() and df_get_ids()" setup = [Setup] begin
    expected = ["top", "1", "2", "3", "1.1", "1.2", "2.1", "2.2", "3.1", "3.2"]
    @test isequal(RollupTree.df_get_keys(wbs_table, :id), expected)
    @test isequal(RollupTree.df_get_ids(wbs_table), expected)
end

@testitem "df_get_row_by_key() and df_get_row_by_id()" setup = [Setup] begin
    expected = (id = "1.1", pid = "1", name = "Electrical", work = 11.8, budget = 25000)
    @test isequal(RollupTree.df_get_row_by_key(wbs_table, :id, "1.1"), expected)
    @test isequal(RollupTree.df_get_row_by_id(wbs_table, "1.1"), expected)
end

@testitem "df_get_by_key() and df_get_by_id()" setup = [Setup] begin
  @test RollupTree.df_get_by_key(wbs_table, :id, "1.1", :work) == 11.8
  @test RollupTree.df_get_by_key(wbs_table, :id, "1.1", :budget) == 25000
  @test RollupTree.df_get_by_id(wbs_table, "1.1", :work) == 11.8
  @test RollupTree.df_get_by_id(wbs_table, "1.1", :budget) == 25000
end

@testitem "df_set_row_by_key() and df_set_row_by_id()" setup = [Setup] begin
  expected = (id = "1.1", pid = "2", name = "Thermal", work = 11.9, budget = 25001)
  shuffled = expected[(:pid, :name, :id, :budget, :work)]
  result1 = RollupTree.df_set_row_by_key(wbs_table, :id, "1.1", shuffled)
  @test isequal(result1[findfirst(result1[!, :id] .== "1.1"), :], expected)

  result2 = RollupTree.df_set_row_by_id(wbs_table, "1.1", shuffled)
  @test isequal(result2[findfirst(result2[!, :id] .== "1.1"), :], expected)
end

@testitem "df_set_by_key() and df_set_by_id()" setup = [Setup] begin
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

@testitem "validate_ds()" setup = [Setup] begin
    get_keys = d -> d[!, :id]
    get_prop = (d, i) -> d[findfirst(d[!, :id] .== i), :work]

     # Test with a valid DataFrame
    @test RollupTree.validate_ds(wbs_tree, wbs_table, get_keys, get_prop) === true

    # Test with mismatched keys
    mismatched_keys_df = deepcopy(wbs_table)
    mismatched_keys_df[1, :id] = "invalid"
    @test_throws ErrorException RollupTree.validate_ds(wbs_tree, mismatched_keys_df, get_keys, get_prop)

    # Test with invalid work value
    invalid_value_df = deepcopy(wbs_table)
    invalid_value_df[findfirst(invalid_value_df[!, :id] .== "1.1"), :work] = missing
    @test_throws ErrorException RollupTree.validate_ds(wbs_tree, invalid_value_df, get_keys, get_prop)
 end

@testitem "validate_dag()" setup = [Setup] begin
    # Test with a valid DAG
    @test RollupTree.validate_dag(wbs_tree) === true

    # Test with an undirected graph
    undirected_graph = Graphs.SimpleGraph(nv(wbs_tree))
    for e in edges(wbs_tree)
        add_edge!(undirected_graph, src(e), dst(e))
    end
    @test_throws ErrorException RollupTree.validate_dag(undirected_graph)

    # Test with a graph containing a directed cycle
    cyclic_graph = deepcopy(wbs_tree)
    cyclic_graph["1", "1.1"] = nothing
    cyclic_graph["1.1", "1.2"] = nothing
    @test_throws ErrorException RollupTree.validate_dag(cyclic_graph)

    # Test with a disconnected graph
    disconnected_graph = deepcopy(wbs_tree)
    add_vertex!(disconnected_graph, "isolated")
    # @test_throws ErrorException RollupTree.validate_dag(disconnected_graph)
end

@testitem "validate_tree()" setup = [Setup] begin
    # Test with a valid tree
    @test RollupTree.validate_tree(wbs_tree) === true

    # Test with a non-directed graph
    undirected_graph = Graphs.SimpleGraph(nv(wbs_tree))
    @test_throws ErrorException RollupTree.validate_tree(undirected_graph)

    # Test with a graph containing an undirected cycle
    cyclic_graph = deepcopy(wbs_tree)
    cyclic_graph["1", "1.1"] = nothing
    @test_throws ErrorException RollupTree.validate_tree(cyclic_graph)

    # Test with a disconnected graph
    disconnected_graph = deepcopy(wbs_tree)
    add_vertex!(disconnected_graph, "isolated")
    @test_throws ErrorException RollupTree.validate_tree(disconnected_graph)

    # Test with a graph that has multiple roots
    multi_root_graph = deepcopy(wbs_tree)
    add_vertex!(multi_root_graph, "new_root")
    multi_root_graph["1.1", "new_root"] = nothing
    @test_throws ErrorException RollupTree.validate_tree(multi_root_graph)
end