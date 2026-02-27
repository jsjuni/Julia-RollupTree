module RollupTree

    using DataFrames
    using Graphs
    using MetaGraphsNext

    export rollup, update_rollup, validate_ds, validate_dag, validate_tree,
            update_prop,
            df_get_by_key, df_get_by_id, df_set_by_key, df_set_by_id,
            df_get_keys, df_get_ids,
            df_get_row_by_key, df_get_row_by_id,
            df_set_row_by_key, df_set_row_by_id,
            update_df_prop_by_key, update_df_prop_by_id

    function rollup(tree::MetaGraphsNext.MetaGraph, ds, update, validate_ds, validate_tree = validate_tree)
        validate_tree(tree)
        validate_ds(tree, ds)
        foldl(
            (s, v) -> update(s, v, inneighbor_labels(tree, v)),
            map(v -> label_for(tree, v), topological_sort(tree)),
            init = ds
        )
    end

    function update_rollup(tree::MetaGraphsNext.MetaGraph, ds, vertex, update)
        if outdegree(tree, code_for(tree, vertex)) == 0
            error("update_rollup should only be called on leaf vertices.")
        end
        todo = [vertex]
        vertices_above = []
        while length(todo) > 0
            v = pop!(todo)
            for p in outneighbor_labels(tree, v)
                push!(vertices_above, p)
                push!(todo, p)
            end
        end
        foldl(
            (s, v) -> update(s, v, inneighbor_labels(tree, v)),
            vertices_above,
            init = ds
        )
    end

    function validate_ds(tree::MetaGraphsNext.MetaGraph, ds, get_keys, get_prop, op = x -> isa(x, Number))
        ids_in_tree = Set(labels(tree))
        ids_in_ds = Set(get_keys(ds))
        if ids_in_tree != ids_in_ds
            error("The set of IDs in the DataFrame does not match the set of vertex labels in the graph.")
        end
        for id in filter(id -> indegree(tree, code_for(tree, id)) == 0, ids_in_tree)
            value = get_prop(ds, id)
            if !op(value)
                error("Invalid value for ID $id: $value")
            end
        end
        return true
    end

    function validate_dag(graph::MetaGraphsNext.MetaGraph)
        if !is_directed(graph)
            error("The provided graph is not directed.")
        end
        if Graphs.is_cyclic(graph)
            error("The provided graph contains a directed cycle.")
        end
        return true
    end

    function validate_tree(graph::MetaGraphsNext.MetaGraph)
        validate_dag(graph)
        if !Graphs.is_connected(graph)
            error("The provided graph is not connected.")
        end
        if Graphs.is_cyclic(SimpleGraph(graph))
            error("The provided graph contains a cycle.")
        end
        nroots = sum(v -> outdegree(graph, v) == 0, vertices(graph))
        if nroots != 1
            error("The provided graph must have exactly one root (vertex with outdegree 0). Found $nroots.")
        end
        return true
    end

    function update_prop(ds, target, sources, set, get; combine = sum, override = (ds, target, v) -> v)
        if length(sources) > 0
            av = map(s -> get(ds, s), sources)
            return set(ds, target, override(ds, target, combine(av)))
        else
            return ds
        end
    end

    function df_get_by_key(df, key, keyval, prop)
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value not found in DataFrame")
        end
        return df[row_idx, prop]
    end

    df_get_by_id(df, idval, prop) = df_get_by_key(df, :id, idval, prop)

    function df_set_by_key(df, key, keyval, prop, value)
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value not found in DataFrame")
        end
        new_df = copy(df)
        new_df[row_idx, prop] = value
        return new_df
    end

    df_set_by_id(df, idval, prop, value) = df_set_by_key(df, :id, idval, prop, value)

    df_get_keys(df, key) = df[!, key]

    df_get_ids(df) = df_get_keys(df, :id)

    function df_get_row_by_key(df, key, keyval)
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value not found in DataFrame")
        end
        return df[row_idx, :]
    end

    df_get_row_by_id(df, idval) = df_get_row_by_key(df, :id, idval)

    function df_set_row_by_key(df, key, keyval, new_row)
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value not found in DataFrame")
        end
        new_df = copy(df)
        for k in keys(new_row)
            new_df[row_idx, k] = new_row[k]
        end
        return new_df
    end
    
    df_set_row_by_id(df, idval, new_row) = df_set_row_by_key(df, :id, idval, new_row)

    function update_df_prop_by_key(df, key, target, sources, prop; combine = sum, override = (ds, target, v) -> v)
        update_prop(df, target, sources, (d, k, v) -> df_set_by_key(d, key, k, prop, v),
            (d, k) -> df_get_by_key(d, key, k, prop),
            combine = combine, override = override)
    end

    function update_df_prop_by_id(df, target, sources, prop; combine = sum, override = (ds, target, v) -> v)
        update_df_prop_by_key(df, :id, target, sources, prop, combine = combine, override = override)
    end
    
end
