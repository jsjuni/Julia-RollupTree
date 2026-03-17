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

    macro predecessor_labels_of(tree, parent)
        :( inneighbor_labels($(esc(tree)), $(esc(parent))) )
    end

    macro successor_labels_of(tree, child)
        :( outneighbor_labels($(esc(tree)), $(esc(child))) )    
    end

    macro n_predecessors(tree, vertex)
        :( indegree($(esc(tree)), code_for($(esc(tree)), $(esc(vertex)))) )
    end

    macro has_predecessors(tree, vertex)
        :( @n_predecessors($(esc(tree)), $(esc(vertex))) > 0 )
    end
    
     macro n_successors(tree, vertex)
        :( outdegree($(esc(tree)), code_for($(esc(tree)), $(esc(vertex)))) )
    end

    macro has_successors(tree, vertex)
        :( @n_successors($(esc(tree)), $(esc(vertex))) > 0 )
    end
    
    function rollup(tree::MetaGraphsNext.MetaGraph, ds, update, validate_ds; validate_tree = validate_tree)
        validate_tree(tree)
        validate_ds(tree, ds)
        mapfoldl(
            v -> label_for(tree, v),                                     # (3) map vertices to their IDs
            (s, vl) -> update(s, vl, @predecessor_labels_of(tree, vl)),  # (4) apply dataset updates
            topological_sort(tree);                                      # (2) get vertices in precedence order
            init = ds                                                    # (1) start with the original dataset
        )                                                                # (5) return the updated dataset
    end

    function update_rollup(tree::MetaGraphsNext.MetaGraph, ds, vertex, update)
        if @has_predecessors(tree, vertex)
            error("Vertex $vertex has predecessors. update_rollup can only be applied to vertices with no predecessors.")
        end
        todo = [vertex]
        vertices_above = []
        while length(todo) > 0
            v = pop!(todo)
            for p in @successor_labels_of(tree, v)
                push!(vertices_above, p)
                push!(todo, p)
            end
        end
        foldl(
            (s, v) -> update(s, v, @predecessor_labels_of(tree, v)),
            vertices_above;
            init = ds
        )
    end

    function validate_ds(tree::MetaGraphsNext.MetaGraph, ds, get_keys, get_prop, op = x -> isa(x, Number))
        ids_in_tree = Set(labels(tree))
        ids_in_ds = Set(get_keys(ds))
        if ids_in_tree != ids_in_ds
            error("The set of IDs in the DataFrame does not match the set of vertex labels in the graph.")
        end
        for id in filter(id -> !@has_predecessors(tree, id), ids_in_tree)
            value = get_prop(ds, id)
            if !op(value)
                error("Invalid value for ID $id: $value")
            end
        end
        true
    end

    function validate_dag(graph::MetaGraphsNext.MetaGraph)
        if !is_directed(graph)
            error("The provided graph is not directed.")
        end
        if is_cyclic(graph)
            error("The provided graph contains a directed cycle.")
        end
        true
    end

    function validate_tree(graph::MetaGraphsNext.MetaGraph)
        validate_dag(graph)
        if !is_connected(graph)
            error("The provided graph is not connected.")
        end
        if is_cyclic(SimpleGraph(graph))
            error("The provided graph contains a cycle.")
        end
        nroots = sum(v -> !@has_successors(graph, v), labels(graph))
        if nroots != 1
            error("The provided graph must have exactly one root with no successor. Found $nroots.")
        end
        true
    end

    function update_prop(data_set, target, predecessors, set, get; combine = sum,
            override = (ds, target, v) -> v,
            initialize = (ds, target) -> ds)
        if length(predecessors) > 0
            values = map(source -> get(data_set, source), predecessors)
            set(data_set, target, override(data_set, target, combine(values)))
        else
            initialize(data_set, target)
        end
    end

    function df_get_by_key(df, key, keyval, prop)
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value $keyval not found in DataFrame")
        end
        df[row_idx, prop]
    end

    function df_get_by_id(df, idval, prop)
        df_get_by_key(df, :id, idval, prop)
    end

    function df_set_by_key(df, key, keyval, prop, value)
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value $keyval not found in DataFrame")
        end
        new_df = copy(df)
        new_df[row_idx, prop] = value
        new_df
    end

    function df_set_by_id(df, idval, prop, value)
        df_set_by_key(df, :id, idval, prop, value)
    end

    function df_get_keys(df, key)
        df[!, key]
    end

    function df_get_ids(df)
        df_get_keys(df, :id)
    end

    function df_get_row_by_key(df, key, keyval)
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value $keyval not found in DataFrame")
        end
        df[row_idx, :]
    end

    function df_get_row_by_id(df, idval)
        df_get_row_by_key(df, :id, idval)
    end

    function df_set_row_by_key(df, key, keyval, new_row)
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value $keyval not found in DataFrame")
        end
        new_df = copy(df)
        for k in keys(new_row)
            new_df[row_idx, k] = new_row[k]
        end
        new_df
    end
    
    function df_set_row_by_id(df, idval, new_row)
        df_set_row_by_key(df, :id, idval, new_row)
    end

    function update_df_prop_by_key(df, key, target, predecessors, prop; combine = sum, override = (ds, target, v) -> v)
        update_prop(df, target, predecessors, (d, k, v) -> df_set_by_key(d, key, k, prop, v),
            (d, k) -> df_get_by_key(d, key, k, prop),
            combine = combine, override = override)
    end

    function update_df_prop_by_id(df, target, predecessors, prop; combine = sum, override = (ds, target, v) -> v)
        update_df_prop_by_key(df, :id, target, predecessors, prop, combine = combine, override = override)
    end
    
end
