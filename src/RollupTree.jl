"Perform recursive computations on a tree-structured graph."
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

    "Get labels of the predecessors of the vertex with the given label in the graph."
    macro predecessor_labels_of(graph, label)
        :( inneighbor_labels($(esc(graph)), $(esc(label))) )
    end

    "Get labels of the successors of the vertex with the given label in the graph."
    macro successor_labels_of(graph, label)
        :( outneighbor_labels($(esc(graph)), $(esc(label))) )    
    end

    "Get the number of predecessors of the vertex with the given label in the graph."
    macro n_predecessors(graph, label)
        :( indegree($(esc(graph)), code_for($(esc(graph)), $(esc(label)))) )
    end

    "Check if the vertex with the given label has any predecessors in the graph."
    macro has_predecessors(graph, label)
        :( @n_predecessors($(esc(graph)), $(esc(label))) > 0 )
    end

    "Get the number of successors of the vertex with the given label in the graph."
    macro n_successors(graph, label)
        :( outdegree($(esc(graph)), code_for($(esc(graph)), $(esc(label)))) )
    end

    "Check if the vertex with the given label has any successors in the graph."
    macro has_successors(graph, label)
        :( @n_successors($(esc(graph)), $(esc(label))) > 0 )
    end
    
    """
        rollup(graph::MetaGraphsNext.MetaGraph, ds, update, validate_ds; validate_graph = validate_tree)

    Perform a rollup operation on the given graph and dataset.
    # Arguments
    - `graph::MetaGraphsNext.MetaGraph`: A directed acyclic graph representing the hierarchy of the data.
    - `ds`: The initial dataset to be rolled up. This can be any data structure that supports the necessary operations for updating properties based on predecessors.
    - `update`: A function that takes the current dataset, a vertex label, and the labels of its predecessors, and returns an updated dataset with the property for that vertex updated based on its predecessors.
    - `validate_ds`: A function that validates the initial dataset against the graph structure. It should throw an error if the dataset is not compatible with the graph.
    - `validate_graph`: An optional function that validates the graph structure. By default, it checks that the graph is a tree. It should throw an error if the graph does not meet the required structure.
    # Returns
    An updated dataset with properties rolled up according to the graph hierarchy.
    """
    function rollup(graph::MetaGraphsNext.MetaGraph, ds, update, validate_ds; validate_graph = validate_tree)
        validate_graph(graph)
        validate_ds(graph, ds)
        mapfoldl(
            v -> label_for(graph, v),                                     # (3) map vertices to their IDs
            (s, vl) -> update(s, vl, @predecessor_labels_of(graph, vl)),  # (4) apply successive dataset updates
            topological_sort(graph),                                      # (2) iterate vertices in precedence order
            init = ds                                                     # (1) start with the original dataset
        )                                                                 # (5) return the updated dataset
    end

    """
        update_rollup(graph::MetaGraphsNext.MetaGraph, ds, vertex, update)

    Update the rollup for a specific vertex and all vertices above it in the hierarchy.
    # Arguments
    - `graph::MetaGraphsNext.MetaGraph`: A directed acyclic graph representing the hierarchy of the data.
    - `ds`: The current dataset with properties already rolled up for all vertices except the specified vertex and its ancestors.
    - `vertex`: The label of the vertex for which the rollup should be updated. This vertex must have no predecessors (i.e., it must be a leaf in the hierarchy).
    - `update`: A function that takes the current dataset, a vertex label, and the labels of its predecessors, and returns an updated dataset
            with the property for that vertex updated based on its predecessors.
    # Returns
    An updated dataset with properties rolled up for the specified vertex and all vertices above it in the hierarchy.
    """
    function update_rollup(graph::MetaGraphsNext.MetaGraph, ds, vertex, update)
        if @has_predecessors(graph, vertex)
            error("Vertex $vertex has predecessors. update_rollup can only be applied to vertices with no predecessors.")
        end
        todo = [vertex]
        vertices_above = []
        while length(todo) > 0
            v = pop!(todo)
            for p in @successor_labels_of(graph, v)
                push!(vertices_above, p)
                push!(todo, p)
            end
        end
        foldl(
            (s, v) -> update(s, v, @predecessor_labels_of(graph, v)),
            vertices_above;
            init = ds
        )
    end
    """
        validate_ds(graph::MetaGraphsNext.MetaGraph, ds, get_keys, get_prop, op = x -> isa(x, Number))

    Validate that the dataset is compatible with the graph structure. This function checks that:
    - The set of IDs in the dataset matches the set of vertex labels in the graph.
    - For each vertex with no predecessors, the corresponding value in the dataset satisfies a specified condition (e.g., is a number).
    # Arguments
    - `graph::MetaGraphsNext.MetaGraph`: The graph representing the hierarchy of the data.
    - `ds`: The dataset to be validated. This can be any data structure that supports the necessary operations for retrieving keys and properties.
    - `get_keys`: A function that retrieves the set of keys (IDs) from the dataset.
    - `get_prop`: A function that retrieves the property value for a given key from the dataset.
    - `op`: An optional function that checks whether a property value satisfies a certain condition. By default, it checks if the value is a number. This function should
        return `true` if the value is valid and `false` otherwise.        
    # Returns
    `true` if the dataset is valid according to the specified conditions, otherwise it throws an error with a descriptive message.
    """
    function validate_ds(graph::MetaGraphsNext.MetaGraph, ds, get_keys, get_prop, op = x -> isa(x, Number))
        ids_in_tree = Set(labels(graph))
        ids_in_ds = Set(get_keys(ds))
        if ids_in_tree != ids_in_ds
            error("The set of IDs in the DataFrame does not match the set of vertex labels in the graph.")
        end
        for id in filter(id -> !@has_predecessors(graph, id), ids_in_tree)
            value = get_prop(ds, id)
            if !op(value)
                error("Invalid value for ID $id: $value")
            end
        end
        true
    end

    """
        validate_dag(graph::MetaGraphsNext.MetaGraph)

    Validate that the graph is a directed acyclic graph (DAG). This function checks that:
    - The graph is directed.
    - The graph does not contain any directed cycles.
    # Arguments
    - `graph::MetaGraphsNext.MetaGraph`: The graph to be validated.
    # Returns
    `true` if the graph is a DAG, otherwise it throws an error with a descriptive message.
    """
    function validate_dag(graph::MetaGraphsNext.MetaGraph)
        if !is_directed(graph)
            error("The provided graph is not directed.")
        end
        if is_cyclic(graph)
            error("The provided graph contains a directed cycle.")
        end
        true
    end

    """
        validate_tree(graph::MetaGraphsNext.MetaGraph)

    Validate that the graph is a tree. This function checks that:
    - The graph is a directed acyclic graph (DAG).
    - The graph is connected.
    - The graph has exactly one root vertex (a vertex with no successors).
    # Arguments
    - `graph::MetaGraphsNext.MetaGraph`: The graph to be validated.
    # Returns
    `true` if the graph is a tree, otherwise it throws an error with a descriptive message.
    """
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

    """
        update_prop(ds, target, predecessors, set, get; combine = sum,
                override = (ds, target, v) -> v,
                initialize = (ds, target) -> ds)

    Update a property for a target vertex based on the properties of its predecessors. This function performs the following steps:
    - If the target vertex has predecessors, it retrieves the property values for all predecessors using the provided `get` function,
        combines them using the specified `combine` function, and then sets the property for the target vertex using the provided `set` function.
        An optional `override` function can be used to modify the combined value before setting it.
    - If the target vertex has no predecessors, it optionally initializes the property for the target vertex using the provided `initialize` function.
    # Arguments
    - `ds`: The dataset containing the properties to be updated. This can be any data structure that supports the necessary operations for retrieving and setting properties.
    - `target`: The label of the target vertex for which the property should be updated.
    - `predecessors`: A list of labels of the predecessor vertices from which to retrieve property values.
    - `set`: A function that takes the dataset, a vertex label, and a value, and returns an updated dataset with the property for that vertex set to the given value.
    - `get`: A function that takes the dataset and a vertex label, and returns the property value for that vertex.
    - `combine`: An optional function that takes a list of property values and combines them into a single value. By default, it sums the values.
    - `override`: An optional function that takes the dataset, the target vertex label, and the combined value, and returns a modified value to be set for the target vertex.
        By default, it returns the combined value without modification.
    - `initialize`: An optional function that takes the dataset and the target vertex label, and returns an updated dataset with the property for the target vertex initialized.
    This function is called only if the target vertex has no predecessors. By default, it returns the dataset unchanged.
    # Returns
    An updated dataset with the property for the target vertex set based on the properties of its predecessors, or initialized if there are no predecessors.
    """
    function update_prop(ds, target, predecessors, set, get; combine = sum,
            override = (ds, target, v) -> v,
            initialize = (ds, target) -> ds)
        if length(predecessors) > 0
            value = combine(map(p -> get(ds, p), predecessors))     # combine propety values from predecessors
            set(ds, target, override(ds, target, value))            # set target property to combined value, with optional override
        else
            initialize(ds, target)                                  # optionally initialize leaf vertex property
        end
    end

    """
        df_get_by_key(df, key, keyval, prop)

    Get the value of a property for a specific key in the DataFrame. This function searches for the row where the specified key column matches the given key value,
        and then retrieves the value of the specified property column in that row. If no such row is found, an error is thrown with a descriptive message.
    # Arguments
    - `df::DataFrame`: The DataFrame containing the data.
    - `key`: The name of the column to be used as the key for searching (e.g., vertex label).
    - `keyval`: The value of the key to search for in the specified key column.
    - `prop`: The name of the column from which to retrieve the property value.
    # Returns
    The value of the specified property for the row where the key column matches the given key value. If no such row is found, an error is thrown with a descriptive message.
"""
    function df_get_by_key(df, key, keyval, prop)
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value $keyval not found in DataFrame")
        end
        df[row_idx, prop]
    end

    """
        df_get_by_id(df, idval, prop)

    Get the value of a property for a specific ID in the DataFrame. This function is a convenience wrapper around `df_get_by_key` that uses the `:id` column as the key.
    # Arguments    - `df::DataFrame`: The DataFrame containing the data.
    - `idval`: The value of the ID to search for in the `:id` column.
    - `prop`: The name of the column from which to retrieve the property value.
    # Returns
    The value of the specified property for the row where the `:id` column matches the given ID value. If no such row is found, an error is thrown with a descriptive message.
    """
    function df_get_by_id(df, idval, prop)
        df_get_by_key(df, :id, idval, prop)
    end

    """
        df_set_by_key(df, key, keyval, prop, value)

    Set the value of a property for a specific key in the DataFrame. This function searches for the row where the specified key column matches the given key value,
        and then updates the value of the specified property column in that row. It returns a new DataFrame with the updated value.
    # Arguments
    - `df::DataFrame`: The DataFrame containing the data.
    - `key`: The name of the column to be used as the key for searching (e.g., vertex label).
    - `keyval`: The value of the key to search for in the specified key column.
    - `prop`: The name of the column for which to set the property value.
    - `value`: The new value to be set for the specified property.
    # Returns
    A new DataFrame with the property for the row where the key column matches the given key value updated to the specified value.
        If no such row is found, an error is thrown with a descriptive message.
"""
    function df_set_by_key(df, key, keyval, prop, value)
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value $keyval not found in DataFrame")
        end
        new_df = copy(df)
        new_df[row_idx, prop] = value
        new_df
    end

    """
        df_set_by_id(df, idval, prop, value)

    Set the value of a property for a specific ID in the DataFrame. This function is a convenience wrapper around `df_set_by_key` that uses the `:id` column as the key.
    # Arguments
    - `df::DataFrame`: The DataFrame containing the data.
    - `idval`: The value of the ID to search for in the `:id` column.
    - `prop`: The name of the column for which to set the property value.
    - `value`: The new value to be set for the specified property.
    # Returns
    A new DataFrame with the property for the row where the `:id` column matches the given ID value updated to the specified value.
        If no such row is found, an error is thrown with a descriptive message.
    """
    function df_set_by_id(df, idval, prop, value)
        df_set_by_key(df, :id, idval, prop, value)
    end

    """
        df_get_keys(df, key)

    Get the values of a specific key column from the DataFrame. This function retrieves all the values from the specified key column, which can
        be used to validate that the set of keys in the DataFrame matches the set of vertex labels in the graph.
    # Arguments
    - `df::DataFrame`: The DataFrame containing the data.
    - `key`: The name of the column to be used as the key for retrieval (e.g., vertex label).
    # Returns
    A vector containing all the values from the specified key column in the DataFrame.
    """
    function df_get_keys(df, key)
        df[!, key]
    end

    """
        df_get_ids(df)

    Get the values of the `:id` column from the DataFrame. This function is a convenience wrapper around `df_get_keys` that retrieves the values from the `:id` column.
    # Arguments
    - `df::DataFrame`: The DataFrame containing the data.
    # Returns
    A vector containing all the values from the `:id` column in the DataFrame.
    """
    function df_get_ids(df)
        df_get_keys(df, :id)
    end

    """
        df_get_row_by_key(df, key, keyval)

    Get the entire row of the DataFrame for a specific key value. This function searches for the row where the specified key column matches the given key value,
        and then retrieves the entire row as a DataFrameRow. If no such row is found, an error is thrown with a descriptive message.
    # Arguments
    - `df::DataFrame`: The DataFrame containing the data.
    - `key`: The name of the column to be used as the key for searching (e.g., vertex label).
    - `keyval`: The value of the key to search for in the specified key column.
    # Returns
    A DataFrameRow containing all the data for the row where the key column matches the given key value. If no such row is found, an error is thrown with a descriptive message.
    """
    function df_get_row_by_key(df, key, keyval)
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value $keyval not found in DataFrame")
        end
        df[row_idx, :]
    end

    """
        df_get_row_by_id(df, idval)

    Get the entire row of the DataFrame for a specific ID value. This function is a convenience wrapper around `df_get_row_by_key` that uses the `:id` column as the key.
    # Arguments
    - `df::DataFrame`: The DataFrame containing the data.
    - `idval`: The value of the ID to search for in the `:id` column.
    # Returns
    A DataFrameRow containing all the data for the row where the `:id` column matches the given ID value. If no such row is found, an error is thrown with a descriptive message.
    """
    function df_get_row_by_id(df, idval)
        df_get_row_by_key(df, :id, idval)
    end

    """
        df_set_row_by_key(df, key, keyval, new_row)

    Set the values of a specific row in the DataFrame based on a key column. This function searches for the row where the specified key column matches the given key value, and then updates the entire row with the new values provided.
    # Arguments
    - `df::DataFrame`: The DataFrame containing the data.
    - `key`: The name of the column to be used as the key for searching (e.g., vertex label).
    - `keyval`: The value of the key to search for in the specified key column.
    - `new_row`: A dictionary containing the new values for the row.
    # Returns
    A new DataFrame with the specified row updated to the new values. If no such row is found, an error is thrown with a descriptive message.
    """
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
    
    """
        df_set_row_by_id(df, idval, new_row)

    Set the values of a specific row in the DataFrame based on the `:id` column. This function is a convenience wrapper around `df_set_row_by_key` that uses the `:id` column as the key.
    # Arguments
    - `df::DataFrame`: The DataFrame containing the data.
    - `idval`: The value of the ID to search for in the `:id` column.
    - `new_row`: A dictionary containing the new values for the row.
    # Returns
    A new DataFrame with the specified row updated to the new values. If no such row is found, an error is thrown with a descriptive message.
    """
    function df_set_row_by_id(df, idval, new_row)
        df_set_row_by_key(df, :id, idval, new_row)
    end

    """
        update_df_prop_by_key(df, key, target, predecessors, prop; combine = sum, override = (ds, target, v) -> v)

    Update a property of a specific row in the DataFrame based on a key column. This function searches for the row where the specified key column matches the given key value, and then updates the specified property with the new value provided.
    # Arguments
    - `df::DataFrame`: The DataFrame containing the data.
    - `key`: The name of the column to be used as the key for searching (e.g., vertex label).
    - `keyval`: The value of the key to search for in the specified key column.
    - `target`: The name of the property to update.
    - `predecessors`: A list of predecessor nodes.
    - `prop`: The new value for the property.
    # Returns
    A new DataFrame with the specified property updated for the row. If no such row is found, an error is thrown with a descriptive message.
    """
    function update_df_prop_by_key(df, key, target, predecessors, prop; combine = sum, override = (ds, target, v) -> v)
        update_prop(df, target, predecessors, (d, k, v) -> df_set_by_key(d, key, k, prop, v),
            (d, k) -> df_get_by_key(d, key, k, prop),
            combine = combine, override = override)
    end

    """
        update_df_prop_by_id(df, target, predecessors, prop; combine = sum, override = (ds, target, v) -> v)

    Update a property of a specific row in the DataFrame based on the `:id` column. This function is a convenience wrapper around `update_df_prop_by_key`
        that uses the `:id` column as the key.
    # Arguments
    - `df::DataFrame`: The DataFrame containing the data.
    - `target`: The name of the property to update.
    - `predecessors`: A list of predecessor nodes.
    - `prop`: The new value for the property.
    # Returns
    A new DataFrame with the specified property updated for the row. If no such row is found, an error is thrown with a descriptive message.
    """
    function update_df_prop_by_id(df, target, predecessors, prop; combine = sum, override = (ds, target, v) -> v)
        update_df_prop_by_key(df, :id, target, predecessors, prop, combine = combine, override = override)
    end
    
end
