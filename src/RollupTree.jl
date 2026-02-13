module RollupTree

using DataFrames
using Graphs

# Write your package code here.

    rollup(table, tree) = begin
        table
    end

    update_prop(ds, target, sources, set, get, combine = (av) -> sum(av), override = (ds, target, v) -> v) = begin
        if length(sources) > 0
            av = map(s -> get(ds,s), sources)
            return set(ds, target, override(ds, target, combine(av)))
        else
            return ds
        end
    end

    df_get_by_key(df, key, keyval, prop)    = begin
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value not found in DataFrame")
        end
        return df[row_idx, prop]
    end

    df_get_by_id(df, idval, prop) = df_get_by_key(df, :id, idval, prop)

    df_set_by_key(df, key, keyval, prop, value) = begin
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value not found in DataFrame")
        end
        df[row_idx, prop] = value
        return df
    end

    df_set_by_id(df, idval, prop, value) = df_set_by_key(df, :id, idval, prop, value)

    df_get_keys(df, key) = df[!, key]

    df_get_ids(df) = df_get_keys(df, :id)

    df_get_row_by_key(df, key, keyval) = begin
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value not found in DataFrame")
        end
        return df[row_idx, :]
    end

    df_get_row_by_id(df, idval) = df_get_row_by_key(df, :id, idval)

    df_set_row_by_key(df, key, keyval, new_row) = begin
        row_idx = findfirst(df[!, key] .== keyval)
        if isnothing(row_idx)
            error("Key value not found in DataFrame")
        end
        for k in keys(new_row)
            df[row_idx, k] = new_row[k]
        end
        return df
    end
    
    df_set_row_by_id(df, idval, new_row) = df_set_row_by_key(df, :id, idval, new_row)

end
