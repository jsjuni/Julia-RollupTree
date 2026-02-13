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
end
